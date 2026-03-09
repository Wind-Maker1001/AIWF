use super::*;

pub(crate) fn run_transform_rows_v2(req: TransformRowsReq) -> Result<TransformRowsResp, String> {
    run_transform_rows_v2_with_cancel(req, None)
}

pub(crate) fn run_transform_rows_v2_with_cancel(
    req: TransformRowsReq,
    cancel_flag: Option<Arc<AtomicBool>>,
) -> Result<TransformRowsResp, String> {
    let started = Instant::now();
    verify_request_signature(&req)?;
    let max_rows = env::var("AIWF_RUST_V2_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(200000);
    let max_bytes = env::var("AIWF_RUST_V2_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(128 * 1024 * 1024);
    let mut rows_in = req.rows.unwrap_or_default();
    if rows_in.is_empty()
        && let Some(uri) = req.input_uri.clone()
    {
        rows_in = load_rows_from_uri_limited(&uri, max_rows, max_bytes)?;
    }
    let input_rows = rows_in.len();
    if input_rows > max_rows {
        return Err(format!(
            "input rows exceed limit: {} > {}",
            input_rows, max_rows
        ));
    }
    let estimated_bytes = serde_json::to_vec(&rows_in).map(|b| b.len()).unwrap_or(0);
    if estimated_bytes > max_bytes {
        return Err(format!(
            "input payload exceeds limit: {} > {}",
            estimated_bytes, max_bytes
        ));
    }
    let rules = if req.rules.is_some() {
        req.rules.clone().unwrap_or_else(|| json!({}))
    } else if let Some(dsl) = req.rules_dsl.clone() {
        compile_rules_dsl(&dsl)?
    } else {
        json!({})
    };
    let gates = req.quality_gates.unwrap_or_else(|| json!({}));

    let null_values: Vec<String> = as_array_str(rule_get(&rules, "null_values"))
        .into_iter()
        .map(|s| s.to_lowercase())
        .collect();
    let trim_strings = as_bool(rule_get(&rules, "trim_strings"), true);

    let rename_map: HashMap<String, String> = rule_get(&rules, "rename_map")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<HashMap<String, String>>()
        })
        .unwrap_or_default();
    let casts: HashMap<String, String> = rule_get(&rules, "casts")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.trim().to_lowercase())))
                .collect::<HashMap<String, String>>()
        })
        .unwrap_or_default();

    let filters = rule_get(&rules, "filters")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let compiled_filters = compile_filters(&filters);
    let required_fields = as_array_str(rule_get(&rules, "required_fields"));
    let default_values = rule_get(&rules, "default_values")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let include_fields = as_array_str(rule_get(&rules, "include_fields"));
    let exclude_fields = as_array_str(rule_get(&rules, "exclude_fields"));
    let deduplicate_by = as_array_str(rule_get(&rules, "deduplicate_by"));
    let dedup_keep = rule_get(&rules, "deduplicate_keep")
        .and_then(|v| v.as_str())
        .unwrap_or("last")
        .to_lowercase();
    let sort_by = rule_get(&rules, "sort_by")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let requested_engine = resolve_transform_engine(&rules);
    let (engine, engine_reason) = if requested_engine == "auto_v1" {
        auto_select_engine(input_rows, estimated_bytes, &rules)
    } else {
        (
            requested_engine.clone(),
            format!("requested: {}", requested_engine),
        )
    };
    let use_columnar = engine == "columnar_v1" || engine == "columnar_arrow_v1";

    let mut invalid_rows = 0usize;
    let mut filtered_rows = 0usize;
    let mut rule_hits: HashMap<String, usize> = HashMap::new();
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    let mut prepared_rows: Vec<Map<String, Value>> = Vec::new();

    for row in rows_in {
        if is_cancelled(&cancel_flag) {
            return Err("task cancelled".to_string());
        }
        let Some(obj) = row.as_object() else {
            invalid_rows += 1;
            *rule_hits.entry("invalid_object".to_string()).or_insert(0) += 1;
            continue;
        };
        let mut out: Map<String, Value> = Map::new();
        for (k, v) in obj {
            let key = rename_map.get(k).cloned().unwrap_or_else(|| k.clone());
            let mut vv = v.clone();
            if let Some(s) = vv.as_str() {
                let mut ss = s.to_string();
                if trim_strings {
                    ss = ss.trim().to_string();
                }
                if null_values.iter().any(|x| x == &ss.to_lowercase()) {
                    vv = Value::Null;
                } else {
                    vv = Value::String(ss);
                }
            }
            out.insert(key, vv);
        }

        for (k, v) in &default_values {
            let should_fill = match out.get(k) {
                None => true,
                Some(Value::Null) => true,
                Some(Value::String(s)) => s.trim().is_empty(),
                Some(_) => false,
            };
            if should_fill {
                out.insert(k.clone(), v.clone());
            }
        }

        if use_columnar {
            prepared_rows.push(out);
            continue;
        }

        for (field, cast_type) in &casts {
            if let Some(v) = out.get(field).cloned() {
                match cast_value(v, cast_type) {
                    Some(casted) => {
                        out.insert(field.clone(), casted);
                    }
                    None => {
                        invalid_rows += 1;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                        out.clear();
                        break;
                    }
                }
            }
        }
        if out.is_empty() {
            continue;
        }

        if !required_fields.is_empty() {
            let mut missing = false;
            for f in &required_fields {
                if is_missing(out.get(f)) {
                    missing = true;
                    break;
                }
            }
            if missing {
                invalid_rows += 1;
                *rule_hits.entry("required_missing".to_string()).or_insert(0) += 1;
                continue;
            }
        }

        if !compiled_filters.is_empty()
            && !compiled_filters
                .iter()
                .all(|f| filter_match_compiled(&out, f))
        {
            filtered_rows += 1;
            *rule_hits.entry("filtered_by_rule".to_string()).or_insert(0) += 1;
            continue;
        }

        if !include_fields.is_empty() {
            let mut next: Map<String, Value> = Map::new();
            for f in &include_fields {
                if let Some(v) = out.get(f) {
                    next.insert(f.clone(), v.clone());
                }
            }
            out = next;
        }
        for f in &exclude_fields {
            out.remove(f);
        }
        rows.push(out);
    }

    if use_columnar {
        let (mut out_rows, bad_rows, f_rows) = if engine == "columnar_arrow_v1" {
            apply_transform_columnar_arrow_v1(
                prepared_rows,
                &casts,
                &required_fields,
                &compiled_filters,
                &mut rule_hits,
            )
        } else {
            apply_transform_columnar_v1(
                prepared_rows,
                &casts,
                &required_fields,
                &compiled_filters,
                &mut rule_hits,
            )
        };
        invalid_rows += bad_rows;
        filtered_rows += f_rows;
        if !include_fields.is_empty() {
            for r in &mut out_rows {
                let mut next: Map<String, Value> = Map::new();
                for f in &include_fields {
                    if let Some(v) = r.get(f) {
                        next.insert(f.clone(), v.clone());
                    }
                }
                *r = next;
            }
        }
        if !exclude_fields.is_empty() {
            for r in &mut out_rows {
                for f in &exclude_fields {
                    r.remove(f);
                }
            }
        }
        rows = out_rows;
    }

    apply_expression_fields(&mut rows, &rules, &mut rule_hits);
    apply_string_and_date_ops(&mut rows, &rules, &mut rule_hits);

    let mut duplicate_rows_removed = 0usize;
    if use_columnar {
        let before = rows.len();
        let (out_rows, dup_removed) =
            apply_dedup_sort_columnar_v1(rows, &deduplicate_by, &dedup_keep, &sort_by);
        rows = out_rows;
        duplicate_rows_removed = if !deduplicate_by.is_empty() {
            dup_removed
        } else {
            before.saturating_sub(rows.len())
        };
    } else {
        if !deduplicate_by.is_empty() {
            let mut map: HashMap<String, Map<String, Value>> = HashMap::new();
            if dedup_keep == "first" {
                for r in rows {
                    let k = dedup_key(&r, &deduplicate_by);
                    map.entry(k).or_insert(r);
                }
            } else {
                for r in rows {
                    let k = dedup_key(&r, &deduplicate_by);
                    map.insert(k, r);
                }
            }
            let out_len = map.len();
            duplicate_rows_removed =
                input_rows.saturating_sub(invalid_rows + filtered_rows + out_len);
            rows = map.into_values().collect();
        }

        if !sort_by.is_empty() {
            rows.sort_by(|a, b| compare_rows(a, b, &sort_by));
        }
    }

    if is_cancelled(&cancel_flag) {
        return Err("task cancelled".to_string());
    }

    let aggregate = compute_aggregate(&rows, rule_get(&rules, "aggregate"));

    let output_rows = rows.len();
    let gate_required_fields = {
        let from_gate = as_array_str(gates.get("required_fields"));
        if from_gate.is_empty() {
            required_fields.clone()
        } else {
            from_gate
        }
    };
    let mut required_missing_by_field: Map<String, Value> = Map::new();
    let mut required_missing_cells = 0usize;
    if !gate_required_fields.is_empty() {
        for f in &gate_required_fields {
            let mut miss = 0usize;
            for r in &rows {
                if is_missing(r.get(f)) {
                    miss += 1;
                }
            }
            required_missing_cells += miss;
            required_missing_by_field.insert(f.clone(), Value::Number((miss as u64).into()));
        }
    }
    let required_total_cells = output_rows.saturating_mul(gate_required_fields.len());
    let required_missing_ratio = if required_total_cells > 0 {
        required_missing_cells as f64 / required_total_cells as f64
    } else {
        0.0
    };

    let quality = json!({
        "input_rows": input_rows,
        "output_rows": output_rows,
        "invalid_rows": invalid_rows,
        "filtered_rows": filtered_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "required_fields": gate_required_fields,
        "required_missing_cells": required_missing_cells,
        "required_missing_by_field": required_missing_by_field,
        "required_missing_ratio": required_missing_ratio,
    });

    let gate_result = evaluate_quality_gates(&quality, &gates);
    let passed = gate_result
        .get("passed")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    if !passed {
        return Err(format!(
            "transform_rows_v2 quality gate failed: {}",
            gate_result
                .get("errors")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .map(value_to_string)
                        .collect::<Vec<String>>()
                        .join("; ")
                })
                .unwrap_or_default()
        ));
    }

    let latency_ms = started.elapsed().as_millis();
    let trace_id = resolve_trace_id(
        req.trace_id.as_deref(),
        req.traceparent.as_deref(),
        &format!(
            "{}:{}:{}:{}",
            req.run_id.clone().unwrap_or_default(),
            input_rows,
            output_rows,
            latency_ms
        ),
    );
    let resp = TransformRowsResp {
        ok: true,
        operator: "transform_rows_v2".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        trace_id,
        rows: rows.into_iter().map(Value::Object).collect(),
        quality,
        gate_result,
        stats: TransformRowsStats {
            input_rows,
            output_rows,
            invalid_rows,
            filtered_rows,
            duplicate_rows_removed,
            latency_ms,
        },
        rust_v2_used: true,
        schema_hint: req.schema_hint,
        aggregate,
        audit: json!({
            "rule_hits": rule_hits,
            "engine": engine,
            "engine_requested": requested_engine,
            "engine_reason": engine_reason,
            "estimated_input_bytes": estimated_bytes,
            "limits": {
                "max_rows": max_rows,
                "max_payload_bytes": max_bytes
            }
        }),
    };
    if let Some(uri) = req.output_uri {
        save_rows_to_uri(&uri, &resp.rows)?;
    }
    Ok(resp)
}
