use super::*;
use crate::FilterOp;

fn reason_sample_keys() -> [&'static str; 5] {
    [
        "invalid_object",
        "cast_failed",
        "required_missing",
        "filter_rejected",
        "duplicate_removed",
    ]
}

fn empty_reason_samples() -> HashMap<String, Vec<Value>> {
    let mut out = HashMap::new();
    for key in reason_sample_keys() {
        out.insert(key.to_string(), Vec::new());
    }
    out
}

fn row_excerpt(row: &Map<String, Value>) -> Value {
    Value::Object(row.clone())
}

fn push_reason_sample(
    reason_samples: &mut HashMap<String, Vec<Value>>,
    reason_code: &str,
    sample_limit: usize,
    sample: Value,
) {
    let items = reason_samples
        .entry(reason_code.to_string())
        .or_default();
    if items.len() < sample_limit {
        items.push(sample);
    }
}

fn filter_detail(filter: &CompiledFilter) -> String {
    let field = filter.field.trim();
    let op = match &filter.op {
        FilterOp::Exists => "exists",
        FilterOp::NotExists => "not_exists",
        FilterOp::Eq(_) => "eq",
        FilterOp::Ne(_) => "ne",
        FilterOp::Contains(_) => "contains",
        FilterOp::In(_) => "in",
        FilterOp::NotIn(_) => "not_in",
        FilterOp::Regex(_) => "regex",
        FilterOp::NotRegex(_) => "not_regex",
        FilterOp::Gt(_) => "gt",
        FilterOp::Gte(_) => "gte",
        FilterOp::Lt(_) => "lt",
        FilterOp::Lte(_) => "lte",
        FilterOp::Invalid => "invalid",
        FilterOp::Passthrough => "passthrough",
    };
    if field.is_empty() {
        op.to_string()
    } else {
        format!("{field}:{op}")
    }
}

fn null_or_string(value: Option<&str>) -> Value {
    match value {
        Some(text) if !text.trim().is_empty() => Value::String(text.to_string()),
        _ => Value::Null,
    }
}

pub(super) fn execute_transform_rows(
    prepared: &TransformPrepared,
    cancel_flag: &Option<Arc<AtomicBool>>,
) -> Result<TransformExecution, String> {
    let mut invalid_rows = 0usize;
    let mut filtered_rows = 0usize;
    let mut rule_hits: HashMap<String, usize> = HashMap::new();
    let mut reason_samples = empty_reason_samples();
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    let mut prepared_rows: Vec<Map<String, Value>> = Vec::new();
    let mut numeric_cells_total = 0usize;
    let mut numeric_cells_parsed = 0usize;

    for (row_index, row) in prepared.rows_in.clone().into_iter().enumerate() {
        if is_cancelled(cancel_flag) {
            return Err("task cancelled".to_string());
        }
        let Some(obj) = row.as_object() else {
            invalid_rows += 1;
            *rule_hits.entry("invalid_object".to_string()).or_insert(0) += 1;
            push_reason_sample(
                &mut reason_samples,
                "invalid_object",
                prepared.audit_sample_limit,
                json!({
                    "reason_code": "invalid_object",
                    "row_index": row_index + 1,
                    "field": Value::Null,
                    "key": Value::Null,
                    "raw_row": row,
                    "detail": "row is not an object"
                }),
            );
            continue;
        };
        let mut out: Map<String, Value> = Map::new();
        for (key, value) in obj {
            let renamed = prepared
                .rename_map
                .get(key)
                .cloned()
                .unwrap_or_else(|| key.clone());
            let mut next_value = value.clone();
            if let Some(s) = next_value.as_str() {
                let mut normalized = s.to_string();
                if prepared.trim_strings {
                    normalized = normalized.trim().to_string();
                }
                if prepared
                    .null_values
                    .iter()
                    .any(|candidate| candidate == &normalized.to_lowercase())
                {
                    next_value = Value::Null;
                } else {
                    next_value = Value::String(normalized);
                }
            }
            out.insert(renamed, next_value);
        }

        for (key, value) in &prepared.default_values {
            let should_fill = match out.get(key) {
                None => true,
                Some(Value::Null) => true,
                Some(Value::String(s)) => s.trim().is_empty(),
                Some(_) => false,
            };
            if should_fill {
                out.insert(key.clone(), value.clone());
            }
        }

        if prepared.use_columnar {
            prepared_rows.push(out);
            continue;
        }

        for (field, cast_type) in &prepared.casts {
            if let Some(value) = out.get(field).cloned() {
                let normalized_cast = cast_type.trim().to_ascii_lowercase();
                let is_numeric_cast = matches!(
                    normalized_cast.as_str(),
                    "int" | "integer" | "float" | "double" | "number" | "decimal"
                );
                if is_numeric_cast && !is_missing(Some(&value)) {
                    numeric_cells_total += 1;
                }
                match cast_value(value, cast_type) {
                    Some(casted) => {
                        if is_numeric_cast {
                            numeric_cells_parsed += 1;
                        }
                        out.insert(field.clone(), casted);
                    }
                    None => {
                        invalid_rows += 1;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                        push_reason_sample(
                            &mut reason_samples,
                            "cast_failed",
                            prepared.audit_sample_limit,
                            json!({
                                "reason_code": "cast_failed",
                                "row_index": row_index + 1,
                                "field": field,
                                "key": Value::Null,
                                "raw_row": row,
                                "detail": format!("cast failed for {field} as {cast_type}")
                            }),
                        );
                        out.clear();
                        break;
                    }
                }
            }
        }
        if out.is_empty() {
            continue;
        }

        if !prepared.required_fields.is_empty() {
            let mut missing = false;
            for field in &prepared.required_fields {
                if is_missing(out.get(field)) {
                    missing = true;
                    break;
                }
            }
            if missing {
                invalid_rows += 1;
                *rule_hits.entry("required_missing".to_string()).or_insert(0) += 1;
                let missing_fields = prepared
                    .required_fields
                    .iter()
                    .filter(|field| is_missing(out.get(field.as_str())))
                    .cloned()
                    .collect::<Vec<String>>();
                push_reason_sample(
                    &mut reason_samples,
                    "required_missing",
                    prepared.audit_sample_limit,
                    json!({
                        "reason_code": "required_missing",
                        "row_index": row_index + 1,
                        "field": null_or_string(missing_fields.first().map(|s| s.as_str())),
                        "key": Value::Null,
                        "row_excerpt": row_excerpt(&out),
                        "detail": format!("missing required fields: {}", missing_fields.join(","))
                    }),
                );
                continue;
            }
        }

        if !prepared.compiled_filters.is_empty() {
            let failed_filter = prepared
                .compiled_filters
                .iter()
                .find(|filter| !filter_match_compiled(&out, filter));
            if let Some(filter) = failed_filter {
                filtered_rows += 1;
                *rule_hits.entry("filtered_by_rule".to_string()).or_insert(0) += 1;
                push_reason_sample(
                    &mut reason_samples,
                    "filter_rejected",
                    prepared.audit_sample_limit,
                    json!({
                    "reason_code": "filter_rejected",
                    "row_index": row_index + 1,
                    "field": filter.field.clone(),
                    "key": Value::Null,
                    "row_excerpt": row_excerpt(&out),
                        "detail": filter_detail(filter)
                    }),
                );
                continue;
            }
        }

        if !prepared.include_fields.is_empty() {
            let mut next: Map<String, Value> = Map::new();
            for field in &prepared.include_fields {
                if let Some(value) = out.get(field) {
                    next.insert(field.clone(), value.clone());
                }
            }
            out = next;
        }
        for field in &prepared.exclude_fields {
            out.remove(field);
        }
        rows.push(out);
    }

    if prepared.use_columnar {
        let (mut out_rows, bad_rows, f_rows) = if prepared.engine == "columnar_arrow_v1" {
            apply_transform_columnar_arrow_v1(
                prepared_rows,
                &prepared.casts,
                &prepared.required_fields,
                &prepared.compiled_filters,
                &mut rule_hits,
            )
        } else {
            apply_transform_columnar_v1(
                prepared_rows,
                &prepared.casts,
                &prepared.required_fields,
                &prepared.compiled_filters,
                &mut rule_hits,
            )
        };
        invalid_rows += bad_rows;
        filtered_rows += f_rows;
        if !prepared.include_fields.is_empty() {
            for row in &mut out_rows {
                let mut next: Map<String, Value> = Map::new();
                for field in &prepared.include_fields {
                    if let Some(value) = row.get(field) {
                        next.insert(field.clone(), value.clone());
                    }
                }
                *row = next;
            }
        }
        if !prepared.exclude_fields.is_empty() {
            for row in &mut out_rows {
                for field in &prepared.exclude_fields {
                    row.remove(field);
                }
            }
        }
        rows = out_rows;
    }

    apply_expression_fields(&mut rows, &prepared.rules, &mut rule_hits);

    let date_ops = rule_get(&prepared.rules, "date_ops")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut date_cells_total = 0usize;
    for row in &rows {
        for op in &date_ops {
            let Some(obj) = op.as_object() else { continue };
            let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
            if field.is_empty() {
                continue;
            }
            if !is_missing(row.get(field)) {
                date_cells_total += 1;
            }
        }
    }
    apply_string_and_date_ops(&mut rows, &prepared.rules, &mut rule_hits);
    let mut date_cells_parsed = 0usize;
    for row in &rows {
        for op in &date_ops {
            let Some(obj) = op.as_object() else { continue };
            let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
            let out_field = obj.get("as").and_then(|v| v.as_str()).unwrap_or(field);
            if out_field.is_empty() {
                continue;
            }
            if !is_missing(row.get(out_field)) {
                date_cells_parsed += 1;
            }
        }
    }

    let mut duplicate_rows_removed = 0usize;
    if prepared.use_columnar {
        let before = rows.len();
        let (out_rows, dup_removed) = apply_dedup_sort_columnar_v1(
            rows,
            &prepared.deduplicate_by,
            &prepared.dedup_keep,
            &prepared.sort_by,
        );
        rows = out_rows;
        duplicate_rows_removed = if !prepared.deduplicate_by.is_empty() {
            dup_removed
        } else {
            before.saturating_sub(rows.len())
        };
    } else {
        if !prepared.deduplicate_by.is_empty() {
            let mut deduped: HashMap<String, Map<String, Value>> = HashMap::new();
            if prepared.dedup_keep == "first" {
                for row in rows {
                    let key = dedup_key(&row, &prepared.deduplicate_by);
                    if deduped.contains_key(&key) {
                        push_reason_sample(
                            &mut reason_samples,
                            "duplicate_removed",
                            prepared.audit_sample_limit,
                            json!({
                                "reason_code": "duplicate_removed",
                                "row_index": Value::Null,
                                "field": Value::Null,
                                "key": key.clone(),
                                "row_excerpt": row_excerpt(&row),
                                "detail": "duplicate removed while keeping first"
                            }),
                        );
                    } else {
                        deduped.insert(key, row);
                    }
                }
            } else {
                for row in rows {
                    let key = dedup_key(&row, &prepared.deduplicate_by);
                    if let Some(previous) = deduped.get(&key) {
                        push_reason_sample(
                            &mut reason_samples,
                            "duplicate_removed",
                            prepared.audit_sample_limit,
                            json!({
                                "reason_code": "duplicate_removed",
                                "row_index": Value::Null,
                                "field": Value::Null,
                                "key": key.clone(),
                                "row_excerpt": row_excerpt(previous),
                                "detail": "duplicate removed while keeping last"
                            }),
                        );
                    }
                    deduped.insert(key, row);
                }
            }
            let out_len = deduped.len();
            duplicate_rows_removed = prepared
                .input_rows
                .saturating_sub(invalid_rows + filtered_rows + out_len);
            rows = deduped.into_values().collect();
        }

        if !prepared.sort_by.is_empty() {
            rows.sort_by(|a, b| compare_rows(a, b, &prepared.sort_by));
        }
    }

    Ok(TransformExecution {
        rows,
        invalid_rows,
        filtered_rows,
        duplicate_rows_removed,
        numeric_cells_total,
        numeric_cells_parsed,
        date_cells_total,
        date_cells_parsed,
        rule_hits,
        reason_samples,
    })
}
