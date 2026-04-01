use super::*;

pub(super) fn finalize_transform_response(
    req: TransformRowsReq,
    prepared: TransformPrepared,
    executed: TransformExecution,
    started: Instant,
) -> Result<TransformRowsResp, String> {
    let TransformExecution {
        rows,
        invalid_rows,
        filtered_rows,
        duplicate_rows_removed,
        numeric_cells_total,
        numeric_cells_parsed,
        date_cells_total,
        date_cells_parsed,
        rule_hits,
        mut reason_samples,
    } = executed;

    let aggregate = compute_aggregate(&rows, rule_get(&prepared.rules, "aggregate"));
    let output_rows = rows.len();
    let gate_required_fields = {
        let from_gate = as_array_str(prepared.gates.get("required_fields"));
        if from_gate.is_empty() {
            prepared.required_fields.clone()
        } else {
            from_gate
        }
    };
    let mut required_missing_by_field: Map<String, Value> = Map::new();
    let mut required_missing_cells = 0usize;
    if !gate_required_fields.is_empty() {
        for field in &gate_required_fields {
            let mut missing = 0usize;
            for row in &rows {
                if is_missing(row.get(field)) {
                    missing += 1;
                }
            }
            required_missing_cells += missing;
            required_missing_by_field.insert(field.clone(), Value::Number((missing as u64).into()));
        }
    }
    let required_total_cells = output_rows.saturating_mul(gate_required_fields.len());
    let required_missing_ratio = if required_total_cells > 0 {
        required_missing_cells as f64 / required_total_cells as f64
    } else {
        0.0
    };
    let numeric_parse_rate = if numeric_cells_total > 0 {
        numeric_cells_parsed as f64 / numeric_cells_total as f64
    } else {
        1.0
    };
    let date_parse_rate = if date_cells_total > 0 {
        date_cells_parsed as f64 / date_cells_total as f64
    } else {
        1.0
    };
    let duplicate_key_ratio = if output_rows + duplicate_rows_removed > 0 {
        duplicate_rows_removed as f64 / (output_rows + duplicate_rows_removed) as f64
    } else {
        0.0
    };
    let blank_output_rows = rows
        .iter()
        .filter(|row| row.values().all(|value| is_missing(Some(value))))
        .count();
    let blank_row_ratio = if output_rows > 0 {
        blank_output_rows as f64 / output_rows as f64
    } else {
        0.0
    };

    let quality = json!({
        "input_rows": prepared.input_rows,
        "output_rows": output_rows,
        "invalid_rows": invalid_rows,
        "filtered_rows": filtered_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "numeric_cells_total": numeric_cells_total,
        "numeric_cells_parsed": numeric_cells_parsed,
        "numeric_parse_rate": numeric_parse_rate,
        "date_cells_total": date_cells_total,
        "date_cells_parsed": date_cells_parsed,
        "date_parse_rate": date_parse_rate,
        "duplicate_key_ratio": duplicate_key_ratio,
        "blank_output_rows": blank_output_rows,
        "blank_row_ratio": blank_row_ratio,
        "required_fields": gate_required_fields,
        "required_missing_cells": required_missing_cells,
        "required_missing_by_field": required_missing_by_field,
        "required_missing_ratio": required_missing_ratio,
    });

    let gate_result = evaluate_quality_gates(&quality, &prepared.gates);
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
                .map(|items| {
                    items
                        .iter()
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
            prepared.input_rows,
            output_rows,
            latency_ms
        ),
    );
    let cast_failed = rule_hits
        .iter()
        .filter(|(key, _)| key.starts_with("cast_fail_"))
        .map(|(_, value)| *value)
        .sum::<usize>();
    let reason_counts = json!({
        "invalid_object": rule_hits.get("invalid_object").copied().unwrap_or(0),
        "cast_failed": cast_failed,
        "required_missing": rule_hits.get("required_missing").copied().unwrap_or(0),
        "filter_rejected": rule_hits.get("filtered_by_rule").copied().unwrap_or(0),
        "duplicate_removed": duplicate_rows_removed,
    });
    for key in [
        "invalid_object",
        "cast_failed",
        "required_missing",
        "filter_rejected",
        "duplicate_removed",
    ] {
        reason_samples.entry(key.to_string()).or_default();
    }
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
            input_rows: prepared.input_rows,
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
            "schema": "transform_rows_v2.audit.v1",
            "rule_hits": rule_hits,
            "reason_counts": reason_counts,
            "reason_samples": reason_samples,
            "engine": prepared.engine,
            "engine_requested": prepared.requested_engine,
            "engine_reason": prepared.engine_reason,
            "estimated_input_bytes": prepared.estimated_bytes,
            "limits": {
                "max_rows": prepared.max_rows,
                "max_payload_bytes": prepared.max_bytes,
                "sample_limit": prepared.audit_sample_limit
            }
        }),
    };
    if let Some(uri) = req.output_uri {
        save_rows_to_uri(&uri, &resp.rows)?;
    }
    Ok(resp)
}
