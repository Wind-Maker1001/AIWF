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
        rule_hits,
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

    let quality = json!({
        "input_rows": prepared.input_rows,
        "output_rows": output_rows,
        "invalid_rows": invalid_rows,
        "filtered_rows": filtered_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
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
            "rule_hits": rule_hits,
            "engine": prepared.engine,
            "engine_requested": prepared.requested_engine,
            "engine_reason": prepared.engine_reason,
            "estimated_input_bytes": prepared.estimated_bytes,
            "limits": {
                "max_rows": prepared.max_rows,
                "max_payload_bytes": prepared.max_bytes
            }
        }),
    };
    if let Some(uri) = req.output_uri {
        save_rows_to_uri(&uri, &resp.rows)?;
    }
    Ok(resp)
}
