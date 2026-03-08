use crate::{
    TransformRowsReq,
    api_types::{
        ExplainPlanV1Req, OptimizerV1Req, SaveRowsReq, SaveRowsResp, TransformRowsStreamReq,
        TransformRowsStreamResp,
    },
    execution_ops::planning::run_optimizer_v1,
    load_kv_store, load_rows_from_uri_limited, read_stream_checkpoint,
    row_io::{
        save_rows_csv, save_rows_jsonl, save_rows_parquet_payload, save_rows_parquet_typed,
        save_rows_sqlite, save_rows_sqlserver, save_rows_to_uri,
    },
    run_transform_rows_v2, save_kv_store,
    transform_support::{
        tenant_max_payload_bytes, tenant_max_rows, utc_now_iso, value_to_f64, value_to_string,
    },
    write_stream_checkpoint,
};
use serde_json::{Value, json};

pub(crate) fn run_explain_plan_v1(req: ExplainPlanV1Req) -> Result<Value, String> {
    let row_count = req.rows.as_ref().map(|r| r.len()).unwrap_or(0);
    let feedback = load_kv_store(&crate::explain_feedback_store_path());
    let opt = run_optimizer_v1(OptimizerV1Req {
        run_id: req.run_id.clone(),
        rows: req.rows.clone(),
        row_count_hint: Some(row_count),
        prefer_arrow: Some(true),
        join_hint: None,
        aggregate_hint: None,
    })?;
    let mut total = 0.0f64;
    let mut steps = Vec::new();
    for (i, s) in req.steps.iter().enumerate() {
        let op = s
            .as_object()
            .and_then(|o| o.get("operator"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let hist_factor = feedback
            .get(&op)
            .and_then(|v| v.get("scale"))
            .and_then(value_to_f64)
            .unwrap_or(1.0)
            .clamp(0.3, 3.0);
        let base = if op.contains("join") {
            5.0
        } else if op.contains("aggregate") {
            4.0
        } else if op.contains("quality") {
            3.0
        } else if op.contains("load") || op.contains("save") {
            2.0
        } else {
            1.0
        };
        let scale = 1.0 + (row_count as f64 / 50_000.0);
        let cost = base * scale * hist_factor;
        total += cost;
        steps.push(json!({
            "idx": i + 1,
            "operator": op,
            "estimated_cost": (cost * 100.0).round() / 100.0,
            "history_scale": hist_factor
        }));
    }
    let mut feedback_updates = Vec::new();
    if let Some(actuals) = req.actual_stats.as_ref() {
        let mut store = load_kv_store(&crate::explain_feedback_store_path());
        for a in actuals {
            let Some(o) = a.as_object() else {
                continue;
            };
            let op = o
                .get("operator")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if op.is_empty() {
                continue;
            }
            let est = o.get("estimated_ms").and_then(value_to_f64).unwrap_or(0.0);
            let act = o.get("actual_ms").and_then(value_to_f64).unwrap_or(0.0);
            if est <= 0.0 || act <= 0.0 {
                continue;
            }
            let ratio = (act / est).clamp(0.2, 5.0);
            let prev = store
                .get(&op)
                .and_then(|v| v.get("scale"))
                .and_then(value_to_f64)
                .unwrap_or(1.0);
            let next = (0.7 * prev + 0.3 * ratio).clamp(0.3, 3.0);
            store.insert(
                op.clone(),
                json!({"scale": next, "updated_at": utc_now_iso()}),
            );
            feedback_updates
                .push(json!({"operator": op, "prev": prev, "next": next, "ratio": ratio}));
        }
        if req.persist_feedback.unwrap_or(true) {
            save_kv_store(&crate::explain_feedback_store_path(), &store)?;
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "explain_plan_v1",
        "status": "done",
        "run_id": req.run_id,
        "estimated_total_cost": (total * 100.0).round() / 100.0,
        "optimizer_plan": opt.get("plan").cloned().unwrap_or_else(|| json!({})),
        "steps": steps,
        "feedback_updates": feedback_updates
    }))
}

pub(crate) fn run_save_rows_v1(req: SaveRowsReq) -> Result<SaveRowsResp, String> {
    let st = req.sink_type.to_lowercase();
    match st.as_str() {
        "jsonl" => save_rows_jsonl(&req.sink, &req.rows)?,
        "csv" => save_rows_csv(&req.sink, &req.rows)?,
        "sqlite" => save_rows_sqlite(&req.sink, req.table.as_deref().unwrap_or("data"), &req.rows)?,
        "sqlserver" => save_rows_sqlserver(
            &req.sink,
            req.table.as_deref().unwrap_or("dbo.aiwf_rows"),
            &req.rows,
        )?,
        "parquet" => {
            let mode = req
                .parquet_mode
                .as_deref()
                .unwrap_or("typed")
                .trim()
                .to_lowercase();
            if mode == "payload" {
                save_rows_parquet_payload(&req.sink, &req.rows)?;
            } else {
                save_rows_parquet_typed(&req.sink, &req.rows)?;
            }
        }
        _ => return Err(format!("unsupported sink_type: {}", req.sink_type)),
    }
    Ok(SaveRowsResp {
        ok: true,
        operator: "save_rows_v1".to_string(),
        status: "done".to_string(),
        written_rows: req.rows.len(),
    })
}

pub(crate) fn run_transform_rows_v2_stream(
    req: TransformRowsStreamReq,
) -> Result<TransformRowsStreamResp, String> {
    let chunk_size = req.chunk_size.unwrap_or(2000).max(1);
    let max_chunks_per_run = req.max_chunks_per_run.unwrap_or(usize::MAX).max(1);
    let mut rows_in = if let Some(rows) = req.rows.clone() {
        rows
    } else if let Some(uri) = req.input_uri.clone() {
        load_rows_from_uri_limited(&uri, tenant_max_rows(), tenant_max_payload_bytes())?
    } else {
        return Err("rows or input_uri is required".to_string());
    };
    let mut watermark_dropped = 0usize;
    if let (Some(field), Some(wv)) = (req.watermark_field.as_ref(), req.watermark_value.as_ref()) {
        let w_num = value_to_f64(wv);
        rows_in.retain(|r| {
            let Some(obj) = r.as_object() else {
                return false;
            };
            let Some(cur) = obj.get(field) else {
                return false;
            };
            let keep = match (value_to_f64(cur), w_num) {
                (Some(a), Some(b)) => a > b,
                _ => value_to_string(cur) > value_to_string(wv),
            };
            if !keep {
                watermark_dropped += 1;
            }
            keep
        });
    }
    let mut start_chunk = 0usize;
    if req.resume.unwrap_or(false)
        && let Some(key) = req.checkpoint_key.as_deref()
        && let Some(cp) = read_stream_checkpoint(key)?
    {
        start_chunk = cp.saturating_add(1);
    }
    let mut merged_rows: Vec<Value> = Vec::new();
    let mut chunks = 0usize;
    let mut total_input = 0usize;
    let mut total_output = 0usize;
    let mut has_more = false;
    let mut next_checkpoint: Option<usize> = None;
    for (chunk_idx, chunk) in rows_in.chunks(chunk_size).enumerate() {
        if chunk_idx < start_chunk {
            continue;
        }
        if chunks >= max_chunks_per_run {
            has_more = true;
            break;
        }
        chunks += 1;
        total_input += chunk.len();
        let part_req = TransformRowsReq {
            run_id: req.run_id.clone(),
            tenant_id: req.tenant_id.clone(),
            trace_id: None,
            traceparent: None,
            rows: Some(chunk.to_vec()),
            rules: req.rules.clone(),
            rules_dsl: req.rules_dsl.clone(),
            quality_gates: req.quality_gates.clone(),
            schema_hint: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: None,
        };
        let out = run_transform_rows_v2(part_req)?;
        total_output += out.rows.len();
        merged_rows.extend(out.rows);
        if let Some(key) = req.checkpoint_key.as_deref() {
            write_stream_checkpoint(key, chunk_idx)?;
            next_checkpoint = Some(chunk_idx);
        }
    }
    if let Some(uri) = req.output_uri.as_deref() {
        save_rows_to_uri(uri, &merged_rows)?;
    }
    Ok(TransformRowsStreamResp {
        ok: true,
        operator: "transform_rows_v2_stream".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        rows: merged_rows,
        chunks,
        has_more,
        next_checkpoint,
        stats: json!({
            "input_rows": total_input,
            "output_rows": total_output,
            "chunk_size": chunk_size,
            "max_chunks_per_run": if max_chunks_per_run == usize::MAX { Value::Null } else { json!(max_chunks_per_run) },
            "resumed_from_chunk": start_chunk,
            "watermark_field": req.watermark_field,
            "watermark_dropped_rows": watermark_dropped,
            "checkpoint_key": req.checkpoint_key
        }),
    })
}
