use super::*;

pub(crate) fn run_lineage_provenance_v1(req: LineageProvenanceV1Req) -> Result<Value, String> {
    let lineage = run_lineage_v3(LineageV3Req {
        run_id: req.run_id.clone(),
        rules: req.rules,
        computed_fields_v3: req.computed_fields_v3,
        workflow_steps: req.workflow_steps,
        rows: req.rows,
    })?;
    let prov = run_provenance_sign_v1(ProvenanceSignReq {
        run_id: req.run_id.clone(),
        payload: req.payload.unwrap_or_else(|| lineage.clone()),
        prev_hash: req.prev_hash,
    })?;
    Ok(json!({
        "ok": true,
        "operator": "lineage_provenance_v1",
        "status": "done",
        "run_id": req.run_id,
        "lineage": lineage,
        "provenance": prov
    }))
}

pub(crate) fn run_contract_regression_v1(req: ContractRegressionV1Req) -> Result<Value, String> {
    let operators = req.operators.unwrap_or_else(|| {
        vec![
            "transform_rows_v3".to_string(),
            "finance_ratio_v1".to_string(),
            "anomaly_explain_v1".to_string(),
            "stream_window_v2".to_string(),
            "plugin_operator_v1".to_string(),
        ]
    });
    let mut cases = Vec::new();
    for op in operators {
        let sample = match op.as_str() {
            "finance_ratio_v1" => json!({"rows":[{"assets":100.0,"liabilities":50.0}]}),
            "anomaly_explain_v1" => {
                json!({"rows":[{"score":0.9}], "score_field":"score","threshold":0.8})
            }
            "stream_window_v2" => {
                json!({"stream_key":"s1","rows":[{"ts":"2025-01-01","value":1}],"event_time_field":"ts","window_ms":60000})
            }
            "plugin_operator_v1" => json!({"plugin":"demo","op":"run","payload":{}}),
            _ => json!({"rows":[{"id":"1","amount":"10"}]}),
        };
        let valid = io_contract_errors(&op, &sample, false).is_empty();
        cases.push(json!({"operator":op,"sample_input":sample,"expect_valid":valid}));
    }
    Ok(
        json!({"ok": true, "operator":"contract_regression_v1","status":"done","run_id":req.run_id,"cases":cases}),
    )
}

pub(crate) fn run_perf_baseline_v1(req: PerfBaselineV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let name = req
        .operator
        .unwrap_or_else(|| "transform_rows_v3".to_string());
    let mut store = load_kv_store(&perf_baseline_store_path());
    if op == "set" {
        let p95 = req.p95_ms.unwrap_or(500);
        store.insert(
            name.clone(),
            json!({"p95_ms": p95, "updated_at": utc_now_iso()}),
        );
        save_kv_store(&perf_baseline_store_path(), &store)?;
        return Ok(
            json!({"ok": true, "operator":"perf_baseline_v1","status":"done","run_id":req.run_id,"target_operator":name,"baseline_p95_ms":p95}),
        );
    }
    if op == "check" {
        let baseline = store
            .get(&name)
            .and_then(|v| v.get("p95_ms"))
            .and_then(|v| v.as_u64())
            .unwrap_or(500) as u128;
        let current = req.max_p95_ms.unwrap_or(baseline);
        let passed = current <= baseline;
        return Ok(
            json!({"ok": true, "operator":"perf_baseline_v1","status":"done","run_id":req.run_id,"target_operator":name,"baseline_p95_ms":baseline,"current_p95_ms":current,"passed":passed}),
        );
    }
    if op == "get" {
        return Ok(
            json!({"ok": true, "operator":"perf_baseline_v1","status":"done","run_id":req.run_id,"items":store}),
        );
    }
    Err(format!("perf_baseline_v1 unsupported op: {op}"))
}
