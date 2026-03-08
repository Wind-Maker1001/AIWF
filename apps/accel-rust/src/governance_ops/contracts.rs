use crate::*;

pub(crate) fn run_runtime_stats_v1(req: RuntimeStatsV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let mut store = load_kv_store(&runtime_stats_store_path());
    if op == "record" {
        let operator = req.operator.unwrap_or_else(|| "unknown".to_string());
        let entry = store.entry(operator.clone()).or_insert(json!({
            "calls": 0u64, "ok": 0u64, "err": 0u64, "durations": [], "rows_in": 0u64, "rows_out": 0u64, "errors": {}
        }));
        let obj = entry
            .as_object_mut()
            .ok_or_else(|| "runtime_stats_v1 bad entry".to_string())?;
        let calls = obj.get("calls").and_then(|v| v.as_u64()).unwrap_or(0) + 1;
        let ok =
            obj.get("ok").and_then(|v| v.as_u64()).unwrap_or(0) + u64::from(req.ok.unwrap_or(true));
        let err = obj.get("err").and_then(|v| v.as_u64()).unwrap_or(0)
            + u64::from(!req.ok.unwrap_or(true));
        let rows_in = obj.get("rows_in").and_then(|v| v.as_u64()).unwrap_or(0)
            + req.rows_in.unwrap_or(0) as u64;
        let rows_out = obj.get("rows_out").and_then(|v| v.as_u64()).unwrap_or(0)
            + req.rows_out.unwrap_or(0) as u64;
        let mut durs = obj
            .get("durations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        if let Some(d) = req.duration_ms {
            durs.push(json!(d as u64));
            if durs.len() > 500 {
                durs = durs.split_off(durs.len() - 500);
            }
        }
        let mut errs = obj
            .get("errors")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        if let Some(ec) = req.error_code {
            let n = errs.get(&ec).and_then(|v| v.as_u64()).unwrap_or(0) + 1;
            errs.insert(ec, json!(n));
        }
        obj.insert("calls".to_string(), json!(calls));
        obj.insert("ok".to_string(), json!(ok));
        obj.insert("err".to_string(), json!(err));
        obj.insert("rows_in".to_string(), json!(rows_in));
        obj.insert("rows_out".to_string(), json!(rows_out));
        obj.insert("durations".to_string(), Value::Array(durs));
        obj.insert("errors".to_string(), Value::Object(errs));
        save_kv_store(&runtime_stats_store_path(), &store)?;
        return Ok(
            json!({"ok": true, "operator":"runtime_stats_v1", "status":"done", "run_id": req.run_id, "op":"record", "target": operator}),
        );
    }
    if op == "reset" {
        store.clear();
        save_kv_store(&runtime_stats_store_path(), &store)?;
        return Ok(
            json!({"ok": true, "operator":"runtime_stats_v1", "status":"done", "run_id": req.run_id, "op":"reset"}),
        );
    }
    let mut items = Vec::new();
    for (k, v) in store {
        let durs = v
            .get("durations")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|x| x.as_u64())
            .collect::<Vec<_>>();
        let mut s = durs.clone();
        s.sort();
        let p50 = if s.is_empty() { 0 } else { s[s.len() / 2] };
        let p95 = if s.is_empty() {
            0
        } else {
            s[((s.len() as f64 * 0.95).floor() as usize).min(s.len() - 1)]
        };
        items.push(json!({
            "operator": k,
            "calls": v.get("calls").and_then(|x| x.as_u64()).unwrap_or(0),
            "ok": v.get("ok").and_then(|x| x.as_u64()).unwrap_or(0),
            "err": v.get("err").and_then(|x| x.as_u64()).unwrap_or(0),
            "rows_in": v.get("rows_in").and_then(|x| x.as_u64()).unwrap_or(0),
            "rows_out": v.get("rows_out").and_then(|x| x.as_u64()).unwrap_or(0),
            "p50_ms": p50,
            "p95_ms": p95,
            "errors": v.get("errors").cloned().unwrap_or_else(|| json!({}))
        }));
    }
    items.sort_by(|a, b| {
        let av = a.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        let bv = b.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        av.cmp(bv)
    });
    Ok(
        json!({"ok": true, "operator":"runtime_stats_v1", "status":"done", "run_id": req.run_id, "op":"summary", "items": items}),
    )
}

pub(crate) fn run_capabilities_v1(req: CapabilitiesV1Req) -> Result<Value, String> {
    let mut ops = vec![
        json!({"operator":"transform_rows_v3","version":"v3","streaming":true,"cache":true,"checkpoint":true,"io_contract":true}),
        json!({"operator":"load_rows_v3","version":"v3","streaming":true,"cache":false,"checkpoint":true,"io_contract":true}),
        json!({"operator":"join_rows_v4","version":"v4","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"aggregate_rows_v4","version":"v4","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"quality_check_v4","version":"v4","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"stream_window_v2","version":"v2","streaming":true,"cache":false,"checkpoint":true,"io_contract":true}),
        json!({"operator":"stream_state_v2","version":"v2","streaming":true,"cache":false,"checkpoint":true,"io_contract":true}),
        json!({"operator":"columnar_eval_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"runtime_stats_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"explain_plan_v2","version":"v2","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"finance_ratio_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"anomaly_explain_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"plugin_operator_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"capabilities_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"io_contract_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"failure_policy_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"incremental_plan_v1","version":"v1","streaming":false,"cache":true,"checkpoint":true,"io_contract":true}),
        json!({"operator":"tenant_isolation_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"operator_policy_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"optimizer_adaptive_v2","version":"v2","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"vector_index_v2_build","version":"v2","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"vector_index_v2_search","version":"v2","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"stream_reliability_v1","version":"v1","streaming":true,"cache":false,"checkpoint":true,"io_contract":true}),
        json!({"operator":"lineage_provenance_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"contract_regression_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"perf_baseline_v1","version":"v1","streaming":false,"cache":true,"checkpoint":false,"io_contract":true}),
    ];
    if let Some(allow) = req.include_ops {
        let set: HashSet<String> = allow
            .into_iter()
            .map(|x| x.trim().to_lowercase())
            .filter(|x| !x.is_empty())
            .collect();
        if !set.is_empty() {
            ops.retain(|x| {
                x.get("operator")
                    .and_then(|v| v.as_str())
                    .map(|s| set.contains(&s.to_lowercase()))
                    .unwrap_or(false)
            });
        }
    }
    ops.sort_by(|a, b| {
        let av = a.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        let bv = b.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        av.cmp(bv)
    });
    Ok(json!({
        "ok": true,
        "operator": "capabilities_v1",
        "status": "done",
        "run_id": req.run_id,
        "schema_version": "aiwf.capabilities.v1",
        "items": ops
    }))
}

pub(crate) fn io_contract_errors(operator: &str, input: &Value, strict: bool) -> Vec<String> {
    let mut errs = Vec::new();
    let op = operator.trim().to_lowercase();
    let obj = if let Some(o) = input.as_object() {
        o
    } else {
        errs.push("input must be object".to_string());
        return errs;
    };
    let require_rows_or_uri = |errs: &mut Vec<String>, o: &Map<String, Value>| {
        let has_rows = o
            .get("rows")
            .and_then(|v| v.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        let has_uri = o
            .get("input_uri")
            .and_then(|v| v.as_str())
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        if !has_rows && !has_uri {
            errs.push("requires rows[] or input_uri".to_string());
        }
    };
    match op.as_str() {
        "transform_rows_v2" | "transform_rows_v3" | "load_rows_v3" => {
            require_rows_or_uri(&mut errs, obj)
        }
        "finance_ratio_v1" => {
            if obj.get("rows").and_then(|v| v.as_array()).is_none() {
                errs.push("finance_ratio_v1 requires rows[]".to_string());
            }
        }
        "anomaly_explain_v1" => {
            if obj.get("rows").and_then(|v| v.as_array()).is_none() {
                errs.push("anomaly_explain_v1 requires rows[]".to_string());
            }
            if obj
                .get("score_field")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
            {
                errs.push("anomaly_explain_v1 requires score_field".to_string());
            }
        }
        "stream_window_v2" => {
            if obj
                .get("stream_key")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
            {
                errs.push("stream_window_v2 requires stream_key".to_string());
            }
            if obj
                .get("event_time_field")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
            {
                errs.push("stream_window_v2 requires event_time_field".to_string());
            }
        }
        "plugin_operator_v1" => {
            if obj
                .get("plugin")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
            {
                errs.push("plugin_operator_v1 requires plugin".to_string());
            }
        }
        _ => {
            if strict {
                errs.push(format!("unsupported contract operator: {operator}"));
            }
        }
    }
    errs
}

pub(crate) fn run_io_contract_v1(req: IoContractV1Req) -> Result<Value, String> {
    let strict = req.strict.unwrap_or(false);
    let errors = io_contract_errors(&req.operator, &req.input, strict);
    Ok(json!({
        "ok": true,
        "operator": "io_contract_v1",
        "status": if errors.is_empty() { "done" } else { "invalid" },
        "run_id": req.run_id,
        "target_operator": req.operator,
        "strict": strict,
        "valid": errors.is_empty(),
        "errors": errors
    }))
}

pub(crate) fn classify_failure(error: &str, status_code: Option<u16>) -> (String, bool, String) {
    let e = error.trim().to_lowercase();
    if e.contains("timeout") || e.contains("timed out") {
        return (
            "transient_timeout".to_string(),
            true,
            "retry_with_backoff".to_string(),
        );
    }
    if e.contains("rust_http_5") || status_code.map(|s| s >= 500).unwrap_or(false) {
        return (
            "upstream_5xx".to_string(),
            true,
            "switch_upstream_or_retry".to_string(),
        );
    }
    if e.contains("quota") || e.contains("tenant") {
        return (
            "quota_reject".to_string(),
            false,
            "queue_and_throttle".to_string(),
        );
    }
    if e.contains("sandbox_limit_exceeded") {
        return (
            "sandbox_limit".to_string(),
            true,
            "reduce_payload_or_raise_limit".to_string(),
        );
    }
    if e.contains("schema") || e.contains("invalid") || e.contains("missing") {
        return (
            "input_invalid".to_string(),
            false,
            "fix_input_contract".to_string(),
        );
    }
    if e.contains("egress_blocked") || e.contains("not_allowed") {
        return (
            "policy_blocked".to_string(),
            false,
            "adjust_policy_or_localize".to_string(),
        );
    }
    ("unknown".to_string(), true, "manual_review".to_string())
}

pub(crate) fn run_failure_policy_v1(req: FailurePolicyV1Req) -> Result<Value, String> {
    if req.error.trim().is_empty() {
        return Err("failure_policy_v1 requires non-empty error".to_string());
    }
    let (class, retryable0, action) = classify_failure(&req.error, req.status_code);
    let attempts = req.attempts.unwrap_or(0);
    let max_retries = req.max_retries.unwrap_or(2);
    let retryable = retryable0 && attempts < max_retries;
    Ok(json!({
        "ok": true,
        "operator": "failure_policy_v1",
        "status": "done",
        "run_id": req.run_id,
        "target_operator": req.operator.unwrap_or_default(),
        "class": class,
        "retryable": retryable,
        "attempts": attempts,
        "max_retries": max_retries,
        "recovery_action": action
    }))
}
