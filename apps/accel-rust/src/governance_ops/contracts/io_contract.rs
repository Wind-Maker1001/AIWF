use super::*;

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
