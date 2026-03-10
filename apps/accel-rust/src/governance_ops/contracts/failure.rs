use super::*;

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
