use super::*;

pub(crate) fn compute_request_signature(req: &TransformRowsReq, secret: &str) -> String {
    let tenant = req
        .tenant_id
        .clone()
        .unwrap_or_else(|| env::var("AIWF_TENANT_ID").unwrap_or_else(|_| "default".to_string()));
    let payload = json!({
        "tenant_id": tenant,
        "run_id": req.run_id,
        "trace_id": req.trace_id,
        "traceparent": req.traceparent,
        "rows": req.rows,
        "rules": req.rules,
        "rules_dsl": req.rules_dsl,
        "quality_gates": req.quality_gates,
        "schema_hint": req.schema_hint,
        "input_uri": req.input_uri,
        "output_uri": req.output_uri,
        "idempotency_key": req.idempotency_key,
    });
    let mut h = Sha256::new();
    h.update(secret.as_bytes());
    h.update(b":");
    h.update(payload.to_string().as_bytes());
    format!("{:x}", h.finalize())
}

pub(crate) fn verify_request_signature(req: &TransformRowsReq) -> Result<(), String> {
    let Ok(secret) = env::var("AIWF_REQUEST_SIGNING_SECRET") else {
        return Ok(());
    };
    if secret.trim().is_empty() {
        return Ok(());
    }
    let expected = compute_request_signature(req, &secret);
    let got = req.request_signature.clone().unwrap_or_default();
    if got.eq_ignore_ascii_case(&expected) {
        Ok(())
    } else {
        Err("invalid request signature".to_string())
    }
}

pub(crate) fn resolve_trace_id(
    explicit: Option<&str>,
    traceparent: Option<&str>,
    seed: &str,
) -> String {
    if let Some(v) = explicit {
        let t = v.trim();
        if t.len() == 32 && t.chars().all(|c| c.is_ascii_hexdigit()) {
            return t.to_lowercase();
        }
    }
    if let Some(tp) = traceparent {
        let p = tp.trim();
        let parts = p.split('-').collect::<Vec<_>>();
        if parts.len() >= 4 {
            let tid = parts[1];
            if tid.len() == 32 && tid.chars().all(|c| c.is_ascii_hexdigit()) {
                return tid.to_lowercase();
            }
        }
    }
    let mut h = Sha256::new();
    h.update(seed.as_bytes());
    format!("{:x}", h.finalize())
}

pub(crate) fn is_cancelled(flag: &Option<Arc<AtomicBool>>) -> bool {
    match flag {
        Some(v) => v.load(Ordering::Relaxed),
        None => false,
    }
}

pub(crate) fn cleanup_task_flag(
    task_id: &str,
    cancel_flags: &Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
    metrics: &Arc<Mutex<ServiceMetrics>>,
) {
    let mut removed = false;
    if let Ok(mut flags) = cancel_flags.lock() {
        removed = flags.remove(task_id).is_some();
    }
    if let Ok(mut m) = metrics.lock()
        && removed
    {
        m.tasks_active = (m.tasks_active - 1).max(0);
        m.task_flag_cleanup_total += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{compute_request_signature, verify_request_signature};
    use crate::operators::transform::TransformRowsReq;
    use serde_json::json;
    use std::env;

    fn sample_req() -> TransformRowsReq {
        TransformRowsReq {
            run_id: Some("run-1".to_string()),
            tenant_id: Some("tenant-a".to_string()),
            trace_id: Some("trace-1".to_string()),
            traceparent: None,
            rows: Some(vec![json!({"id": 1, "amount": "10.5"})]),
            rules: Some(json!({"casts":{"id":"int"}})),
            rules_dsl: None,
            quality_gates: Some(json!({"min_output_rows": 1})),
            schema_hint: None,
            input_uri: Some("input.csv".to_string()),
            output_uri: Some("output.parquet".to_string()),
            request_signature: None,
            idempotency_key: Some("idem-1".to_string()),
        }
    }

    #[test]
    fn compute_request_signature_changes_when_payload_changes() {
        let first = compute_request_signature(&sample_req(), "secret");
        let mut changed = sample_req();
        changed.rows = Some(vec![json!({"id": 2, "amount": "99.9"})]);
        let second = compute_request_signature(&changed, "secret");
        assert_ne!(first, second);
    }

    #[test]
    fn verify_request_signature_rejects_payload_tampering() {
        let prev = env::var("AIWF_REQUEST_SIGNING_SECRET").ok();
        unsafe {
            env::set_var("AIWF_REQUEST_SIGNING_SECRET", "secret");
        }
        let mut req = sample_req();
        req.request_signature = Some(compute_request_signature(&req, "secret"));
        req.rows = Some(vec![json!({"id": 3, "amount": "5.0"})]);
        let result = verify_request_signature(&req);
        if let Some(value) = prev {
            unsafe {
                env::set_var("AIWF_REQUEST_SIGNING_SECRET", value);
            }
        } else {
            unsafe {
                env::remove_var("AIWF_REQUEST_SIGNING_SECRET");
            }
        }
        assert!(result.is_err());
    }
}
