use super::*;

pub(crate) fn verify_request_signature(req: &TransformRowsReq) -> Result<(), String> {
    let Ok(secret) = env::var("AIWF_REQUEST_SIGNING_SECRET") else {
        return Ok(());
    };
    if secret.trim().is_empty() {
        return Ok(());
    }
    let run_id = req.run_id.clone().unwrap_or_default();
    let tenant = req
        .tenant_id
        .clone()
        .unwrap_or_else(|| env::var("AIWF_TENANT_ID").unwrap_or_else(|_| "default".to_string()));
    let expected = {
        let mut h = Sha256::new();
        h.update(format!("{secret}:{tenant}:{run_id}").as_bytes());
        format!("{:x}", h.finalize())
    };
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
