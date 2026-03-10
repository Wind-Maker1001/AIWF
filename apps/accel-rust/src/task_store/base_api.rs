use super::*;

pub(super) fn base_api_probe_task_store(cfg: &TaskStoreConfig) -> bool {
    let Some(base) = &cfg.base_api_url else {
        return false;
    };
    let url = format!("{}/actuator/health", base.trim_end_matches('/'));
    let mut req = ureq::get(&url);
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let Ok(resp) = req.call() else {
        return false;
    };
    let Ok(v) = resp.into_json::<Value>() else {
        return false;
    };
    v.get("status")
        .and_then(|x| x.as_str())
        .map(|s| s.eq_ignore_ascii_case("UP"))
        .unwrap_or(false)
}

pub(super) fn base_api_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) {
    let Some(base) = &cfg.base_api_url else {
        return;
    };
    let url = format!("{}/api/v1/runtime/tasks/upsert", base.trim_end_matches('/'));
    let payload = json!({
        "task_id": task.task_id,
        "tenant_id": task.tenant_id,
        "operator": task.operator,
        "status": task.status,
        "created_at": task.created_at.parse::<u64>().unwrap_or(0),
        "updated_at": task.updated_at.parse::<u64>().unwrap_or(0),
        "result": task.result.clone(),
        "error": task.error.clone(),
        "idempotency_key": task.idempotency_key,
        "attempts": task.attempts,
        "source": "accel-rust"
    });
    let mut req = ureq::post(&url).set("Content-Type", "application/json");
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let _ = req.send_json(payload);
}

pub(super) fn base_api_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<TaskState> {
    let Some(base) = &cfg.base_api_url else {
        return None;
    };
    let url = format!(
        "{}/api/v1/runtime/tasks/{}",
        base.trim_end_matches('/'),
        task_id
    );
    let mut req = ureq::get(&url);
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let resp = req.call().ok()?;
    let body: Value = resp.into_json().ok()?;
    let task = body.get("task")?;
    parse_task_from_runtime_row(task)
}

pub(super) fn base_api_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<Value> {
    let Some(base) = &cfg.base_api_url else {
        return None;
    };
    let url = format!(
        "{}/api/v1/runtime/tasks/{}/cancel",
        base.trim_end_matches('/'),
        task_id
    );
    let mut req = ureq::post(&url).set("Content-Type", "application/json");
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let resp = req.send_string("{}").ok()?;
    resp.into_json().ok()
}
