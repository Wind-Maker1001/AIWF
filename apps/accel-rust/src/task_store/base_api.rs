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

pub(super) fn base_api_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) -> Result<(), String> {
    let Some(base) = &cfg.base_api_url else {
        return Err("base_api task store requires AIWF_BASE_URL".to_string());
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
    req.send_json(payload)
        .map_err(|e| format!("base_api upsert task: {e}"))?;
    Ok(())
}

pub(super) fn base_api_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Result<Option<TaskState>, String> {
    let Some(base) = &cfg.base_api_url else {
        return Err("base_api task store requires AIWF_BASE_URL".to_string());
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
    let resp = match req.call() {
        Ok(resp) => resp,
        Err(ureq::Error::Status(404, _)) => return Ok(None),
        Err(e) => return Err(format!("base_api get task: {e}")),
    };
    let body: Value = resp
        .into_json()
        .map_err(|e| format!("base_api get task json: {e}"))?;
    let Some(task) = body.get("task") else {
        return Ok(None);
    };
    parse_task_from_runtime_row(task)
        .map(Some)
        .ok_or_else(|| "base_api get task: invalid task payload".to_string())
}

pub(super) fn base_api_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Result<Option<Value>, String> {
    let Some(base) = &cfg.base_api_url else {
        return Err("base_api task store requires AIWF_BASE_URL".to_string());
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
    let resp = match req.send_string("{}") {
        Ok(resp) => resp,
        Err(ureq::Error::Status(404, _)) => return Ok(None),
        Err(e) => return Err(format!("base_api cancel task: {e}")),
    };
    let body = resp
        .into_json()
        .map_err(|e| format!("base_api cancel task json: {e}"))?;
    Ok(Some(body))
}
