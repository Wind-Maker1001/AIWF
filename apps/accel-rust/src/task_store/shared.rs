use super::*;

pub(super) fn task_epoch(task: &TaskState) -> u64 {
    task.updated_at
        .parse::<u64>()
        .ok()
        .or_else(|| task.created_at.parse::<u64>().ok())
        .unwrap_or(0)
}

pub(super) fn utc_now_epoch_string() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{ts}")
}

pub(super) fn parse_task_from_runtime_row(task: &Value) -> Option<TaskState> {
    let task_id = task.get("task_id")?.as_str()?.to_string();
    let tenant_id = task
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    let operator = task
        .get("operator")
        .and_then(|v| v.as_str())
        .unwrap_or("transform_rows_v2")
        .to_string();
    let status = task
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("queued")
        .to_string();
    let created_at = task
        .get("created_at_epoch")
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        .to_string();
    let updated_at = task
        .get("updated_at_epoch")
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        .to_string();
    let result = task
        .get("result_json")
        .and_then(|v| v.as_str())
        .and_then(|s| serde_json::from_str::<Value>(s).ok());
    let error = task
        .get("error")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let idempotency_key = task
        .get("idempotency_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let attempts = task.get("attempts").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    Some(TaskState {
        task_id,
        tenant_id,
        operator,
        status,
        created_at,
        updated_at,
        result,
        error,
        idempotency_key,
        attempts,
    })
}
