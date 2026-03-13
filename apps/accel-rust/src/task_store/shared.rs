use super::*;
use chrono::DateTime;

pub(super) fn task_time_epoch(value: &str) -> Option<u64> {
    let raw = value.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(epoch) = raw.parse::<u64>() {
        return Some(epoch);
    }
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.timestamp().max(0) as u64)
}

pub(super) fn task_epoch(task: &TaskState) -> u64 {
    task.updated_at
        .as_str()
        .pipe(task_time_epoch)
        .or_else(|| task_time_epoch(&task.created_at))
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

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::task_time_epoch;

    #[test]
    fn task_time_epoch_accepts_epoch_string() {
        assert_eq!(task_time_epoch("1710000000"), Some(1710000000));
    }

    #[test]
    fn task_time_epoch_accepts_rfc3339_string() {
        assert_eq!(task_time_epoch("2026-03-12T12:34:56Z"), Some(1773318896));
    }
}
