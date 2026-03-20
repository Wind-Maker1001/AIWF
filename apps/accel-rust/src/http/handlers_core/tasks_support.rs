use accel_rust::{
    app_state::{AppState, TaskState, TaskStoreConfig},
    task_store::{persist_tasks_to_store, prune_tasks},
};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::sync::atomic::Ordering;

use crate::transform_support::{can_cancel_status, utc_now_iso};

pub(super) fn mark_cancel_flag(state: &AppState, task_id: &str) {
    if let Ok(flags) = state.cancel_flags.lock()
        && let Some(flag) = flags.get(task_id)
    {
        flag.store(true, Ordering::Relaxed);
    }
}

pub(super) fn task_not_found_response(task_id: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"ok": false, "error": "task_not_found", "task_id": task_id})),
    )
        .into_response()
}

pub(super) fn task_store_unavailable_response(task_id: &str, detail: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"ok": false, "error": "task_store_unavailable", "detail": detail, "task_id": task_id})),
    )
        .into_response()
}

pub(super) fn cache_remote_task(state: &AppState, task_id: &str, remote: &TaskState, cfg: &TaskStoreConfig) {
    if let Ok(mut tasks) = state.tasks.lock() {
        tasks.insert(task_id.to_string(), remote.clone());
        let _ = prune_tasks(&mut tasks, cfg);
        persist_tasks_to_store(&tasks, cfg.store_path.as_ref());
    }
}

pub(super) fn load_local_task(state: &AppState, task_id: &str, cfg: &TaskStoreConfig) -> Option<TaskState> {
    if let Ok(mut tasks) = state.tasks.lock() {
        let removed = prune_tasks(&mut tasks, cfg);
        if removed > 0 {
            persist_tasks_to_store(&tasks, cfg.store_path.as_ref());
        }
        return tasks.get(task_id).cloned();
    }
    None
}

pub(super) fn apply_remote_cancel_result(
    state: &AppState,
    task_id: &str,
    value: &Value,
    cfg: &TaskStoreConfig,
) -> bool {
    if let Ok(mut tasks) = state.tasks.lock()
        && let Some(current) = tasks.get_mut(task_id)
    {
        if let Some(status) = value.get("status").and_then(|item| item.as_str()) {
            current.status = status.to_string();
            current.updated_at = utc_now_iso();
        }
        let _ = prune_tasks(&mut tasks, cfg);
        persist_tasks_to_store(&tasks, cfg.store_path.as_ref());
    }
    if value.get("cancelled").and_then(|item| item.as_bool()) == Some(true) {
        mark_cancel_flag(state, task_id);
        if let Ok(mut metrics) = state.metrics.lock() {
            metrics.task_cancel_effective_total += 1;
        }
        return true;
    }
    false
}

pub(super) fn cancel_local_task(state: &AppState, task_id: &str, cfg: &TaskStoreConfig) -> Option<(bool, String)> {
    let mut cancelled = false;
    let mut status = "not_found".to_string();
    if let Ok(mut tasks) = state.tasks.lock() {
        let removed = prune_tasks(&mut tasks, cfg);
        if removed > 0 {
            persist_tasks_to_store(&tasks, cfg.store_path.as_ref());
        }
        if let Some(current) = tasks.get_mut(task_id) {
            status = current.status.clone();
            if can_cancel_status(&status) {
                current.status = "cancelled".to_string();
                current.updated_at = utc_now_iso();
                mark_cancel_flag(state, task_id);
                status = current.status.clone();
                cancelled = true;
                if let Ok(mut metrics) = state.metrics.lock() {
                    metrics.task_cancel_effective_total += 1;
                }
            }
            persist_tasks_to_store(&tasks, cfg.store_path.as_ref());
        }
    }
    if status == "not_found" {
        return None;
    }
    Some((cancelled, status))
}
