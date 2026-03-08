use crate::*;

pub(crate) async fn get_task_operator(
    State(state): State<AppState>,
    AxPath(task_id): AxPath<String>,
) -> impl IntoResponse {
    let cfg = current_task_cfg(&state);
    if task_store_remote_enabled(&cfg)
        && let Some(remote) = task_store_get_task(&task_id, &cfg)
    {
        if let Ok(mut t) = state.tasks.lock() {
            t.insert(task_id.clone(), remote.clone());
            let _ = prune_tasks(&mut t, &cfg);
            persist_tasks_to_store(&t, cfg.store_path.as_ref());
        }
        return (StatusCode::OK, Json(remote)).into_response();
    }

    let out = if let Ok(mut t) = state.tasks.lock() {
        let removed = prune_tasks(&mut t, &cfg);
        if removed > 0 {
            persist_tasks_to_store(&t, cfg.store_path.as_ref());
        }
        t.get(&task_id).cloned()
    } else {
        None
    };
    match out {
        Some(v) => (StatusCode::OK, Json(v)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"ok": false, "error": "task_not_found", "task_id": task_id})),
        )
            .into_response(),
    }
}

pub(crate) async fn cancel_task_operator(
    State(state): State<AppState>,
    AxPath(task_id): AxPath<String>,
) -> impl IntoResponse {
    let cfg = current_task_cfg(&state);
    if let Ok(mut m) = state.metrics.lock() {
        m.task_cancel_requested_total += 1;
    }
    if let Ok(flags) = state.cancel_flags.lock()
        && let Some(flag) = flags.get(&task_id)
    {
        flag.store(true, Ordering::Relaxed);
    }

    if task_store_remote_enabled(&cfg)
        && let Some(v) = task_store_cancel_task(&task_id, &cfg)
    {
        if v.get("ok").and_then(|x| x.as_bool()) == Some(true)
            && let Some(task) = task_store_get_task(&task_id, &cfg)
            && let Ok(mut t) = state.tasks.lock()
        {
            t.insert(task_id.clone(), task);
            let _ = prune_tasks(&mut t, &cfg);
            persist_tasks_to_store(&t, cfg.store_path.as_ref());
        }
        if v.get("cancelled").and_then(|x| x.as_bool()) == Some(true)
            && let Ok(mut m) = state.metrics.lock()
        {
            m.task_cancel_effective_total += 1;
        }
        return (StatusCode::OK, Json(v)).into_response();
    }

    let mut cancelled = false;
    let mut status = "not_found".to_string();
    if let Ok(mut t) = state.tasks.lock() {
        let removed = prune_tasks(&mut t, &cfg);
        if removed > 0 {
            persist_tasks_to_store(&t, cfg.store_path.as_ref());
        }
        if let Some(cur) = t.get_mut(&task_id) {
            status = cur.status.clone();
            if can_cancel_status(&status) {
                cur.status = "cancelled".to_string();
                cur.updated_at = utc_now_iso();
                status = cur.status.clone();
                cancelled = true;
                if let Ok(mut m) = state.metrics.lock() {
                    m.task_cancel_effective_total += 1;
                }
            }
            persist_tasks_to_store(&t, cfg.store_path.as_ref());
        }
    }
    if status == "not_found" {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"ok": false, "error": "task_not_found", "task_id": task_id})),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "task_id": task_id,
            "cancelled": cancelled,
            "status": status
        })),
    )
        .into_response()
}
