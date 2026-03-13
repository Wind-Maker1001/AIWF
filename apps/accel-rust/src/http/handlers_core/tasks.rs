use crate::{
    current_task_cfg,
    transform_support::{can_cancel_status, utc_now_iso},
};
use accel_rust::{
    app_state::AppState,
    task_store::{
        persist_tasks_to_store, prune_tasks, task_store_cancel_task, task_store_get_task,
        task_store_remote_enabled,
    },
};
use axum::{
    Json,
    extract::{Path as AxPath, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::json;
use std::sync::atomic::Ordering;

fn mark_cancel_flag(state: &AppState, task_id: &str) {
    if let Ok(flags) = state.cancel_flags.lock()
        && let Some(flag) = flags.get(task_id)
    {
        flag.store(true, Ordering::Relaxed);
    }
}

pub(crate) async fn get_task_operator(
    State(state): State<AppState>,
    AxPath(task_id): AxPath<String>,
) -> impl IntoResponse {
    let cfg = current_task_cfg(&state);
    if task_store_remote_enabled(&cfg) {
        match task_store_get_task(&task_id, &cfg) {
            Ok(Some(remote)) => {
                if let Ok(mut t) = state.tasks.lock() {
                    t.insert(task_id.clone(), remote.clone());
                    let _ = prune_tasks(&mut t, &cfg);
                    persist_tasks_to_store(&t, cfg.store_path.as_ref());
                }
                return (StatusCode::OK, Json(remote)).into_response();
            }
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"ok": false, "error": "task_not_found", "task_id": task_id})),
                )
                    .into_response();
            }
            Err(e) => {
                tracing::error!("remote task-store get failed task_id={task_id}: {e}");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"ok": false, "error": "task_store_unavailable", "detail": e, "task_id": task_id})),
                )
                    .into_response();
            }
        }
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

    if task_store_remote_enabled(&cfg) {
        match task_store_cancel_task(&task_id, &cfg) {
            Ok(Some(v)) => {
                if let Ok(mut t) = state.tasks.lock()
                    && let Some(cur) = t.get_mut(&task_id)
                {
                    if let Some(status) = v.get("status").and_then(|x| x.as_str()) {
                        cur.status = status.to_string();
                        cur.updated_at = utc_now_iso();
                    }
                    let _ = prune_tasks(&mut t, &cfg);
                    persist_tasks_to_store(&t, cfg.store_path.as_ref());
                }
                if v.get("cancelled").and_then(|x| x.as_bool()) == Some(true)
                {
                    mark_cancel_flag(&state, &task_id);
                    if let Ok(mut m) = state.metrics.lock() {
                    m.task_cancel_effective_total += 1;
                    }
                }
                return (StatusCode::OK, Json(v)).into_response();
            }
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"ok": false, "error": "task_not_found", "task_id": task_id})),
                )
                    .into_response();
            }
            Err(e) => {
                tracing::error!("remote task-store cancel failed task_id={task_id}: {e}");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"ok": false, "error": "task_store_unavailable", "detail": e, "task_id": task_id})),
                )
                    .into_response();
            }
        }
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
                mark_cancel_flag(&state, &task_id);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::routes::build_router;
    use accel_rust::app_state::{ServiceMetrics, TaskState, TaskStoreConfig};
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex, atomic::AtomicBool},
    };
    use tower::ServiceExt;

    fn remote_base_api_state(base_api_url: &str) -> (AppState, Arc<Mutex<HashMap<String, TaskState>>>) {
        let tasks = Arc::new(Mutex::new(HashMap::<String, TaskState>::new()));
        let state = AppState {
            service: "accel-rust".to_string(),
            tasks: Arc::clone(&tasks),
            metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
            task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
                ttl_sec: 3600,
                max_tasks: 1000,
                store_path: None,
                remote_enabled: true,
                backend: "base_api".to_string(),
                base_api_url: Some(base_api_url.to_string()),
                base_api_key: None,
                sql_host: "127.0.0.1".to_string(),
                sql_port: 1433,
                sql_db: "AIWF".to_string(),
                sql_user: None,
                sql_password: None,
                sql_use_windows_auth: false,
            })),
            cancel_flags: Arc::new(Mutex::new(HashMap::<String, Arc<AtomicBool>>::new())),
            tenant_running: Arc::new(Mutex::new(HashMap::new())),
            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
            transform_cache: Arc::new(Mutex::new(HashMap::new())),
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
        };
        (state, tasks)
    }

    #[tokio::test]
    async fn get_task_returns_503_when_remote_lookup_errors() {
        let (state, tasks) = remote_base_api_state("http://127.0.0.1:1");
        if let Ok(mut t) = tasks.lock() {
            t.insert(
                "task-remote-error".to_string(),
                TaskState {
                    task_id: "task-remote-error".to_string(),
                    tenant_id: "default".to_string(),
                    operator: "transform_rows_v2".to_string(),
                    status: "done".to_string(),
                    created_at: utc_now_iso(),
                    updated_at: utc_now_iso(),
                    result: None,
                    error: None,
                    idempotency_key: "".to_string(),
                    attempts: 0,
                },
            );
        }
        let app = build_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/tasks/task-remote-error")
                    .body(Body::empty())
                    .expect("get request"),
            )
            .await
            .expect("get response");
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("get body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("get json");
        assert_eq!(
            v.get("error").and_then(|x| x.as_str()),
            Some("task_store_unavailable")
        );
    }

    #[tokio::test]
    async fn cancel_task_returns_503_without_mutating_local_state_when_remote_cancel_errors() {
        let (state, tasks) = remote_base_api_state("http://127.0.0.1:1");
        let flag = Arc::new(AtomicBool::new(false));
        if let Ok(mut t) = tasks.lock() {
            t.insert(
                "task-cancel-remote-error".to_string(),
                TaskState {
                    task_id: "task-cancel-remote-error".to_string(),
                    tenant_id: "default".to_string(),
                    operator: "transform_rows_v2".to_string(),
                    status: "running".to_string(),
                    created_at: utc_now_iso(),
                    updated_at: utc_now_iso(),
                    result: None,
                    error: None,
                    idempotency_key: "".to_string(),
                    attempts: 0,
                },
            );
        }
        if let Ok(mut flags) = state.cancel_flags.lock() {
            flags.insert("task-cancel-remote-error".to_string(), Arc::clone(&flag));
        }
        let app = build_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/tasks/task-cancel-remote-error/cancel")
                    .body(Body::empty())
                    .expect("cancel request"),
            )
            .await
            .expect("cancel response");
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("cancel body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("cancel json");
        assert_eq!(
            v.get("error").and_then(|x| x.as_str()),
            Some("task_store_unavailable")
        );
        let status = tasks
            .lock()
            .expect("tasks lock")
            .get("task-cancel-remote-error")
            .map(|task| task.status.clone());
        assert_eq!(status.as_deref(), Some("running"));
        assert!(!flag.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn cancel_task_sets_flag_when_remote_cancel_succeeds() {
        let std_listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind std listener");
        let addr = std_listener.local_addr().expect("listener addr");
        std_listener
            .set_nonblocking(true)
            .expect("set nonblocking");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build runtime");
            rt.block_on(async move {
                let listener =
                    tokio::net::TcpListener::from_std(std_listener).expect("listener from std");
                let app = axum::Router::new().route(
                    "/api/v1/runtime/tasks/task-remote-ok/cancel",
                    axum::routing::post(|| async {
                        Json(json!({"ok": true, "task_id": "task-remote-ok", "cancelled": true, "status": "cancelled"}))
                    }),
                );
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .expect("serve test api");
            });
        });

        let (state, tasks) = remote_base_api_state(&format!("http://{}", addr));
        let flag = Arc::new(AtomicBool::new(false));
        if let Ok(mut t) = tasks.lock() {
            t.insert(
                "task-remote-ok".to_string(),
                TaskState {
                    task_id: "task-remote-ok".to_string(),
                    tenant_id: "default".to_string(),
                    operator: "transform_rows_v2".to_string(),
                    status: "running".to_string(),
                    created_at: utc_now_iso(),
                    updated_at: utc_now_iso(),
                    result: None,
                    error: None,
                    idempotency_key: "".to_string(),
                    attempts: 0,
                },
            );
        }
        if let Ok(mut flags) = state.cancel_flags.lock() {
            flags.insert("task-remote-ok".to_string(), Arc::clone(&flag));
        }
        let app = build_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/tasks/task-remote-ok/cancel")
                    .body(Body::empty())
                    .expect("cancel request"),
            )
            .await
            .expect("cancel response");
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(flag.load(Ordering::Relaxed));
        let _ = shutdown_tx.send(());
        server.join().expect("join server thread");
    }
}
