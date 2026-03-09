use crate::{
    api_types::{
        CleaningReq, ComputeReq, ErrResp, RulesCompileReq, RulesCompileResp, TextPreprocessReq,
    },
    cleaning_runtime::{run_cleaning_operator, run_compute_metrics},
    current_task_cfg,
    misc_ops::{compile_rules_dsl, run_text_preprocess_v2},
    operators::transform::{
        TransformRowsReq, TransformRowsV3Req, observe_transform_success,
        run_transform_rows_v2_with_cache, run_transform_rows_v3,
    },
    transform_support::{
        cleanup_task_flag, enforce_tenant_payload_quota, release_tenant_slot,
        request_prefers_columnar, transform_cache_enabled, transform_cache_max_entries,
        transform_cache_ttl_sec, try_acquire_tenant_slot, unique_trace, unix_now_sec, utc_now_iso,
        verify_request_signature,
    },
};
use accel_rust::{
    app_state::{AppState, TaskState, TransformRowsResp},
    metrics::observe_operator_latency_v2,
    task_store::{persist_tasks_to_store, prune_tasks, task_store_upsert_task},
};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde_json::json;
use std::{
    env,
    sync::{Arc, atomic::AtomicBool},
    time::Instant,
};

fn normalized_key(value: Option<String>) -> Option<String> {
    value.and_then(|item| {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn effective_submit_idempotency_key(req: &TransformRowsReq, tenant_id: &str) -> String {
    normalized_key(req.idempotency_key.clone())
        .or_else(|| normalized_key(req.run_id.clone()))
        .unwrap_or_else(|| unique_trace(&format!("tenant:{tenant_id}:submit")))
}

fn next_submit_task_id(req: &TransformRowsReq, id_full: &str) -> String {
    let run_id = normalized_key(req.run_id.clone()).unwrap_or_default();
    unique_trace(&format!("task:{run_id}:{id_full}"))
}

pub(crate) async fn cleaning_operator(Json(req): Json<CleaningReq>) -> impl IntoResponse {
    match run_cleaning_operator(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrResp {
                ok: false,
                operator: "cleaning".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn compute_metrics_operator(Json(req): Json<ComputeReq>) -> impl IntoResponse {
    match run_compute_metrics(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrResp {
                ok: false,
                operator: "compute_metrics".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn transform_rows_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<TransformRowsReq>,
) -> impl IntoResponse {
    let columnar_preferred = request_prefers_columnar(&req);
    if let Some(rows) = req.rows.as_ref() {
        let bytes = serde_json::to_vec(rows).map(|v| v.len()).unwrap_or(0);
        if let Err(e) =
            enforce_tenant_payload_quota(Some(&state), req.tenant_id.as_deref(), rows.len(), bytes)
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(ErrResp {
                    ok: false,
                    operator: "transform_rows_v2".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response();
        }
    }
    if let Ok(mut m) = state.metrics.lock() {
        m.transform_rows_v2_calls += 1;
        if columnar_preferred {
            m.transform_rows_v2_columnar_calls += 1;
        }
    }
    match run_transform_rows_v2_with_cache(
        req,
        None,
        Some(&state.transform_cache),
        Some(&state.metrics),
    ) {
        Ok(resp) => {
            observe_transform_success(&state.metrics, &resp);
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => {
            if let Ok(mut m) = state.metrics.lock() {
                m.transform_rows_v2_errors += 1;
            }
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrResp {
                    ok: false,
                    operator: "transform_rows_v2".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response()
        }
    }
}

pub(crate) async fn transform_rows_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<TransformRowsV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.transform_rows_v3_calls += 1;
    }
    match run_transform_rows_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "transform_rows_v3",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "transform_rows_v3".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn transform_rows_v2_cache_stats_operator(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let now = unix_now_sec();
    let mut entries = 0usize;
    let mut expired = 0usize;
    if let Ok(guard) = state.transform_cache.lock() {
        entries = guard.len();
        expired = guard.values().filter(|v| v.expires_at_epoch <= now).count();
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "operator": "transform_rows_v2_cache_stats",
            "cache_enabled": transform_cache_enabled(),
            "entries": entries,
            "expired_entries": expired,
            "ttl_sec": transform_cache_ttl_sec(),
            "max_entries": transform_cache_max_entries()
        })),
    )
        .into_response()
}

pub(crate) async fn transform_rows_v2_cache_clear_operator(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut cleared = 0usize;
    if let Ok(mut guard) = state.transform_cache.lock() {
        cleared = guard.len();
        guard.clear();
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "operator": "transform_rows_v2_cache_clear",
            "cleared": cleared
        })),
    )
        .into_response()
}

pub(crate) async fn transform_rows_v2_submit_operator(
    State(state): State<AppState>,
    Json(req): Json<TransformRowsReq>,
) -> impl IntoResponse {
    let columnar_preferred = request_prefers_columnar(&req);
    if let Some(rows) = req.rows.as_ref() {
        let bytes = serde_json::to_vec(rows).map(|v| v.len()).unwrap_or(0);
        if let Err(e) =
            enforce_tenant_payload_quota(Some(&state), req.tenant_id.as_deref(), rows.len(), bytes)
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(ErrResp {
                    ok: false,
                    operator: "transform_rows_v2".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response();
        }
    }
    if let Err(e) = verify_request_signature(&req) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrResp {
                ok: false,
                operator: "transform_rows_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response();
    }
    let cfg = current_task_cfg(&state);
    let tenant_id = req
        .tenant_id
        .clone()
        .unwrap_or_else(|| env::var("AIWF_TENANT_ID").unwrap_or_else(|_| "default".to_string()));
    if let Err(e) = try_acquire_tenant_slot(&state, &tenant_id) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrResp {
                ok: false,
                operator: "transform_rows_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response();
    }
    let id_key = effective_submit_idempotency_key(&req, &tenant_id);
    let id_full = format!("{tenant_id}:{id_key}");
    if let Ok(idx) = state.idempotency_index.lock()
        && let Some(existing_task_id) = idx.get(&id_full)
        && let Ok(t) = state.tasks.lock()
        && let Some(ts) = t.get(existing_task_id)
    {
        let st = ts.status.to_lowercase();
        if st == "queued" || st == "running" || st == "done" {
            release_tenant_slot(&state, &tenant_id);
            return (
                StatusCode::OK,
                Json(json!({"ok": true, "task_id": existing_task_id, "status": st, "idempotent_hit": true})),
            )
                .into_response();
        }
    }
    let now = utc_now_iso();
    let task_id = next_submit_task_id(&req, &id_full);
    let task = TaskState {
        task_id: task_id.clone(),
        tenant_id,
        operator: "transform_rows_v2".to_string(),
        status: "queued".to_string(),
        created_at: now.clone(),
        updated_at: now,
        result: None,
        error: None,
        idempotency_key: id_key,
        attempts: 0,
    };
    task_store_upsert_task(&task, &cfg);
    let cancel_flag = Arc::new(AtomicBool::new(false));
    if let Ok(mut flags) = state.cancel_flags.lock() {
        flags.insert(task_id.clone(), Arc::clone(&cancel_flag));
    }
    if let Ok(mut t) = state.tasks.lock() {
        t.insert(task_id.clone(), task.clone());
        let _ = prune_tasks(&mut t, &cfg);
        persist_tasks_to_store(&t, cfg.store_path.as_ref());
    }
    if let Ok(mut idx) = state.idempotency_index.lock() {
        idx.insert(id_full.clone(), task_id.clone());
    }

    if let Ok(mut m) = state.metrics.lock() {
        m.transform_rows_v2_calls += 1;
        if columnar_preferred {
            m.transform_rows_v2_columnar_calls += 1;
        }
    }

    let task_id_for_worker = task_id.clone();
    let tasks = Arc::clone(&state.tasks);
    let metrics = Arc::clone(&state.metrics);
    let transform_cache = Arc::clone(&state.transform_cache);
    let task_cfg = cfg.clone();
    let cancel_flags = Arc::clone(&state.cancel_flags);
    let tenant_running = Arc::clone(&state.tenant_running);
    let idempotency_index = Arc::clone(&state.idempotency_index);
    let retry_max = env::var("AIWF_RUST_TASK_RETRY_MAX")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(1);
    let tenant_for_worker = task.tenant_id.clone();
    let id_full_for_worker = id_full.clone();
    tokio::spawn(async move {
        if let Ok(mut t) = tasks.lock()
            && let Some(cur) = t.get_mut(&task_id_for_worker)
            && cur.status != "cancelled"
        {
            cur.status = "running".to_string();
            cur.updated_at = utc_now_iso();
            let cur_snapshot = cur.clone();
            let _ = prune_tasks(&mut t, &task_cfg);
            persist_tasks_to_store(&t, task_cfg.store_path.as_ref());
            task_store_upsert_task(&cur_snapshot, &task_cfg);
        }

        if let Ok(t) = tasks.lock()
            && let Some(cur) = t.get(&task_id_for_worker)
            && cur.status == "cancelled"
        {
            cleanup_task_flag(&task_id_for_worker, &cancel_flags, &metrics);
            if let Ok(mut running) = tenant_running.lock()
                && let Some(v) = running.get_mut(&tenant_for_worker)
                && *v > 0
            {
                *v -= 1;
            }
            return;
        }

        let mut local_attempt = 0u32;
        let req_base = req;
        let mut res: Result<TransformRowsResp, String>;
        loop {
            local_attempt += 1;
            res = tokio::task::spawn_blocking({
                let cancel_flag = Arc::clone(&cancel_flag);
                let req_call = req_base.clone();
                let metrics_for_call = Arc::clone(&metrics);
                let cache_for_call = Arc::clone(&transform_cache);
                move || {
                    run_transform_rows_v2_with_cache(
                        req_call,
                        Some(cancel_flag),
                        Some(&cache_for_call),
                        Some(&metrics_for_call),
                    )
                }
            })
            .await
            .map_err(|e| format!("task join error: {e}"))
            .and_then(|inner| inner);
            if res.is_ok() || local_attempt > retry_max {
                break;
            }
            if let Ok(mut m) = metrics.lock() {
                m.task_retry_total += 1;
            }
        }

        if let Ok(mut t) = tasks.lock()
            && let Some(cur) = t.get_mut(&task_id_for_worker)
        {
            if cur.status == "cancelled" {
                let cur_snapshot = cur.clone();
                persist_tasks_to_store(&t, task_cfg.store_path.as_ref());
                task_store_upsert_task(&cur_snapshot, &task_cfg);
                cleanup_task_flag(&task_id_for_worker, &cancel_flags, &metrics);
                if let Ok(mut running) = tenant_running.lock()
                    && let Some(v) = running.get_mut(&tenant_for_worker)
                    && *v > 0
                {
                    *v -= 1;
                }
                return;
            }
            match res {
                Ok(resp) => {
                    observe_transform_success(&metrics, &resp);
                    cur.status = "done".to_string();
                    cur.result = Some(serde_json::to_value(resp).unwrap_or_else(|_| json!({})));
                    cur.attempts = local_attempt;
                    cur.updated_at = utc_now_iso();
                }
                Err(e) => {
                    cur.status = "failed".to_string();
                    cur.error = Some(e);
                    cur.attempts = local_attempt;
                    cur.updated_at = utc_now_iso();
                    if let Ok(mut m) = metrics.lock() {
                        m.transform_rows_v2_errors += 1;
                    }
                }
            }
            let cur_snapshot = cur.clone();
            let _ = prune_tasks(&mut t, &task_cfg);
            persist_tasks_to_store(&t, task_cfg.store_path.as_ref());
            task_store_upsert_task(&cur_snapshot, &task_cfg);
            cleanup_task_flag(&task_id_for_worker, &cancel_flags, &metrics);
        }
        if let Ok(mut running) = tenant_running.lock()
            && let Some(v) = running.get_mut(&tenant_for_worker)
            && *v > 0
        {
            *v -= 1;
        }
        if let Ok(t) = tasks.lock()
            && let Some(cur) = t.get(&task_id_for_worker)
            && cur.status == "failed"
            && let Ok(mut idx) = idempotency_index.lock()
        {
            idx.remove(&id_full_for_worker);
        }
    });

    (
        StatusCode::OK,
        Json(json!({"ok": true, "task_id": task_id, "status": "queued"})),
    )
        .into_response()
}

pub(crate) async fn text_preprocess_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<TextPreprocessReq>,
) -> impl IntoResponse {
    if let Ok(mut m) = state.metrics.lock() {
        m.text_preprocess_v2_calls += 1;
    }
    match run_text_preprocess_v2(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => {
            if let Ok(mut m) = state.metrics.lock() {
                m.text_preprocess_v2_errors += 1;
            }
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrResp {
                    ok: false,
                    operator: "text_preprocess_v2".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response()
        }
    }
}

pub(crate) async fn rules_compile_v1_operator(
    Json(req): Json<RulesCompileReq>,
) -> impl IntoResponse {
    match compile_rules_dsl(&req.dsl) {
        Ok(rules) => (
            StatusCode::OK,
            Json(RulesCompileResp {
                ok: true,
                operator: "rules_compile_v1".to_string(),
                status: "done".to_string(),
                rules,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "rules_compile_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::{effective_submit_idempotency_key, next_submit_task_id};
    use crate::TransformRowsReq;

    fn blank_req() -> TransformRowsReq {
        TransformRowsReq {
            run_id: Some("   ".to_string()),
            tenant_id: None,
            trace_id: None,
            traceparent: None,
            rows: None,
            rules: None,
            rules_dsl: None,
            quality_gates: None,
            schema_hint: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: Some("".to_string()),
        }
    }

    #[test]
    fn submit_idempotency_fallback_is_unique_when_inputs_are_blank() {
        let first = effective_submit_idempotency_key(&blank_req(), "tenant-a");
        let second = effective_submit_idempotency_key(&blank_req(), "tenant-a");
        assert_ne!(first, second);
    }

    #[test]
    fn submit_task_id_is_unique_for_same_request_shape() {
        let req = blank_req();
        let first = next_submit_task_id(&req, "tenant-a:key");
        let second = next_submit_task_id(&req, "tenant-a:key");
        assert_ne!(first, second);
    }
}
