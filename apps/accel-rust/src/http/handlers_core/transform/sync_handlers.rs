use super::*;

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
