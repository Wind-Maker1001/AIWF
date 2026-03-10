use super::*;

pub(crate) async fn save_rows_v1_operator(Json(req): Json<SaveRowsReq>) -> impl IntoResponse {
    match run_save_rows_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "save_rows_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn transform_rows_v2_stream_operator(
    State(state): State<AppState>,
    Json(req): Json<TransformRowsStreamReq>,
) -> impl IntoResponse {
    if let Some(rows) = req.rows.as_ref() {
        let bytes = serde_json::to_vec(rows).map(|v| v.len()).unwrap_or(0);
        if let Err(e) =
            enforce_tenant_payload_quota(Some(&state), req.tenant_id.as_deref(), rows.len(), bytes)
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(ErrResp {
                    ok: false,
                    operator: "transform_rows_v2_stream".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response();
        }
    }
    match run_transform_rows_v2_stream(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "transform_rows_v2_stream".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn workflow_run_operator(
    State(state): State<AppState>,
    Json(req): Json<WorkflowRunReq>,
) -> impl IntoResponse {
    let step_limit = tenant_max_workflow_steps_for(req.tenant_id.as_deref());
    if req.steps.len() > step_limit {
        if let Ok(mut m) = state.metrics.lock() {
            m.quota_reject_total += 1;
        }
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrResp {
                ok: false,
                operator: "workflow_run".to_string(),
                status: "failed".to_string(),
                error: format!(
                    "workflow step quota exceeded: {} > {}",
                    req.steps.len(),
                    step_limit
                ),
            }),
        )
            .into_response();
    }
    match crate::operators::workflow::run_workflow_with_state(&state, req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "workflow_run".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}
