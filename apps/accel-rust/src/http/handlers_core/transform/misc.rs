use super::*;

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

pub(crate) async fn postprocess_rows_v1_operator(
    Json(req): Json<PostprocessRowsV1Req>,
) -> impl IntoResponse {
    match run_postprocess_rows_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "postprocess_rows_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
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
