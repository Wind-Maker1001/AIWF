use super::*;

pub(crate) async fn stream_state_save_v1_operator(
    Json(req): Json<StreamStateSaveReq>,
) -> impl IntoResponse {
    match run_stream_state_save_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_state_v1_save".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn stream_state_load_v1_operator(
    Json(req): Json<StreamStateLoadReq>,
) -> impl IntoResponse {
    match run_stream_state_load_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_state_v1_load".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn query_lang_v1_operator(Json(req): Json<QueryLangReq>) -> impl IntoResponse {
    match run_query_lang_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "query_lang_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}
