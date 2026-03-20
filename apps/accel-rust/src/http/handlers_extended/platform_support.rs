use crate::api_types::ErrResp;
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

pub(super) fn ok_json<T: Serialize>(payload: T) -> Response {
    (StatusCode::OK, Json(payload)).into_response()
}

pub(super) fn bad_request(operator: &str, error: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrResp {
            ok: false,
            operator: operator.to_string(),
            status: "failed".to_string(),
            error,
        }),
    )
        .into_response()
}
