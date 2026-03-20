use crate::api_types::{ErrResp, PluginHealthResp};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::{Value, json};

pub(super) fn ok_json<T: Serialize>(payload: T) -> Response {
    (StatusCode::OK, Json(payload)).into_response()
}

pub(super) fn error_json(status: StatusCode, operator: &str, error: String) -> Response {
    (
        status,
        Json(ErrResp {
            ok: false,
            operator: operator.to_string(),
            status: "failed".to_string(),
            error,
        }),
    )
        .into_response()
}

pub(super) fn plugin_health_ok(plugin: String, details: Value) -> Response {
    (
        StatusCode::OK,
        Json(PluginHealthResp {
            ok: true,
            operator: "plugin_health_v1".to_string(),
            status: "done".to_string(),
            plugin,
            details,
        }),
    )
        .into_response()
}

pub(super) fn plugin_health_error(plugin: String, error: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(PluginHealthResp {
            ok: false,
            operator: "plugin_health_v1".to_string(),
            status: "failed".to_string(),
            plugin,
            details: json!({ "error": error }),
        }),
    )
        .into_response()
}
