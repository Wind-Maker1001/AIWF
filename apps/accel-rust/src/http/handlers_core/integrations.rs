use crate::{
    api_types::{
        ErrResp, LoadRowsReq, LoadRowsV2Req, LoadRowsV3Req, PluginExecReq, PluginHealthReq,
        PluginHealthResp, PluginOperatorV1Req, PluginRegistryV1Req,
    },
    load_ops::{run_load_rows_v1, run_load_rows_v2, run_load_rows_v3},
    misc_ops::safe_pkg_token,
    plugin_runtime::{
        run_plugin_exec_v1, run_plugin_healthcheck, run_plugin_operator_v1, run_plugin_registry_v1,
    },
    transform_support::enforce_tenant_payload_quota,
};
use accel_rust::{app_state::AppState, metrics::observe_operator_latency_v2};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde_json::json;
use std::time::Instant;

pub(crate) async fn plugin_exec_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<PluginExecReq>,
) -> impl IntoResponse {
    let bytes = serde_json::to_vec(&req.input).map(|v| v.len()).unwrap_or(0);
    if let Err(e) = enforce_tenant_payload_quota(Some(&state), req.tenant_id.as_deref(), 1, bytes) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrResp {
                ok: false,
                operator: "plugin_exec_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response();
    }
    match run_plugin_exec_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "plugin_exec_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn plugin_health_v1_operator(
    Json(req): Json<PluginHealthReq>,
) -> impl IntoResponse {
    let plugin = match safe_pkg_token(&req.plugin) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrResp {
                    ok: false,
                    operator: "plugin_health_v1".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response();
        }
    };
    match run_plugin_healthcheck(&plugin, req.tenant_id.as_deref()) {
        Ok(details) => (
            StatusCode::OK,
            Json(PluginHealthResp {
                ok: true,
                operator: "plugin_health_v1".to_string(),
                status: "done".to_string(),
                plugin,
                details,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(PluginHealthResp {
                ok: false,
                operator: "plugin_health_v1".to_string(),
                status: "failed".to_string(),
                plugin,
                details: json!({ "error": e }),
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn plugin_registry_v1_operator(
    Json(req): Json<PluginRegistryV1Req>,
) -> impl IntoResponse {
    match run_plugin_registry_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "plugin_registry_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn plugin_operator_v1_operator(
    Json(req): Json<PluginOperatorV1Req>,
) -> impl IntoResponse {
    match run_plugin_operator_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "plugin_operator_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn load_rows_v1_operator(Json(req): Json<LoadRowsReq>) -> impl IntoResponse {
    match run_load_rows_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "load_rows_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn load_rows_v2_operator(Json(req): Json<LoadRowsV2Req>) -> impl IntoResponse {
    match run_load_rows_v2(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "load_rows_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn load_rows_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<LoadRowsV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.load_rows_v3_calls += 1;
    }
    match run_load_rows_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "load_rows_v3",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "load_rows_v3".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}
