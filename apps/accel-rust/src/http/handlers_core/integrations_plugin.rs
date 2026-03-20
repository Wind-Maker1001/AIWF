use crate::{
    api_types::{PluginExecReq, PluginHealthReq, PluginOperatorV1Req, PluginRegistryV1Req},
    misc_ops::safe_pkg_token,
    plugin_runtime::{
        run_plugin_exec_v1, run_plugin_healthcheck, run_plugin_operator_v1, run_plugin_registry_v1,
    },
    transform_support::enforce_tenant_payload_quota,
};
use accel_rust::app_state::AppState;
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};

use super::support;

pub(crate) async fn plugin_exec_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<PluginExecReq>,
) -> impl IntoResponse {
    let bytes = serde_json::to_vec(&req.input).map(|value| value.len()).unwrap_or(0);
    if let Err(error) = enforce_tenant_payload_quota(Some(&state), req.tenant_id.as_deref(), 1, bytes) {
        return support::error_json(StatusCode::TOO_MANY_REQUESTS, "plugin_exec_v1", error);
    }
    match run_plugin_exec_v1(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::error_json(StatusCode::BAD_REQUEST, "plugin_exec_v1", error),
    }
}

pub(crate) async fn plugin_health_v1_operator(
    Json(req): Json<PluginHealthReq>,
) -> impl IntoResponse {
    let plugin = match safe_pkg_token(&req.plugin) {
        Ok(value) => value,
        Err(error) => return support::plugin_health_error(req.plugin, error),
    };
    match run_plugin_healthcheck(&plugin, req.tenant_id.as_deref()) {
        Ok(details) => support::plugin_health_ok(plugin, details),
        Err(error) => support::plugin_health_error(plugin, error),
    }
}

pub(crate) async fn plugin_registry_v1_operator(
    Json(req): Json<PluginRegistryV1Req>,
) -> impl IntoResponse {
    match run_plugin_registry_v1(req) {
        Ok(value) => support::ok_json(value),
        Err(error) => support::error_json(StatusCode::BAD_REQUEST, "plugin_registry_v1", error),
    }
}

pub(crate) async fn plugin_operator_v1_operator(
    Json(req): Json<PluginOperatorV1Req>,
) -> impl IntoResponse {
    match run_plugin_operator_v1(req) {
        Ok(value) => support::ok_json(value),
        Err(error) => support::error_json(StatusCode::BAD_REQUEST, "plugin_operator_v1", error),
    }
}
