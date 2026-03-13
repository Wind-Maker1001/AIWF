use accel_rust::app_state::AppState;
use axum::{Router, routing::post};

pub(super) fn integration_routes() -> Router<AppState> {
    super::operator_guarded(Router::new()
        .route(
            "/operators/plugin_exec_v1",
            post(crate::http::plugin_exec_v1_operator),
        )
        .route(
            "/operators/plugin_health_v1",
            post(crate::http::plugin_health_v1_operator),
        )
        .route(
            "/operators/plugin_registry_v1",
            post(crate::http::plugin_registry_v1_operator),
        )
        .route(
            "/operators/plugin_operator_v1",
            post(crate::http::plugin_operator_v1_operator),
        )
        .route(
            "/operators/rules_compile_v1",
            post(crate::http::rules_compile_v1_operator),
        )
        .route(
            "/operators/rules_package_v1/publish",
            post(crate::http::rules_package_publish_v1_operator),
        )
        .route(
            "/operators/rules_package_v1/get",
            post(crate::http::rules_package_get_v1_operator),
        )
        .route(
            "/operators/udf_wasm_v1/apply",
            post(crate::http::udf_wasm_v1_operator),
        )
        .route(
            "/operators/udf_wasm_v2/apply",
            post(crate::http::udf_wasm_v2_operator),
        ))
}
