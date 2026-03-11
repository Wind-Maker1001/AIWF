use accel_rust::app_state::AppState;
use axum::{
    Router,
    routing::{get, post},
};

pub(super) fn system_routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(crate::http::health))
        .route("/capabilities", get(crate::http::capabilities_operator))
        .route("/metrics", get(crate::http::metrics))
        .route("/metrics_v2", get(crate::http::metrics_v2))
        .route("/metrics_v2/prom", get(crate::http::metrics_v2_prom))
        .route(
            "/admin/reload_runtime_config",
            post(crate::http::reload_runtime_config),
        )
}
