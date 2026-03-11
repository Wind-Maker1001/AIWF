use accel_rust::app_state::AppState;
use axum::{Router, routing::post};

pub(super) fn table_routes() -> Router<AppState> {
    Router::new()
        .route("/operators/join_rows_v1", post(crate::http::join_rows_v1_operator))
        .route("/operators/join_rows_v2", post(crate::http::join_rows_v2_operator))
        .route("/operators/join_rows_v3", post(crate::http::join_rows_v3_operator))
        .route("/operators/join_rows_v4", post(crate::http::join_rows_v4_operator))
        .route(
            "/operators/aggregate_rows_v1",
            post(crate::http::aggregate_rows_v1_operator),
        )
        .route(
            "/operators/aggregate_rows_v2",
            post(crate::http::aggregate_rows_v2_operator),
        )
        .route(
            "/operators/aggregate_rows_v3",
            post(crate::http::aggregate_rows_v3_operator),
        )
        .route(
            "/operators/aggregate_rows_v4",
            post(crate::http::aggregate_rows_v4_operator),
        )
        .route(
            "/operators/quality_check_v1",
            post(crate::http::quality_check_v1_operator),
        )
        .route(
            "/operators/quality_check_v2",
            post(crate::http::quality_check_v2_operator),
        )
        .route(
            "/operators/quality_check_v3",
            post(crate::http::quality_check_v3_operator),
        )
        .route(
            "/operators/quality_check_v4",
            post(crate::http::quality_check_v4_operator),
        )
        .route(
            "/operators/window_rows_v1",
            post(crate::http::window_rows_v1_operator),
        )
}
