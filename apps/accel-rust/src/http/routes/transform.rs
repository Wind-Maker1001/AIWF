use accel_rust::app_state::AppState;
use axum::{
    Router,
    routing::{get, post},
};

pub(super) fn transform_routes() -> Router<AppState> {
    super::operator_guarded(Router::new()
        .route("/operators/cleaning", post(crate::http::cleaning_operator))
        .route(
            "/operators/compute_metrics",
            post(crate::http::compute_metrics_operator),
        )
        .route(
            "/operators/transform_rows_v2",
            post(crate::http::transform_rows_v2_operator),
        )
        .route(
            "/operators/transform_rows_v3",
            post(crate::http::transform_rows_v3_operator),
        )
        .route(
            "/operators/transform_rows_v2/cache_stats",
            get(crate::http::transform_rows_v2_cache_stats_operator),
        )
        .route(
            "/operators/transform_rows_v2/cache_clear",
            post(crate::http::transform_rows_v2_cache_clear_operator),
        )
        .route(
            "/operators/transform_rows_v2/stream",
            post(crate::http::transform_rows_v2_stream_operator),
        )
        .route(
            "/operators/transform_rows_v2/submit",
            post(crate::http::transform_rows_v2_submit_operator),
        )
        .route(
            "/operators/text_preprocess_v2",
            post(crate::http::text_preprocess_v2_operator),
        )
        .route(
            "/operators/postprocess_rows_v1",
            post(crate::http::postprocess_rows_v1_operator),
        )
        .route(
            "/operators/normalize_schema_v1",
            post(crate::http::normalize_schema_v1_operator),
        )
        .route(
            "/operators/entity_extract_v1",
            post(crate::http::entity_extract_v1_operator),
        )
        .route(
            "/operators/aggregate_pushdown_v1",
            post(crate::http::aggregate_pushdown_v1_operator),
        ))
}
