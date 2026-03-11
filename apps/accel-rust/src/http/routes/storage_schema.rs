use accel_rust::app_state::AppState;
use axum::{Router, routing::post};

pub(super) fn storage_schema_routes() -> Router<AppState> {
    Router::new()
        .route("/operators/load_rows_v1", post(crate::http::load_rows_v1_operator))
        .route("/operators/load_rows_v2", post(crate::http::load_rows_v2_operator))
        .route("/operators/load_rows_v3", post(crate::http::load_rows_v3_operator))
        .route("/operators/save_rows_v1", post(crate::http::save_rows_v1_operator))
        .route(
            "/operators/schema_registry_v1/register",
            post(crate::http::schema_registry_register_v1_operator),
        )
        .route(
            "/operators/schema_registry_v1/get",
            post(crate::http::schema_registry_get_v1_operator),
        )
        .route(
            "/operators/schema_registry_v1/infer",
            post(crate::http::schema_registry_infer_v1_operator),
        )
        .route(
            "/operators/schema_registry_v2/register",
            post(crate::http::schema_registry_register_v2_operator),
        )
        .route(
            "/operators/schema_registry_v2/get",
            post(crate::http::schema_registry_get_v2_operator),
        )
        .route(
            "/operators/schema_registry_v2/infer",
            post(crate::http::schema_registry_infer_v2_operator),
        )
        .route(
            "/operators/schema_registry_v2/check_compat",
            post(crate::http::schema_registry_check_compat_v2_operator),
        )
        .route(
            "/operators/schema_registry_v2/suggest_migration",
            post(crate::http::schema_registry_suggest_migration_v2_operator),
        )
        .route(
            "/operators/feature_store_v1/upsert",
            post(crate::http::feature_store_upsert_v1_operator),
        )
        .route(
            "/operators/feature_store_v1/get",
            post(crate::http::feature_store_get_v1_operator),
        )
        .route(
            "/operators/parquet_io_v2",
            post(crate::http::parquet_io_v2_operator),
        )
        .route(
            "/operators/stream_state_v1/save",
            post(crate::http::stream_state_save_v1_operator),
        )
        .route(
            "/operators/stream_state_v1/load",
            post(crate::http::stream_state_load_v1_operator),
        )
        .route(
            "/operators/stream_state_v2",
            post(crate::http::stream_state_v2_operator),
        )
}
