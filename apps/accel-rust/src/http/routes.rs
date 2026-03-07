use accel_rust::app_state::AppState;
use axum::{
    Router,
    routing::{get, post},
};

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(crate::health))
        .route("/metrics", get(crate::metrics))
        .route("/admin/reload_runtime_config", post(crate::reload_runtime_config))
        .route("/operators/cleaning", post(crate::cleaning_operator))
        .route("/operators/compute_metrics", post(crate::compute_metrics_operator))
        .route(
            "/operators/transform_rows_v2",
            post(crate::transform_rows_v2_operator),
        )
        .route(
            "/operators/transform_rows_v3",
            post(crate::transform_rows_v3_operator),
        )
        .route(
            "/operators/transform_rows_v2/cache_stats",
            get(crate::transform_rows_v2_cache_stats_operator),
        )
        .route(
            "/operators/transform_rows_v2/cache_clear",
            post(crate::transform_rows_v2_cache_clear_operator),
        )
        .route(
            "/operators/transform_rows_v2/stream",
            post(crate::transform_rows_v2_stream_operator),
        )
        .route(
            "/operators/transform_rows_v2/submit",
            post(crate::transform_rows_v2_submit_operator),
        )
        .route(
            "/operators/text_preprocess_v2",
            post(crate::text_preprocess_v2_operator),
        )
        .route("/operators/join_rows_v1", post(crate::join_rows_v1_operator))
        .route("/operators/join_rows_v2", post(crate::join_rows_v2_operator))
        .route("/operators/join_rows_v3", post(crate::join_rows_v3_operator))
        .route(
            "/operators/normalize_schema_v1",
            post(crate::normalize_schema_v1_operator),
        )
        .route(
            "/operators/entity_extract_v1",
            post(crate::entity_extract_v1_operator),
        )
        .route(
            "/operators/aggregate_rows_v1",
            post(crate::aggregate_rows_v1_operator),
        )
        .route(
            "/operators/aggregate_rows_v2",
            post(crate::aggregate_rows_v2_operator),
        )
        .route(
            "/operators/aggregate_rows_v3",
            post(crate::aggregate_rows_v3_operator),
        )
        .route(
            "/operators/quality_check_v1",
            post(crate::quality_check_v1_operator),
        )
        .route(
            "/operators/quality_check_v2",
            post(crate::quality_check_v2_operator),
        )
        .route(
            "/operators/quality_check_v3",
            post(crate::quality_check_v3_operator),
        )
        .route(
            "/operators/aggregate_pushdown_v1",
            post(crate::aggregate_pushdown_v1_operator),
        )
        .route("/operators/plugin_exec_v1", post(crate::plugin_exec_v1_operator))
        .route(
            "/operators/plugin_health_v1",
            post(crate::plugin_health_v1_operator),
        )
        .route(
            "/operators/plugin_registry_v1",
            post(crate::plugin_registry_v1_operator),
        )
        .route(
            "/operators/plugin_operator_v1",
            post(crate::plugin_operator_v1_operator),
        )
        .route(
            "/operators/rules_compile_v1",
            post(crate::rules_compile_v1_operator),
        )
        .route(
            "/operators/rules_package_v1/publish",
            post(crate::rules_package_publish_v1_operator),
        )
        .route(
            "/operators/rules_package_v1/get",
            post(crate::rules_package_get_v1_operator),
        )
        .route("/operators/load_rows_v1", post(crate::load_rows_v1_operator))
        .route("/operators/load_rows_v2", post(crate::load_rows_v2_operator))
        .route("/operators/load_rows_v3", post(crate::load_rows_v3_operator))
        .route("/operators/save_rows_v1", post(crate::save_rows_v1_operator))
        .route(
            "/operators/schema_registry_v1/register",
            post(crate::schema_registry_register_v1_operator),
        )
        .route(
            "/operators/schema_registry_v1/get",
            post(crate::schema_registry_get_v1_operator),
        )
        .route(
            "/operators/schema_registry_v1/infer",
            post(crate::schema_registry_infer_v1_operator),
        )
        .route(
            "/operators/schema_registry_v2/register",
            post(crate::schema_registry_register_v2_operator),
        )
        .route(
            "/operators/schema_registry_v2/get",
            post(crate::schema_registry_get_v2_operator),
        )
        .route(
            "/operators/schema_registry_v2/infer",
            post(crate::schema_registry_infer_v2_operator),
        )
        .route(
            "/operators/schema_registry_v2/check_compat",
            post(crate::schema_registry_check_compat_v2_operator),
        )
        .route(
            "/operators/schema_registry_v2/suggest_migration",
            post(crate::schema_registry_suggest_migration_v2_operator),
        )
        .route("/operators/udf_wasm_v1/apply", post(crate::udf_wasm_v1_operator))
        .route("/operators/time_series_v1", post(crate::time_series_v1_operator))
        .route("/operators/stats_v1", post(crate::stats_v1_operator))
        .route(
            "/operators/entity_linking_v1",
            post(crate::entity_linking_v1_operator),
        )
        .route(
            "/operators/table_reconstruct_v1",
            post(crate::table_reconstruct_v1_operator),
        )
        .route(
            "/operators/feature_store_v1/upsert",
            post(crate::feature_store_upsert_v1_operator),
        )
        .route(
            "/operators/feature_store_v1/get",
            post(crate::feature_store_get_v1_operator),
        )
        .route("/operators/lineage_v2", post(crate::lineage_v2_operator))
        .route(
            "/operators/rule_simulator_v1",
            post(crate::rule_simulator_v1_operator),
        )
        .route(
            "/operators/constraint_solver_v1",
            post(crate::constraint_solver_v1_operator),
        )
        .route(
            "/operators/chart_data_prep_v1",
            post(crate::chart_data_prep_v1_operator),
        )
        .route("/operators/diff_audit_v1", post(crate::diff_audit_v1_operator))
        .route(
            "/operators/vector_index_v1/build",
            post(crate::vector_index_build_v1_operator),
        )
        .route(
            "/operators/vector_index_v1/search",
            post(crate::vector_index_search_v1_operator),
        )
        .route(
            "/operators/evidence_rank_v1",
            post(crate::evidence_rank_v1_operator),
        )
        .route(
            "/operators/fact_crosscheck_v1",
            post(crate::fact_crosscheck_v1_operator),
        )
        .route(
            "/operators/timeseries_forecast_v1",
            post(crate::timeseries_forecast_v1_operator),
        )
        .route(
            "/operators/finance_ratio_v1",
            post(crate::finance_ratio_v1_operator),
        )
        .route(
            "/operators/anomaly_explain_v1",
            post(crate::anomaly_explain_v1_operator),
        )
        .route(
            "/operators/template_bind_v1",
            post(crate::template_bind_v1_operator),
        )
        .route(
            "/operators/provenance_sign_v1",
            post(crate::provenance_sign_v1_operator),
        )
        .route(
            "/operators/stream_state_v1/save",
            post(crate::stream_state_save_v1_operator),
        )
        .route(
            "/operators/stream_state_v1/load",
            post(crate::stream_state_load_v1_operator),
        )
        .route("/operators/query_lang_v1", post(crate::query_lang_v1_operator))
        .route(
            "/operators/columnar_eval_v1",
            post(crate::columnar_eval_v1_operator),
        )
        .route(
            "/operators/stream_window_v1",
            post(crate::stream_window_v1_operator),
        )
        .route(
            "/operators/stream_window_v2",
            post(crate::stream_window_v2_operator),
        )
        .route("/operators/sketch_v1", post(crate::sketch_v1_operator))
        .route(
            "/operators/runtime_stats_v1",
            post(crate::runtime_stats_v1_operator),
        )
        .route("/operators/capabilities_v1", post(crate::capabilities_v1_operator))
        .route(
            "/operators/io_contract_v1/validate",
            post(crate::io_contract_v1_operator),
        )
        .route(
            "/operators/failure_policy_v1",
            post(crate::failure_policy_v1_operator),
        )
        .route(
            "/operators/incremental_plan_v1",
            post(crate::incremental_plan_v1_operator),
        )
        .route(
            "/operators/tenant_isolation_v1",
            post(crate::tenant_isolation_v1_operator),
        )
        .route(
            "/operators/operator_policy_v1",
            post(crate::operator_policy_v1_operator),
        )
        .route(
            "/operators/optimizer_adaptive_v2",
            post(crate::optimizer_adaptive_v2_operator),
        )
        .route(
            "/operators/vector_index_v2/build",
            post(crate::vector_index_build_v2_operator),
        )
        .route(
            "/operators/vector_index_v2/search",
            post(crate::vector_index_search_v2_operator),
        )
        .route(
            "/operators/vector_index_v2/eval",
            post(crate::vector_index_eval_v2_operator),
        )
        .route(
            "/operators/stream_reliability_v1",
            post(crate::stream_reliability_v1_operator),
        )
        .route(
            "/operators/lineage_provenance_v1",
            post(crate::lineage_provenance_v1_operator),
        )
        .route(
            "/operators/contract_regression_v1",
            post(crate::contract_regression_v1_operator),
        )
        .route(
            "/operators/perf_baseline_v1",
            post(crate::perf_baseline_v1_operator),
        )
        .route("/operators/window_rows_v1", post(crate::window_rows_v1_operator))
        .route("/operators/optimizer_v1", post(crate::optimizer_v1_operator))
        .route("/operators/join_rows_v4", post(crate::join_rows_v4_operator))
        .route(
            "/operators/aggregate_rows_v4",
            post(crate::aggregate_rows_v4_operator),
        )
        .route(
            "/operators/quality_check_v4",
            post(crate::quality_check_v4_operator),
        )
        .route("/operators/lineage_v3", post(crate::lineage_v3_operator))
        .route("/operators/parquet_io_v2", post(crate::parquet_io_v2_operator))
        .route("/operators/stream_state_v2", post(crate::stream_state_v2_operator))
        .route("/operators/udf_wasm_v2/apply", post(crate::udf_wasm_v2_operator))
        .route("/operators/explain_plan_v1", post(crate::explain_plan_v1_operator))
        .route("/operators/explain_plan_v2", post(crate::explain_plan_v2_operator))
        .route("/metrics_v2", get(crate::metrics_v2))
        .route("/metrics_v2/prom", get(crate::metrics_v2_prom))
        .route("/workflow/run", post(crate::workflow_run_operator))
        .route("/tasks/{task_id}", get(crate::get_task_operator))
        .route("/tasks/{task_id}/cancel", post(crate::cancel_task_operator))
        .with_state(state)
}
