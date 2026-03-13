use accel_rust::app_state::AppState;
use axum::{Router, routing::post};

pub(super) fn analysis_routes() -> Router<AppState> {
    super::operator_guarded(Router::new()
        .route("/operators/time_series_v1", post(crate::http::time_series_v1_operator))
        .route("/operators/stats_v1", post(crate::http::stats_v1_operator))
        .route(
            "/operators/entity_linking_v1",
            post(crate::http::entity_linking_v1_operator),
        )
        .route(
            "/operators/table_reconstruct_v1",
            post(crate::http::table_reconstruct_v1_operator),
        )
        .route("/operators/lineage_v2", post(crate::http::lineage_v2_operator))
        .route("/operators/lineage_v3", post(crate::http::lineage_v3_operator))
        .route(
            "/operators/rule_simulator_v1",
            post(crate::http::rule_simulator_v1_operator),
        )
        .route(
            "/operators/constraint_solver_v1",
            post(crate::http::constraint_solver_v1_operator),
        )
        .route(
            "/operators/chart_data_prep_v1",
            post(crate::http::chart_data_prep_v1_operator),
        )
        .route("/operators/diff_audit_v1", post(crate::http::diff_audit_v1_operator))
        .route(
            "/operators/vector_index_v1/build",
            post(crate::http::vector_index_build_v1_operator),
        )
        .route(
            "/operators/vector_index_v1/search",
            post(crate::http::vector_index_search_v1_operator),
        )
        .route(
            "/operators/evidence_rank_v1",
            post(crate::http::evidence_rank_v1_operator),
        )
        .route(
            "/operators/fact_crosscheck_v1",
            post(crate::http::fact_crosscheck_v1_operator),
        )
        .route(
            "/operators/timeseries_forecast_v1",
            post(crate::http::timeseries_forecast_v1_operator),
        )
        .route(
            "/operators/finance_ratio_v1",
            post(crate::http::finance_ratio_v1_operator),
        )
        .route(
            "/operators/anomaly_explain_v1",
            post(crate::http::anomaly_explain_v1_operator),
        )
        .route(
            "/operators/template_bind_v1",
            post(crate::http::template_bind_v1_operator),
        )
        .route(
            "/operators/provenance_sign_v1",
            post(crate::http::provenance_sign_v1_operator),
        )
        .route(
            "/operators/query_lang_v1",
            post(crate::http::query_lang_v1_operator),
        )
        .route(
            "/operators/columnar_eval_v1",
            post(crate::http::columnar_eval_v1_operator),
        )
        .route(
            "/operators/stream_window_v1",
            post(crate::http::stream_window_v1_operator),
        )
        .route(
            "/operators/stream_window_v2",
            post(crate::http::stream_window_v2_operator),
        )
        .route("/operators/sketch_v1", post(crate::http::sketch_v1_operator))
        .route(
            "/operators/explain_plan_v1",
            post(crate::http::explain_plan_v1_operator),
        )
        .route(
            "/operators/explain_plan_v2",
            post(crate::http::explain_plan_v2_operator),
        )
        .route(
            "/operators/optimizer_v1",
            post(crate::http::optimizer_v1_operator),
        ))
}
