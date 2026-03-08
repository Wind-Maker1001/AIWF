use crate::{
    LineageV2Req,
    analysis_ops::{
        run_chart_data_prep_v1, run_constraint_solver_v1, run_diff_audit_v1, run_entity_linking_v1,
        run_feature_store_get_v1, run_feature_store_upsert_v1, run_rule_simulator_v1, run_stats_v1,
        run_table_reconstruct_v1, run_time_series_v1,
    },
    api_types::{
        AnomalyExplainReq, ChartDataPrepReq, ConstraintSolverReq, DiffAuditReq, EntityLinkReq,
        ErrResp, EvidenceRankReq, FactCrosscheckReq, FeatureStoreGetReq, FeatureStoreUpsertReq,
        FinanceRatioReq, ProvenanceSignReq, QueryLangReq, RuleSimulatorReq, StatsReq,
        StreamStateLoadReq, StreamStateSaveReq, TableReconstructReq, TemplateBindReq,
        TimeSeriesForecastReq, TimeSeriesReq, UdfWasmReq, VectorIndexBuildReq,
        VectorIndexSearchReq,
    },
    run_anomaly_explain_v1, run_evidence_rank_v1, run_fact_crosscheck_v1, run_finance_ratio_v1,
    run_lineage_v2, run_provenance_sign_v1, run_query_lang_v1, run_stream_state_load_v1,
    run_stream_state_save_v1, run_template_bind_v1, run_timeseries_forecast_v1,
    run_vector_index_build_v1, run_vector_index_search_v1,
    wasm_ops::run_udf_wasm_v1,
};
use accel_rust::{app_state::AppState, metrics::observe_operator_latency_v2};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use std::time::Instant;

pub(crate) async fn udf_wasm_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<UdfWasmReq>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.udf_wasm_v1_calls += 1;
    }
    match run_udf_wasm_v1(req) {
        Ok(resp) => {
            observe_operator_latency_v2(&state.metrics, "udf_wasm_v1", begin.elapsed().as_millis());
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "udf_wasm_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn time_series_v1_operator(Json(req): Json<TimeSeriesReq>) -> impl IntoResponse {
    match run_time_series_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "time_series_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn stats_v1_operator(Json(req): Json<StatsReq>) -> impl IntoResponse {
    match run_stats_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stats_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn entity_linking_v1_operator(
    Json(req): Json<EntityLinkReq>,
) -> impl IntoResponse {
    match run_entity_linking_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "entity_linking_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn table_reconstruct_v1_operator(
    Json(req): Json<TableReconstructReq>,
) -> impl IntoResponse {
    match run_table_reconstruct_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "table_reconstruct_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn feature_store_upsert_v1_operator(
    Json(req): Json<FeatureStoreUpsertReq>,
) -> impl IntoResponse {
    match run_feature_store_upsert_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "feature_store_v1_upsert".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn feature_store_get_v1_operator(
    Json(req): Json<FeatureStoreGetReq>,
) -> impl IntoResponse {
    match run_feature_store_get_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "feature_store_v1_get".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn lineage_v2_operator(Json(req): Json<LineageV2Req>) -> impl IntoResponse {
    match run_lineage_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "lineage_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn rule_simulator_v1_operator(
    Json(req): Json<RuleSimulatorReq>,
) -> impl IntoResponse {
    match run_rule_simulator_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "rule_simulator_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn constraint_solver_v1_operator(
    Json(req): Json<ConstraintSolverReq>,
) -> impl IntoResponse {
    match run_constraint_solver_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "constraint_solver_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn chart_data_prep_v1_operator(
    Json(req): Json<ChartDataPrepReq>,
) -> impl IntoResponse {
    match run_chart_data_prep_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "chart_data_prep_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn diff_audit_v1_operator(Json(req): Json<DiffAuditReq>) -> impl IntoResponse {
    match run_diff_audit_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "diff_audit_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn vector_index_build_v1_operator(
    Json(req): Json<VectorIndexBuildReq>,
) -> impl IntoResponse {
    match run_vector_index_build_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v1_build".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn vector_index_search_v1_operator(
    Json(req): Json<VectorIndexSearchReq>,
) -> impl IntoResponse {
    match run_vector_index_search_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v1_search".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn evidence_rank_v1_operator(
    Json(req): Json<EvidenceRankReq>,
) -> impl IntoResponse {
    match run_evidence_rank_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "evidence_rank_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn fact_crosscheck_v1_operator(
    Json(req): Json<FactCrosscheckReq>,
) -> impl IntoResponse {
    match run_fact_crosscheck_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "fact_crosscheck_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn timeseries_forecast_v1_operator(
    Json(req): Json<TimeSeriesForecastReq>,
) -> impl IntoResponse {
    match run_timeseries_forecast_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "timeseries_forecast_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn finance_ratio_v1_operator(
    Json(req): Json<FinanceRatioReq>,
) -> impl IntoResponse {
    match run_finance_ratio_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "finance_ratio_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn anomaly_explain_v1_operator(
    Json(req): Json<AnomalyExplainReq>,
) -> impl IntoResponse {
    match run_anomaly_explain_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "anomaly_explain_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn template_bind_v1_operator(
    Json(req): Json<TemplateBindReq>,
) -> impl IntoResponse {
    match run_template_bind_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "template_bind_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn provenance_sign_v1_operator(
    Json(req): Json<ProvenanceSignReq>,
) -> impl IntoResponse {
    match run_provenance_sign_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "provenance_sign_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn stream_state_save_v1_operator(
    Json(req): Json<StreamStateSaveReq>,
) -> impl IntoResponse {
    match run_stream_state_save_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_state_v1_save".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn stream_state_load_v1_operator(
    Json(req): Json<StreamStateLoadReq>,
) -> impl IntoResponse {
    match run_stream_state_load_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_state_v1_load".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn query_lang_v1_operator(Json(req): Json<QueryLangReq>) -> impl IntoResponse {
    match run_query_lang_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "query_lang_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}
