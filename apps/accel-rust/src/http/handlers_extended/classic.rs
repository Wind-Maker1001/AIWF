use crate::{
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
    operators::workflow::{LineageV2Req, run_lineage_v2},
    platform_ops::{
        run_anomaly_explain_v1, run_evidence_rank_v1, run_fact_crosscheck_v1, run_finance_ratio_v1,
        run_provenance_sign_v1, run_query_lang_v1, run_stream_state_load_v1,
        run_stream_state_save_v1, run_template_bind_v1, run_timeseries_forecast_v1,
        run_vector_index_build_v1, run_vector_index_search_v1,
    },
    wasm_ops::run_udf_wasm_v1,
};
use accel_rust::{app_state::AppState, metrics::observe_operator_latency_v2};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use std::time::Instant;

#[path = "classic/udf.rs"]
mod udf;
pub(crate) use udf::udf_wasm_v1_operator;

#[path = "classic/analytics.rs"]
mod analytics;
pub(crate) use analytics::{
    chart_data_prep_v1_operator, constraint_solver_v1_operator, diff_audit_v1_operator,
    entity_linking_v1_operator, feature_store_get_v1_operator, feature_store_upsert_v1_operator,
    lineage_v2_operator, rule_simulator_v1_operator, stats_v1_operator,
    table_reconstruct_v1_operator, time_series_v1_operator,
};

#[path = "classic/intelligence.rs"]
mod intelligence;
pub(crate) use intelligence::{
    anomaly_explain_v1_operator, evidence_rank_v1_operator, fact_crosscheck_v1_operator,
    finance_ratio_v1_operator, provenance_sign_v1_operator, template_bind_v1_operator,
    timeseries_forecast_v1_operator, vector_index_build_v1_operator,
    vector_index_search_v1_operator,
};

#[path = "classic/state.rs"]
mod state;
pub(crate) use state::{
    query_lang_v1_operator, stream_state_load_v1_operator, stream_state_save_v1_operator,
};
