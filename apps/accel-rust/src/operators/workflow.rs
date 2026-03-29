use crate::{
    analysis_ops::{
        run_chart_data_prep_v1, run_constraint_solver_v1, run_diff_audit_v1, run_entity_linking_v1,
        run_feature_store_get_v1, run_feature_store_upsert_v1, run_rule_simulator_v1, run_stats_v1,
        run_table_reconstruct_v1, run_time_series_v1,
    },
    api_types::{
        AggregatePushdownReq, AnomalyExplainReq, CapabilitiesV1Req, ChartDataPrepReq, CleaningReq,
        ColumnarEvalV1Req, ComputeReq, ConstraintSolverReq, ContractRegressionV1Req, DiffAuditReq,
        EntityExtractReq, EntityLinkReq, EvidenceRankReq, ExplainPlanV1Req, ExplainPlanV2Req,
        FactCrosscheckReq, FailurePolicyV1Req, FeatureStoreGetReq, FeatureStoreUpsertReq,
        FinanceRatioReq, IncrementalPlanV1Req, IoContractV1Req, LineageProvenanceV1Req,
        LoadRowsV2Req, LoadRowsV3Req, NormalizeSchemaReq, OperatorPolicyV1Req,
        OptimizerAdaptiveV2Req, OptimizerV1Req, ParquetIoV2Req, PerfBaselineV1Req, PluginExecReq,
        PluginHealthReq, PluginOperatorV1Req, PluginRegistryV1Req, ProvenanceSignReq, QueryLangReq,
        RuleSimulatorReq, RulesPackageGetReq, RulesPackagePublishReq, RuntimeStatsV1Req,
        SchemaCompatReq, SchemaGetReq, SchemaInferReq, SchemaMigrationSuggestReq,
        SchemaRegisterReq, SketchV1Req, StatsReq, StreamReliabilityV1Req, StreamStateLoadReq,
        StreamStateSaveReq, StreamStateV2Req, StreamWindowV1Req, StreamWindowV2Req,
        TableReconstructReq, TemplateBindReq, TenantIsolationV1Req, TextPreprocessReq,
        TimeSeriesForecastReq, TimeSeriesReq, UdfWasmReq, UdfWasmV2Req, VectorIndexBuildReq,
        VectorIndexBuildV2Req, VectorIndexEvalV2Req, VectorIndexSearchReq, VectorIndexSearchV2Req,
        WindowRowsV1Req,
    },
    cleaning_runtime::{run_cleaning_operator, run_compute_metrics},
    execution_ops::{
        run_explain_plan_v1, run_optimizer_v1, run_parquet_io_v2, run_stream_state_v2,
        run_udf_wasm_v2, run_window_rows_v1,
    },
    governance_ops::{
        run_capabilities_v1, run_contract_regression_v1, run_explain_plan_v2,
        run_failure_policy_v1, run_incremental_plan_v1, run_io_contract_v1,
        run_lineage_provenance_v1, run_operator_policy_v1, run_optimizer_adaptive_v2,
        run_perf_baseline_v1, run_runtime_stats_v1, run_stream_reliability_v1,
        run_tenant_isolation_v1, run_vector_index_build_v2, run_vector_index_eval_v2,
        run_vector_index_search_v2,
    },
    load_ops::{run_load_rows_v2, run_load_rows_v3},
    misc_ops::{
        run_aggregate_pushdown_v1, run_entity_extract_v1, run_normalize_schema_v1,
        run_rules_package_get_v1, run_rules_package_publish_v1, run_text_preprocess_v2,
        safe_pkg_token,
    },
    operators::{
        analytics::{
            AggregateRowsReq, AggregateRowsV2Req, AggregateRowsV3Req, AggregateRowsV4Req,
            QualityCheckReq, QualityCheckV2Req, QualityCheckV3Req, QualityCheckV4Req,
            run_aggregate_rows_v1, run_aggregate_rows_v2, run_aggregate_rows_v3,
            run_aggregate_rows_v4, run_quality_check_v1, run_quality_check_v2,
            run_quality_check_v3, run_quality_check_v4,
        },
        join::{
            JoinRowsReq, JoinRowsV2Req, JoinRowsV3Req, JoinRowsV4Req, run_join_rows_v1,
            run_join_rows_v2, run_join_rows_v3, run_join_rows_v4,
        },
        transform::{
            TransformRowsReq, TransformRowsV3Req, collect_expr_lineage,
            run_transform_rows_v2_with_cache, run_transform_rows_v3,
        },
    },
    platform_ops::{
        normalize_error_code, run_anomaly_explain_v1, run_columnar_eval_v1, run_evidence_rank_v1,
        run_fact_crosscheck_v1, run_finance_ratio_v1, run_provenance_sign_v1, run_query_lang_v1,
        run_sketch_v1, run_stream_state_load_v1, run_stream_state_save_v1, run_stream_window_v1,
        run_stream_window_v2, run_template_bind_v1, run_timeseries_forecast_v1,
        run_vector_index_build_v1, run_vector_index_search_v1,
    },
    plugin_runtime::{
        run_plugin_exec_v1, run_plugin_healthcheck, run_plugin_operator_v1, run_plugin_registry_v1,
    },
    schema_registry::{
        run_schema_registry_check_compat_v2, run_schema_registry_get_v1,
        run_schema_registry_infer_v1, run_schema_registry_register_v1,
        run_schema_registry_suggest_migration_v2,
    },
    transform_support::{
        operator_allowed_for_tenant, resolve_trace_id, tenant_max_workflow_steps_for, utc_now_iso,
        value_to_string,
    },
    wasm_ops::run_udf_wasm_v1,
};
use accel_rust::app_state::AppState;
use accel_rust::error::AccelError;
use regex::Regex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::OnceLock, time::Instant};

#[cfg(test)]
use accel_rust::{app_state::ServiceMetrics, task_store::task_store_config_from_env};

#[cfg(test)]
use crate::schema_registry::load_schema_registry_store;

#[cfg(test)]
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

mod types;
pub(crate) use types::*;

mod lineage;
pub(crate) use lineage::{run_lineage_v2, run_lineage_v3};

mod custom;
mod engine;
mod support;

mod runner;
#[cfg(test)]
pub(crate) use runner::run_workflow;
pub(crate) use engine::workflow_step_operator_names;
pub(crate) use runner::run_workflow_with_state;
