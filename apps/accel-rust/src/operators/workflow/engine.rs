use super::custom::{
    workflow_plugin_health_v1_handler, workflow_schema_registry_v2_get_handler,
    workflow_schema_registry_v2_infer_handler, workflow_schema_registry_v2_register_handler,
    workflow_transform_rows_v2_handler,
};
use super::support::{run_stateful_workflow_step, run_stateless_workflow_step, workflow_error};
use super::*;
use crate::operator_catalog::resolve_operator_metadata;

type WorkflowStepHandler = fn(&AppState, Value) -> Result<Value, String>;

struct WorkflowStepDefinition {
    op: &'static str,
    handler: WorkflowStepHandler,
}

#[path = "engine_domains/transform.rs"]
mod transform_domain;
#[path = "engine_domains/table.rs"]
mod table_domain;
#[path = "engine_domains/integration.rs"]
mod integration_domain;
#[path = "engine_domains/storage_schema.rs"]
mod storage_schema_domain;
#[path = "engine_domains/analysis.rs"]
mod analysis_domain;
#[path = "engine_domains/governance.rs"]
mod governance_domain;

macro_rules! define_stateless_workflow_handlers {
    ($($fn_name:ident => $op:literal, $req:ty, $runner:path;)+) => {
        $(
            fn $fn_name(_: &AppState, input: Value) -> Result<Value, String> {
                run_stateless_workflow_step::<$req, _, _>(input, $runner)
            }
        )+
    };
}

macro_rules! define_stateful_workflow_handlers {
    ($($fn_name:ident => $op:literal, $req:ty, $runner:path;)+) => {
        $(
            fn $fn_name(state: &AppState, input: Value) -> Result<Value, String> {
                run_stateful_workflow_step::<$req, _, _>(state, input, $runner)
            }
        )+
    };
}

define_stateless_workflow_handlers! {
    workflow_cleaning_handler => "cleaning", CleaningReq, run_cleaning_operator;
    workflow_transform_rows_v3_handler => "transform_rows_v3", TransformRowsV3Req, run_transform_rows_v3;
    workflow_text_preprocess_v2_handler => "text_preprocess_v2", TextPreprocessReq, run_text_preprocess_v2;
    workflow_compute_metrics_handler => "compute_metrics", ComputeReq, run_compute_metrics;
    workflow_join_rows_v1_handler => "join_rows_v1", JoinRowsReq, run_join_rows_v1;
    workflow_join_rows_v2_handler => "join_rows_v2", JoinRowsV2Req, run_join_rows_v2;
    workflow_join_rows_v3_handler => "join_rows_v3", JoinRowsV3Req, run_join_rows_v3;
    workflow_normalize_schema_v1_handler => "normalize_schema_v1", NormalizeSchemaReq, run_normalize_schema_v1;
    workflow_entity_extract_v1_handler => "entity_extract_v1", EntityExtractReq, run_entity_extract_v1;
    workflow_aggregate_rows_v1_handler => "aggregate_rows_v1", AggregateRowsReq, run_aggregate_rows_v1;
    workflow_aggregate_rows_v2_handler => "aggregate_rows_v2", AggregateRowsV2Req, run_aggregate_rows_v2;
    workflow_aggregate_rows_v3_handler => "aggregate_rows_v3", AggregateRowsV3Req, run_aggregate_rows_v3;
    workflow_quality_check_v1_handler => "quality_check_v1", QualityCheckReq, run_quality_check_v1;
    workflow_quality_check_v2_handler => "quality_check_v2", QualityCheckV2Req, run_quality_check_v2;
    workflow_quality_check_v3_handler => "quality_check_v3", QualityCheckV3Req, run_quality_check_v3;
    workflow_aggregate_pushdown_v1_handler => "aggregate_pushdown_v1", AggregatePushdownReq, run_aggregate_pushdown_v1;
    workflow_plugin_exec_v1_handler => "plugin_exec_v1", PluginExecReq, run_plugin_exec_v1;
    workflow_plugin_registry_v1_handler => "plugin_registry_v1", PluginRegistryV1Req, run_plugin_registry_v1;
    workflow_plugin_operator_v1_handler => "plugin_operator_v1", PluginOperatorV1Req, run_plugin_operator_v1;
    workflow_rules_package_publish_v1_handler => "rules_package_publish_v1", RulesPackagePublishReq, run_rules_package_publish_v1;
    workflow_rules_package_get_v1_handler => "rules_package_get_v1", RulesPackageGetReq, run_rules_package_get_v1;
    workflow_load_rows_v2_handler => "load_rows_v2", LoadRowsV2Req, run_load_rows_v2;
    workflow_load_rows_v3_handler => "load_rows_v3", LoadRowsV3Req, run_load_rows_v3;
    workflow_udf_wasm_v1_handler => "udf_wasm_v1", UdfWasmReq, run_udf_wasm_v1;
    workflow_time_series_v1_handler => "time_series_v1", TimeSeriesReq, run_time_series_v1;
    workflow_stats_v1_handler => "stats_v1", StatsReq, run_stats_v1;
    workflow_entity_linking_v1_handler => "entity_linking_v1", EntityLinkReq, run_entity_linking_v1;
    workflow_table_reconstruct_v1_handler => "table_reconstruct_v1", TableReconstructReq, run_table_reconstruct_v1;
    workflow_feature_store_v1_upsert_handler => "feature_store_v1_upsert", FeatureStoreUpsertReq, run_feature_store_upsert_v1;
    workflow_feature_store_v1_get_handler => "feature_store_v1_get", FeatureStoreGetReq, run_feature_store_get_v1;
    workflow_lineage_v2_handler => "lineage_v2", LineageV2Req, run_lineage_v2;
    workflow_rule_simulator_v1_handler => "rule_simulator_v1", RuleSimulatorReq, run_rule_simulator_v1;
    workflow_constraint_solver_v1_handler => "constraint_solver_v1", ConstraintSolverReq, run_constraint_solver_v1;
    workflow_chart_data_prep_v1_handler => "chart_data_prep_v1", ChartDataPrepReq, run_chart_data_prep_v1;
    workflow_diff_audit_v1_handler => "diff_audit_v1", DiffAuditReq, run_diff_audit_v1;
    workflow_vector_index_v1_build_handler => "vector_index_v1_build", VectorIndexBuildReq, run_vector_index_build_v1;
    workflow_vector_index_v1_search_handler => "vector_index_v1_search", VectorIndexSearchReq, run_vector_index_search_v1;
    workflow_evidence_rank_v1_handler => "evidence_rank_v1", EvidenceRankReq, run_evidence_rank_v1;
    workflow_fact_crosscheck_v1_handler => "fact_crosscheck_v1", FactCrosscheckReq, run_fact_crosscheck_v1;
    workflow_timeseries_forecast_v1_handler => "timeseries_forecast_v1", TimeSeriesForecastReq, run_timeseries_forecast_v1;
    workflow_finance_ratio_v1_handler => "finance_ratio_v1", FinanceRatioReq, run_finance_ratio_v1;
    workflow_anomaly_explain_v1_handler => "anomaly_explain_v1", AnomalyExplainReq, run_anomaly_explain_v1;
    workflow_template_bind_v1_handler => "template_bind_v1", TemplateBindReq, run_template_bind_v1;
    workflow_provenance_sign_v1_handler => "provenance_sign_v1", ProvenanceSignReq, run_provenance_sign_v1;
    workflow_stream_state_v1_save_handler => "stream_state_v1_save", StreamStateSaveReq, run_stream_state_save_v1;
    workflow_stream_state_v1_load_handler => "stream_state_v1_load", StreamStateLoadReq, run_stream_state_load_v1;
    workflow_query_lang_v1_handler => "query_lang_v1", QueryLangReq, run_query_lang_v1;
    workflow_columnar_eval_v1_handler => "columnar_eval_v1", ColumnarEvalV1Req, run_columnar_eval_v1;
    workflow_stream_window_v1_handler => "stream_window_v1", StreamWindowV1Req, run_stream_window_v1;
    workflow_stream_window_v2_handler => "stream_window_v2", StreamWindowV2Req, run_stream_window_v2;
    workflow_sketch_v1_handler => "sketch_v1", SketchV1Req, run_sketch_v1;
    workflow_runtime_stats_v1_handler => "runtime_stats_v1", RuntimeStatsV1Req, run_runtime_stats_v1;
    workflow_capabilities_v1_handler => "capabilities_v1", CapabilitiesV1Req, run_capabilities_v1;
    workflow_io_contract_v1_handler => "io_contract_v1", IoContractV1Req, run_io_contract_v1;
    workflow_failure_policy_v1_handler => "failure_policy_v1", FailurePolicyV1Req, run_failure_policy_v1;
    workflow_window_rows_v1_handler => "window_rows_v1", WindowRowsV1Req, run_window_rows_v1;
    workflow_optimizer_v1_handler => "optimizer_v1", OptimizerV1Req, run_optimizer_v1;
    workflow_join_rows_v4_handler => "join_rows_v4", JoinRowsV4Req, run_join_rows_v4;
    workflow_aggregate_rows_v4_handler => "aggregate_rows_v4", AggregateRowsV4Req, run_aggregate_rows_v4;
    workflow_quality_check_v4_handler => "quality_check_v4", QualityCheckV4Req, run_quality_check_v4;
    workflow_lineage_v3_handler => "lineage_v3", LineageV3Req, run_lineage_v3;
    workflow_parquet_io_v2_handler => "parquet_io_v2", ParquetIoV2Req, run_parquet_io_v2;
    workflow_stream_state_v2_handler => "stream_state_v2", StreamStateV2Req, run_stream_state_v2;
    workflow_udf_wasm_v2_handler => "udf_wasm_v2", UdfWasmV2Req, run_udf_wasm_v2;
    workflow_explain_plan_v1_handler => "explain_plan_v1", ExplainPlanV1Req, run_explain_plan_v1;
    workflow_explain_plan_v2_handler => "explain_plan_v2", ExplainPlanV2Req, run_explain_plan_v2;
    workflow_tenant_isolation_v1_handler => "tenant_isolation_v1", TenantIsolationV1Req, run_tenant_isolation_v1;
    workflow_operator_policy_v1_handler => "operator_policy_v1", OperatorPolicyV1Req, run_operator_policy_v1;
    workflow_optimizer_adaptive_v2_handler => "optimizer_adaptive_v2", OptimizerAdaptiveV2Req, run_optimizer_adaptive_v2;
    workflow_vector_index_v2_build_handler => "vector_index_v2_build", VectorIndexBuildV2Req, run_vector_index_build_v2;
    workflow_vector_index_v2_search_handler => "vector_index_v2_search", VectorIndexSearchV2Req, run_vector_index_search_v2;
    workflow_vector_index_v2_eval_handler => "vector_index_v2_eval", VectorIndexEvalV2Req, run_vector_index_eval_v2;
    workflow_stream_reliability_v1_handler => "stream_reliability_v1", StreamReliabilityV1Req, run_stream_reliability_v1;
    workflow_lineage_provenance_v1_handler => "lineage_provenance_v1", LineageProvenanceV1Req, run_lineage_provenance_v1;
    workflow_contract_regression_v1_handler => "contract_regression_v1", ContractRegressionV1Req, run_contract_regression_v1;
    workflow_perf_baseline_v1_handler => "perf_baseline_v1", PerfBaselineV1Req, run_perf_baseline_v1;
}

define_stateful_workflow_handlers! {
    workflow_schema_registry_v2_check_compat_handler => "schema_registry_v2_check_compat", SchemaCompatReq, run_schema_registry_check_compat_v2;
    workflow_schema_registry_v2_suggest_migration_handler => "schema_registry_v2_suggest_migration", SchemaMigrationSuggestReq, run_schema_registry_suggest_migration_v2;
    workflow_schema_registry_v1_register_handler => "schema_registry_v1_register", SchemaRegisterReq, run_schema_registry_register_v1;
    workflow_schema_registry_v1_get_handler => "schema_registry_v1_get", SchemaGetReq, run_schema_registry_get_v1;
    workflow_schema_registry_v1_infer_handler => "schema_registry_v1_infer", SchemaInferReq, run_schema_registry_infer_v1;
    workflow_incremental_plan_v1_handler => "incremental_plan_v1", IncrementalPlanV1Req, run_incremental_plan_v1;
}

const WORKFLOW_STEP_DEF_GROUPS: &[&[WorkflowStepDefinition]] = &[
    transform_domain::WORKFLOW_STEP_DEFS,
    table_domain::WORKFLOW_STEP_DEFS,
    integration_domain::WORKFLOW_STEP_DEFS,
    storage_schema_domain::WORKFLOW_STEP_DEFS,
    analysis_domain::WORKFLOW_STEP_DEFS,
    governance_domain::WORKFLOW_STEP_DEFS,
];

fn workflow_step_definitions() -> impl Iterator<Item = &'static WorkflowStepDefinition> {
    WORKFLOW_STEP_DEF_GROUPS.iter().flat_map(|group| group.iter())
}

pub(crate) fn workflow_step_operator_names() -> Vec<&'static str> {
    workflow_step_definitions().map(|step| step.op).collect()
}

fn workflow_operator_is_stateful(op: &str) -> bool {
    matches!(
        op,
        "transform_rows_v2"
            | "schema_registry_v2_check_compat"
            | "schema_registry_v2_suggest_migration"
            | "schema_registry_v1_register"
            | "schema_registry_v1_get"
            | "schema_registry_v1_infer"
            | "schema_registry_v2_register"
            | "schema_registry_v2_get"
            | "schema_registry_v2_infer"
            | "incremental_plan_v1"
    )
}

pub(super) fn workflow_resolution_metadata(op: &str) -> Value {
    let requested = op.trim();
    let supported = workflow_step_definitions().any(|step| step.op == requested);
    let mut metadata = resolve_operator_metadata(requested)
        .map(|entry| entry.to_workflow_resolution_metadata())
        .unwrap_or_else(|| json!({ "operator": requested }));
    if let Some(obj) = metadata.as_object_mut() {
        obj.insert(
            "workflow".to_string(),
            json!({
                "supported": supported,
                "stateful": supported && workflow_operator_is_stateful(requested)
            }),
        );
    }
    metadata
}

pub(super) fn execute_workflow_step(
    state: &AppState,
    op: &str,
    input: Value,
) -> Result<Value, String> {
    let handler = workflow_step_definitions()
        .find(|step| step.op == op)
        .map(|step| step.handler)
        .ok_or_else(|| {
            workflow_error(
                "unsupported_workflow_operator",
                "workflow_run",
                format!("unsupported workflow operator: {op}"),
            )
        })?;
    handler(state, input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_resolution_metadata_covers_registered_steps() {
        for step in workflow_step_definitions() {
            let metadata = workflow_resolution_metadata(step.op);
            assert_eq!(
                metadata
                    .get("workflow")
                    .and_then(|v| v.get("supported"))
                    .and_then(|v| v.as_bool()),
                Some(true),
                "workflow support missing for {}",
                step.op
            );
            assert!(
                metadata.get("domain").and_then(|v| v.as_str()).is_some(),
                "domain missing for {}",
                step.op
            );
            assert!(
                metadata.get("catalog").and_then(|v| v.as_str()).is_some(),
                "catalog missing for {}",
                step.op
            );
        }
    }

    #[test]
    fn workflow_resolution_metadata_marks_stateful_steps() {
        let metadata = workflow_resolution_metadata("incremental_plan_v1");
        assert_eq!(
            metadata
                .get("workflow")
                .and_then(|v| v.get("stateful"))
                .and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            metadata.get("catalog").and_then(|v| v.as_str()),
            Some("governance.planning")
        );
    }
}
