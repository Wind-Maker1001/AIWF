use super::lineage::summarize_value;
use super::*;

pub(super) fn workflow_error(code: &str, operator: &str, message: impl Into<String>) -> String {
    AccelError::new(code, message)
        .with_operator(operator)
        .to_string()
}

pub(super) fn record_workflow_runtime_stat(
    run_id: Option<String>,
    op: &str,
    ok: bool,
    error: Option<&str>,
    duration_ms: u128,
) {
    let _ = run_runtime_stats_v1(RuntimeStatsV1Req {
        run_id,
        op: "record".to_string(),
        operator: Some(op.to_string()),
        ok: Some(ok),
        error_code: error.map(normalize_error_code),
        duration_ms: Some(duration_ms),
        rows_in: None,
        rows_out: None,
    });
}

pub(super) fn push_failed_workflow_step(
    trace: &mut Vec<WorkflowStepReplay>,
    id: &str,
    op: &str,
    started_at: String,
    duration_ms: u128,
    input_summary: Value,
    error: String,
) {
    trace.push(WorkflowStepReplay {
        id: id.to_string(),
        operator: op.to_string(),
        status: "failed".to_string(),
        started_at,
        finished_at: utc_now_iso(),
        duration_ms,
        input_summary,
        output_summary: None,
        error: Some(error),
    });
}

pub(super) fn push_success_workflow_step(
    trace: &mut Vec<WorkflowStepReplay>,
    step_key: (&str, &str),
    started_at: String,
    finished_at: String,
    duration_ms: u128,
    input_summary: Value,
    output: &Value,
) {
    let (id, op) = step_key;
    trace.push(WorkflowStepReplay {
        id: id.to_string(),
        operator: op.to_string(),
        status: "done".to_string(),
        started_at,
        finished_at,
        duration_ms,
        input_summary,
        output_summary: Some(summarize_value(output)),
        error: None,
    });
}

type WorkflowStepHandler = fn(&AppState, Value) -> Result<Value, String>;

struct WorkflowStepDefinition {
    op: &'static str,
    handler: WorkflowStepHandler,
}

fn parse_workflow_input<T: DeserializeOwned>(input: Value) -> Result<T, String> {
    serde_json::from_value::<T>(input).map_err(|e| e.to_string())
}

fn serialize_workflow_output<T: Serialize>(output: T) -> Result<Value, String> {
    serde_json::to_value(output).map_err(|e| e.to_string())
}

fn run_stateless_workflow_step<T, R, F>(input: Value, runner: F) -> Result<Value, String>
where
    T: DeserializeOwned,
    R: Serialize,
    F: FnOnce(T) -> Result<R, String>,
{
    let req = parse_workflow_input::<T>(input)?;
    let resp = runner(req)?;
    serialize_workflow_output(resp)
}

fn run_stateful_workflow_step<T, R, F>(
    state: &AppState,
    input: Value,
    runner: F,
) -> Result<Value, String>
where
    T: DeserializeOwned,
    R: Serialize,
    F: FnOnce(&AppState, T) -> Result<R, String>,
{
    let req = parse_workflow_input::<T>(input)?;
    let resp = runner(state, req)?;
    serialize_workflow_output(resp)
}

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

macro_rules! define_workflow_step_registry {
    ($($op:literal => $handler:path;)+) => {
        const WORKFLOW_STEP_DEFS: &[WorkflowStepDefinition] = &[
            $(
                WorkflowStepDefinition {
                    op: $op,
                    handler: $handler,
                },
            )+
        ];
    };
}

fn workflow_transform_rows_v2_handler(state: &AppState, input: Value) -> Result<Value, String> {
    let req = parse_workflow_input::<TransformRowsReq>(input)?;
    let resp = run_transform_rows_v2_with_cache(
        req,
        None,
        Some(&state.transform_cache),
        Some(&state.metrics),
    )?;
    serialize_workflow_output(resp)
}

fn workflow_plugin_health_v1_handler(_: &AppState, input: Value) -> Result<Value, String> {
    let req = parse_workflow_input::<PluginHealthReq>(input)?;
    let plugin = safe_pkg_token(&req.plugin)?;
    let details = run_plugin_healthcheck(&plugin, req.tenant_id.as_deref())?;
    Ok(json!({
        "ok": true,
        "operator": "plugin_health_v1",
        "status": "done",
        "plugin": plugin,
        "details": details
    }))
}

fn workflow_schema_registry_v2_register_handler(
    state: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<SchemaRegisterReq>(input)?;
    let mut resp = run_schema_registry_register_v1(state, req)?;
    resp.operator = "schema_registry_v2_register".to_string();
    serialize_workflow_output(resp)
}

fn workflow_schema_registry_v2_get_handler(
    state: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<SchemaGetReq>(input)?;
    let mut resp = run_schema_registry_get_v1(state, req)?;
    resp.operator = "schema_registry_v2_get".to_string();
    serialize_workflow_output(resp)
}

fn workflow_schema_registry_v2_infer_handler(
    state: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<SchemaInferReq>(input)?;
    let mut resp = run_schema_registry_infer_v1(state, req)?;
    resp.operator = "schema_registry_v2_infer".to_string();
    serialize_workflow_output(resp)
}

define_stateless_workflow_handlers! {
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

define_workflow_step_registry! {
    "transform_rows_v2" => workflow_transform_rows_v2_handler;
    "transform_rows_v3" => workflow_transform_rows_v3_handler;
    "text_preprocess_v2" => workflow_text_preprocess_v2_handler;
    "compute_metrics" => workflow_compute_metrics_handler;
    "join_rows_v1" => workflow_join_rows_v1_handler;
    "join_rows_v2" => workflow_join_rows_v2_handler;
    "join_rows_v3" => workflow_join_rows_v3_handler;
    "normalize_schema_v1" => workflow_normalize_schema_v1_handler;
    "entity_extract_v1" => workflow_entity_extract_v1_handler;
    "aggregate_rows_v1" => workflow_aggregate_rows_v1_handler;
    "aggregate_rows_v2" => workflow_aggregate_rows_v2_handler;
    "aggregate_rows_v3" => workflow_aggregate_rows_v3_handler;
    "quality_check_v1" => workflow_quality_check_v1_handler;
    "quality_check_v2" => workflow_quality_check_v2_handler;
    "quality_check_v3" => workflow_quality_check_v3_handler;
    "aggregate_pushdown_v1" => workflow_aggregate_pushdown_v1_handler;
    "plugin_exec_v1" => workflow_plugin_exec_v1_handler;
    "plugin_health_v1" => workflow_plugin_health_v1_handler;
    "plugin_registry_v1" => workflow_plugin_registry_v1_handler;
    "plugin_operator_v1" => workflow_plugin_operator_v1_handler;
    "rules_package_publish_v1" => workflow_rules_package_publish_v1_handler;
    "rules_package_get_v1" => workflow_rules_package_get_v1_handler;
    "load_rows_v2" => workflow_load_rows_v2_handler;
    "load_rows_v3" => workflow_load_rows_v3_handler;
    "schema_registry_v2_check_compat" => workflow_schema_registry_v2_check_compat_handler;
    "schema_registry_v2_suggest_migration" => workflow_schema_registry_v2_suggest_migration_handler;
    "udf_wasm_v1" => workflow_udf_wasm_v1_handler;
    "schema_registry_v1_register" => workflow_schema_registry_v1_register_handler;
    "schema_registry_v1_get" => workflow_schema_registry_v1_get_handler;
    "schema_registry_v1_infer" => workflow_schema_registry_v1_infer_handler;
    "schema_registry_v2_register" => workflow_schema_registry_v2_register_handler;
    "schema_registry_v2_get" => workflow_schema_registry_v2_get_handler;
    "schema_registry_v2_infer" => workflow_schema_registry_v2_infer_handler;
    "time_series_v1" => workflow_time_series_v1_handler;
    "stats_v1" => workflow_stats_v1_handler;
    "entity_linking_v1" => workflow_entity_linking_v1_handler;
    "table_reconstruct_v1" => workflow_table_reconstruct_v1_handler;
    "feature_store_v1_upsert" => workflow_feature_store_v1_upsert_handler;
    "feature_store_v1_get" => workflow_feature_store_v1_get_handler;
    "lineage_v2" => workflow_lineage_v2_handler;
    "rule_simulator_v1" => workflow_rule_simulator_v1_handler;
    "constraint_solver_v1" => workflow_constraint_solver_v1_handler;
    "chart_data_prep_v1" => workflow_chart_data_prep_v1_handler;
    "diff_audit_v1" => workflow_diff_audit_v1_handler;
    "vector_index_v1_build" => workflow_vector_index_v1_build_handler;
    "vector_index_v1_search" => workflow_vector_index_v1_search_handler;
    "evidence_rank_v1" => workflow_evidence_rank_v1_handler;
    "fact_crosscheck_v1" => workflow_fact_crosscheck_v1_handler;
    "timeseries_forecast_v1" => workflow_timeseries_forecast_v1_handler;
    "finance_ratio_v1" => workflow_finance_ratio_v1_handler;
    "anomaly_explain_v1" => workflow_anomaly_explain_v1_handler;
    "template_bind_v1" => workflow_template_bind_v1_handler;
    "provenance_sign_v1" => workflow_provenance_sign_v1_handler;
    "stream_state_v1_save" => workflow_stream_state_v1_save_handler;
    "stream_state_v1_load" => workflow_stream_state_v1_load_handler;
    "query_lang_v1" => workflow_query_lang_v1_handler;
    "columnar_eval_v1" => workflow_columnar_eval_v1_handler;
    "stream_window_v1" => workflow_stream_window_v1_handler;
    "stream_window_v2" => workflow_stream_window_v2_handler;
    "sketch_v1" => workflow_sketch_v1_handler;
    "runtime_stats_v1" => workflow_runtime_stats_v1_handler;
    "capabilities_v1" => workflow_capabilities_v1_handler;
    "io_contract_v1" => workflow_io_contract_v1_handler;
    "failure_policy_v1" => workflow_failure_policy_v1_handler;
    "window_rows_v1" => workflow_window_rows_v1_handler;
    "optimizer_v1" => workflow_optimizer_v1_handler;
    "join_rows_v4" => workflow_join_rows_v4_handler;
    "aggregate_rows_v4" => workflow_aggregate_rows_v4_handler;
    "quality_check_v4" => workflow_quality_check_v4_handler;
    "lineage_v3" => workflow_lineage_v3_handler;
    "parquet_io_v2" => workflow_parquet_io_v2_handler;
    "stream_state_v2" => workflow_stream_state_v2_handler;
    "udf_wasm_v2" => workflow_udf_wasm_v2_handler;
    "explain_plan_v1" => workflow_explain_plan_v1_handler;
    "explain_plan_v2" => workflow_explain_plan_v2_handler;
    "incremental_plan_v1" => workflow_incremental_plan_v1_handler;
    "tenant_isolation_v1" => workflow_tenant_isolation_v1_handler;
    "operator_policy_v1" => workflow_operator_policy_v1_handler;
    "optimizer_adaptive_v2" => workflow_optimizer_adaptive_v2_handler;
    "vector_index_v2_build" => workflow_vector_index_v2_build_handler;
    "vector_index_v2_search" => workflow_vector_index_v2_search_handler;
    "vector_index_v2_eval" => workflow_vector_index_v2_eval_handler;
    "stream_reliability_v1" => workflow_stream_reliability_v1_handler;
    "lineage_provenance_v1" => workflow_lineage_provenance_v1_handler;
    "contract_regression_v1" => workflow_contract_regression_v1_handler;
    "perf_baseline_v1" => workflow_perf_baseline_v1_handler;
}

pub(super) fn execute_workflow_step(
    state: &AppState,
    op: &str,
    input: Value,
) -> Result<Value, String> {
    let handler = WORKFLOW_STEP_DEFS
        .iter()
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
