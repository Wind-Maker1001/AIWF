use super::OperatorMetadata;
use std::sync::OnceLock;

pub(super) fn published_operator_catalog() -> &'static [OperatorMetadata] {
    static CATALOG: OnceLock<Vec<OperatorMetadata>> = OnceLock::new();
    CATALOG
        .get_or_init(|| {
            vec![
                published_entry("transform_rows_v3", true, true, true, true),
                published_entry("load_rows_v3", true, false, true, true),
                published_entry("join_rows_v4", false, false, false, true),
                published_entry("aggregate_rows_v4", false, false, false, true),
                published_entry("quality_check_v4", false, false, false, true),
                published_entry("stream_window_v2", true, false, true, true),
                published_entry("stream_state_v2", true, false, true, true),
                published_entry("columnar_eval_v1", false, false, false, true),
                published_entry("runtime_stats_v1", false, false, false, true),
                published_entry("explain_plan_v2", false, false, false, true),
                published_entry("finance_ratio_v1", false, false, false, true),
                published_entry("anomaly_explain_v1", false, false, false, true),
                published_entry("plugin_operator_v1", false, false, false, true),
                published_entry("capabilities_v1", false, false, false, true),
                published_entry("io_contract_v1", false, false, false, true),
                published_entry("failure_policy_v1", false, false, false, true),
                published_entry("incremental_plan_v1", false, true, true, true),
                published_entry("tenant_isolation_v1", false, false, false, true),
                published_entry("operator_policy_v1", false, false, false, true),
                published_entry("optimizer_adaptive_v2", false, false, false, true),
                published_entry("vector_index_v2_build", false, false, false, true),
                published_entry("vector_index_v2_search", false, false, false, true),
                published_entry("stream_reliability_v1", true, false, true, true),
                published_entry("lineage_provenance_v1", false, false, false, true),
                published_entry("contract_regression_v1", false, false, false, true),
                published_entry("perf_baseline_v1", false, true, false, true),
            ]
        })
        .as_slice()
}

pub(super) fn normalize_operator(operator: &str) -> String {
    operator.trim().to_lowercase()
}

pub(super) fn infer_operator_version(operator: &str) -> Option<String> {
    operator
        .split('_')
        .rev()
        .find_map(|part| part.strip_prefix('v'))
        .filter(|digits| !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit()))
        .map(|digits| format!("v{digits}"))
}

pub(super) fn infer_streaming(operator: &str) -> bool {
    matches!(
        operator,
        "transform_rows_v2"
            | "transform_rows_v3"
            | "load_rows_v2"
            | "load_rows_v3"
            | "stream_window_v1"
            | "stream_window_v2"
            | "stream_state_v1_save"
            | "stream_state_v1_load"
            | "stream_state_v2"
            | "stream_reliability_v1"
    )
}

pub(super) fn infer_cache(operator: &str) -> bool {
    matches!(
        operator,
        "transform_rows_v2" | "transform_rows_v3" | "incremental_plan_v1" | "perf_baseline_v1"
    )
}

pub(super) fn infer_checkpoint(operator: &str) -> bool {
    matches!(
        operator,
        "load_rows_v3"
            | "stream_window_v2"
            | "stream_state_v2"
            | "stream_reliability_v1"
            | "incremental_plan_v1"
    )
}

pub(super) fn infer_io_contract(operator: &str) -> bool {
    matches!(
        operator,
        "transform_rows_v2"
            | "transform_rows_v3"
            | "load_rows_v3"
            | "finance_ratio_v1"
            | "anomaly_explain_v1"
            | "stream_window_v2"
            | "plugin_operator_v1"
    )
}

pub(super) fn infer_desktop_hidden(operator: &str) -> bool {
    matches!(
        operator,
        "aggregate_pushdown_v1"
            | "aggregate_rows_v1"
            | "cleaning"
            | "compute_metrics"
            | "entity_extract_v1"
            | "join_rows_v1"
            | "normalize_schema_v1"
            | "plugin_exec_v1"
            | "quality_check_v1"
            | "rules_package_get_v1"
            | "rules_package_publish_v1"
            | "text_preprocess_v2"
    )
}

pub(super) fn infer_domain_catalog(operator: &str) -> Option<(&'static str, &'static str)> {
    match operator {
        "cleaning" | "compute_metrics" => Some(("transform", "cleaning_runtime")),
        "transform_rows_v2" | "transform_rows_v3" => Some(("transform", "operators.transform")),
        "text_preprocess_v2"
        | "normalize_schema_v1"
        | "entity_extract_v1"
        | "aggregate_pushdown_v1"
        | "rules_package_publish_v1"
        | "rules_package_get_v1" => Some(("transform", "misc_ops")),
        "load_rows_v2" | "load_rows_v3" | "save_rows_v1" => Some(("storage", "load_ops")),
        "join_rows_v1" | "join_rows_v2" | "join_rows_v3" | "join_rows_v4" => {
            Some(("join", "operators.join"))
        }
        "aggregate_rows_v1" | "aggregate_rows_v2" | "aggregate_rows_v3" | "aggregate_rows_v4"
        | "quality_check_v1" | "quality_check_v2" | "quality_check_v3" | "quality_check_v4" => {
            Some(("analytics", "operators.analytics"))
        }
        "plugin_exec_v1" | "plugin_health_v1" | "plugin_registry_v1" | "plugin_operator_v1" => {
            Some(("integration", "plugin_runtime"))
        }
        "schema_registry_v1_register"
        | "schema_registry_v1_get"
        | "schema_registry_v1_infer"
        | "schema_registry_v2_register"
        | "schema_registry_v2_get"
        | "schema_registry_v2_infer"
        | "schema_registry_v2_check_compat"
        | "schema_registry_v2_suggest_migration" => Some(("schema", "schema_registry")),
        "udf_wasm_v1" | "udf_wasm_v2" => Some(("execution", "wasm_ops")),
        "time_series_v1"
        | "stats_v1"
        | "entity_linking_v1"
        | "table_reconstruct_v1"
        | "feature_store_v1_upsert"
        | "feature_store_v1_get"
        | "rule_simulator_v1"
        | "constraint_solver_v1"
        | "chart_data_prep_v1"
        | "diff_audit_v1" => Some(("analysis", "analysis_ops")),
        "lineage_v2" | "lineage_v3" | "workflow_run" => Some(("workflow", "operators.workflow")),
        "vector_index_v1_build"
        | "vector_index_v1_search"
        | "evidence_rank_v1"
        | "fact_crosscheck_v1"
        | "timeseries_forecast_v1"
        | "finance_ratio_v1"
        | "anomaly_explain_v1"
        | "template_bind_v1"
        | "provenance_sign_v1" => Some(("intelligence", "platform_ops.intelligence")),
        "stream_state_v1_save" | "stream_state_v1_load" | "query_lang_v1" => {
            Some(("platform", "platform_ops.state_query"))
        }
        "columnar_eval_v1" | "stream_window_v1" | "stream_window_v2" | "sketch_v1" => {
            Some(("platform", "platform_ops.streaming"))
        }
        "runtime_stats_v1" | "capabilities_v1" | "io_contract_v1" | "failure_policy_v1" => {
            Some(("governance", "governance.contracts"))
        }
        "incremental_plan_v1" | "explain_plan_v2" => Some(("governance", "governance.planning")),
        "tenant_isolation_v1" | "operator_policy_v1" | "optimizer_adaptive_v2" => {
            Some(("governance", "governance.tenant"))
        }
        "vector_index_v2_build"
        | "vector_index_v2_search"
        | "vector_index_v2_eval"
        | "stream_reliability_v1"
        | "lineage_provenance_v1"
        | "contract_regression_v1"
        | "perf_baseline_v1" => Some(("governance", "governance.reliability")),
        "parquet_io_v2" | "stream_state_v2" | "window_rows_v1" | "optimizer_v1"
        | "explain_plan_v1" => Some(("execution", "execution_ops")),
        _ => None,
    }
}

fn published_entry(
    operator: &'static str,
    streaming: bool,
    cache: bool,
    checkpoint: bool,
    io_contract: bool,
) -> OperatorMetadata {
    let (domain, catalog) =
        infer_domain_catalog(operator).unwrap_or_else(|| panic!("missing metadata for {operator}"));
    OperatorMetadata::new(operator, domain, catalog)
        .with_capabilities(streaming, cache, checkpoint, io_contract)
}
