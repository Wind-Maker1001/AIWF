use crate::operators::workflow::workflow_step_operator_names;
use serde_json::{Value, json};
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub(crate) struct OperatorMetadata {
    pub(crate) operator: String,
    pub(crate) version: Option<String>,
    pub(crate) domain: &'static str,
    pub(crate) catalog: &'static str,
    pub(crate) streaming: bool,
    pub(crate) cache: bool,
    pub(crate) checkpoint: bool,
    pub(crate) io_contract: bool,
}

impl OperatorMetadata {
    fn new(operator: impl Into<String>, domain: &'static str, catalog: &'static str) -> Self {
        let operator = operator.into();
        Self {
            version: infer_operator_version(&operator),
            operator,
            domain,
            catalog,
            streaming: false,
            cache: false,
            checkpoint: false,
            io_contract: false,
        }
    }

    fn with_capabilities(
        mut self,
        streaming: bool,
        cache: bool,
        checkpoint: bool,
        io_contract: bool,
    ) -> Self {
        self.streaming = streaming;
        self.cache = cache;
        self.checkpoint = checkpoint;
        self.io_contract = io_contract;
        self
    }

    pub(crate) fn to_capabilities_item(&self) -> Value {
        let mut out = json!({
            "operator": self.operator.clone(),
            "streaming": self.streaming,
            "cache": self.cache,
            "checkpoint": self.checkpoint,
            "io_contract": self.io_contract,
            "domain": self.domain,
            "catalog": self.catalog,
        });
        if let Some(version) = &self.version
            && let Some(obj) = out.as_object_mut()
        {
            obj.insert("version".to_string(), json!(version));
        }
        out
    }

    pub(crate) fn to_workflow_resolution_metadata(&self) -> Value {
        let mut out = json!({
            "operator": self.operator.clone(),
            "domain": self.domain,
            "catalog": self.catalog,
            "capabilities": {
                "streaming": self.streaming,
                "cache": self.cache,
                "checkpoint": self.checkpoint,
                "io_contract": self.io_contract,
            }
        });
        if let Some(version) = &self.version
            && let Some(obj) = out.as_object_mut()
        {
            obj.insert("version".to_string(), json!(version));
        }
        out
    }
}

pub(crate) fn capability_catalog_entries() -> Vec<OperatorMetadata> {
    published_operator_catalog().to_vec()
}

pub(crate) fn workflow_catalog_entries() -> Vec<OperatorMetadata> {
    let mut items = workflow_step_operator_names()
        .into_iter()
        .filter_map(resolve_operator_metadata)
        .collect::<Vec<_>>();
    items.sort_by(|a, b| a.operator.cmp(&b.operator));
    items
}

pub(crate) fn metadata_domain_summaries(entries: &[OperatorMetadata]) -> Vec<Value> {
    let mut grouped: std::collections::BTreeMap<&'static str, Vec<String>> =
        std::collections::BTreeMap::new();
    for entry in entries {
        grouped
            .entry(entry.domain)
            .or_default()
            .push(entry.operator.clone());
    }
    grouped
        .into_iter()
        .map(|(name, operators)| {
            json!({
                "name": name,
                "operator_count": operators.len(),
                "operators": operators,
            })
        })
        .collect()
}

pub(crate) fn resolve_operator_metadata(operator: &str) -> Option<OperatorMetadata> {
    let normalized = normalize_operator(operator);
    if normalized.is_empty() {
        return None;
    }
    if let Some(entry) = published_operator_catalog()
        .iter()
        .find(|entry| entry.operator == normalized)
    {
        return Some(entry.clone());
    }
    let (domain, catalog) = infer_domain_catalog(&normalized)?;
    Some(
        OperatorMetadata::new(normalized.clone(), domain, catalog).with_capabilities(
            infer_streaming(&normalized),
            infer_cache(&normalized),
            infer_checkpoint(&normalized),
            infer_io_contract(&normalized),
        ),
    )
}

fn published_operator_catalog() -> &'static [OperatorMetadata] {
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

fn published_entry(
    operator: &'static str,
    streaming: bool,
    cache: bool,
    checkpoint: bool,
    io_contract: bool,
) -> OperatorMetadata {
    let (domain, catalog) =
        infer_domain_catalog(operator).unwrap_or_else(|| panic!("missing metadata for {operator}"));
    OperatorMetadata::new(operator, domain, catalog).with_capabilities(
        streaming,
        cache,
        checkpoint,
        io_contract,
    )
}

fn normalize_operator(operator: &str) -> String {
    operator.trim().to_lowercase()
}

fn infer_operator_version(operator: &str) -> Option<String> {
    operator
        .split('_')
        .rev()
        .find_map(|part| part.strip_prefix('v'))
        .filter(|digits| !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit()))
        .map(|digits| format!("v{digits}"))
}

fn infer_streaming(operator: &str) -> bool {
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

fn infer_cache(operator: &str) -> bool {
    matches!(
        operator,
        "transform_rows_v2" | "transform_rows_v3" | "incremental_plan_v1" | "perf_baseline_v1"
    )
}

fn infer_checkpoint(operator: &str) -> bool {
    matches!(
        operator,
        "load_rows_v3"
            | "stream_window_v2"
            | "stream_state_v2"
            | "stream_reliability_v1"
            | "incremental_plan_v1"
    )
}

fn infer_io_contract(operator: &str) -> bool {
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

fn infer_domain_catalog(operator: &str) -> Option<(&'static str, &'static str)> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn workflow_catalog_entries_cover_registered_workflow_steps() {
        let workflow_ops = workflow_step_operator_names()
            .into_iter()
            .map(|item| item.to_string())
            .collect::<BTreeSet<_>>();
        let catalog_ops = workflow_catalog_entries()
            .into_iter()
            .map(|entry| entry.operator)
            .collect::<BTreeSet<_>>();
        assert_eq!(catalog_ops, workflow_ops);
    }

    #[test]
    fn domain_summaries_group_entries() {
        let entries = vec![
            resolve_operator_metadata("transform_rows_v2").expect("transform_rows_v2"),
            resolve_operator_metadata("transform_rows_v3").expect("transform_rows_v3"),
            resolve_operator_metadata("io_contract_v1").expect("io_contract_v1"),
        ];
        let domains = metadata_domain_summaries(&entries);
        assert!(domains.iter().any(|item| {
            item.get("name").and_then(|v| v.as_str()) == Some("transform")
                && item.get("operator_count").and_then(|v| v.as_u64()) == Some(2)
        }));
        assert!(domains.iter().any(|item| {
            item.get("name").and_then(|v| v.as_str()) == Some("governance")
        }));
    }
}
