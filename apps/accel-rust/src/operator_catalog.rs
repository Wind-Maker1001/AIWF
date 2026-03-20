use crate::operators::workflow::workflow_step_operator_names;
use serde_json::{Value, json};

#[path = "operator_catalog_data.rs"]
mod data;

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
            version: data::infer_operator_version(&operator),
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
    data::published_operator_catalog().to_vec()
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
    let normalized = data::normalize_operator(operator);
    if normalized.is_empty() {
        return None;
    }
    if let Some(entry) = data::published_operator_catalog()
        .iter()
        .find(|entry| entry.operator == normalized)
    {
        return Some(entry.clone());
    }
    let (domain, catalog) = data::infer_domain_catalog(&normalized)?;
    Some(
        OperatorMetadata::new(normalized.clone(), domain, catalog).with_capabilities(
            data::infer_streaming(&normalized),
            data::infer_cache(&normalized),
            data::infer_checkpoint(&normalized),
            data::infer_io_contract(&normalized),
        ),
    )
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
