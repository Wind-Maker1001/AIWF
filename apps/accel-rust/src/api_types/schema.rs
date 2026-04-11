use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
pub(crate) struct SchemaRegisterReq {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) schema: Value,
}

#[derive(Deserialize)]
pub(crate) struct SchemaGetReq {
    pub(crate) name: String,
    pub(crate) version: String,
}

#[derive(Deserialize)]
pub(crate) struct SchemaInferReq {
    pub(crate) name: Option<String>,
    pub(crate) version: Option<String>,
    pub(crate) rows: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct SchemaCompatReq {
    pub(crate) name: String,
    pub(crate) from_version: String,
    pub(crate) to_version: String,
    pub(crate) mode: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct SchemaCompatResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) compatible: bool,
    pub(crate) mode: String,
    pub(crate) breaking_fields: Vec<String>,
    pub(crate) widening_fields: Vec<String>,
}

#[derive(Deserialize)]
pub(crate) struct SchemaMigrationSuggestReq {
    pub(crate) name: String,
    pub(crate) from_version: String,
    pub(crate) to_version: String,
}

#[derive(Serialize)]
pub(crate) struct SchemaMigrationSuggestResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) steps: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct WorkflowContractV1Req {
    pub(crate) workflow_definition: Value,
    pub(crate) allow_version_migration: Option<bool>,
    pub(crate) require_non_empty_nodes: Option<bool>,
    pub(crate) validation_scope: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct WorkflowContractV1Resp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) schema_version: String,
    pub(crate) graph_contract: String,
    pub(crate) error_item_contract: String,
    pub(crate) validation_scope: String,
    pub(crate) valid: bool,
    pub(crate) normalized_workflow_definition: Value,
    pub(crate) error_items: Vec<Value>,
    pub(crate) notes: Vec<String>,
    pub(crate) node_type_inventory: Value,
    pub(crate) operator_resolutions: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct WorkflowReferenceRunV1Req {
    pub(crate) workflow_definition: Value,
    pub(crate) version_id: Option<String>,
    pub(crate) published_version_id: Option<String>,
    pub(crate) job_id: Option<String>,
    #[allow(dead_code)]
    pub(crate) actor: Option<String>,
    #[allow(dead_code)]
    pub(crate) ruleset_version: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) trace_id: Option<String>,
    pub(crate) traceparent: Option<String>,
    pub(crate) tenant_id: Option<String>,
    pub(crate) job_context: Option<Value>,
    pub(crate) params: Option<Value>,
}

#[derive(Deserialize)]
pub(crate) struct WorkflowDraftRunV1Req {
    pub(crate) workflow_definition: Value,
    pub(crate) job_id: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) trace_id: Option<String>,
    pub(crate) traceparent: Option<String>,
    pub(crate) tenant_id: Option<String>,
    pub(crate) job_context: Option<Value>,
    pub(crate) params: Option<Value>,
}

#[derive(Serialize)]
pub(crate) struct SchemaRegistryResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) schema: Value,
}

#[derive(Serialize)]
pub(crate) struct SchemaInferResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) name: Option<String>,
    pub(crate) version: Option<String>,
    pub(crate) schema: Value,
    pub(crate) stats: Value,
}
