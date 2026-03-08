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
