use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
pub(crate) struct PluginOperatorV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) tenant_id: Option<String>,
    pub(crate) plugin: String,
    pub(crate) op: Option<String>,
    pub(crate) payload: Option<Value>,
}

#[derive(Deserialize)]
pub(crate) struct PluginExecReq {
    pub(crate) run_id: Option<String>,
    pub(crate) tenant_id: Option<String>,
    pub(crate) trace_id: Option<String>,
    pub(crate) plugin: String,
    pub(crate) input: Value,
}

#[derive(Serialize)]
pub(crate) struct PluginExecResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) run_id: Option<String>,
    pub(crate) trace_id: String,
    pub(crate) plugin: String,
    pub(crate) output: Value,
    pub(crate) stderr: String,
}

#[derive(Deserialize)]
pub(crate) struct PluginHealthReq {
    pub(crate) plugin: String,
    pub(crate) tenant_id: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct PluginRegistryV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) plugin: Option<String>,
    pub(crate) manifest: Option<Value>,
}

#[derive(Serialize)]
pub(crate) struct PluginHealthResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) plugin: String,
    pub(crate) details: Value,
}

#[derive(Deserialize, Clone)]
pub(crate) struct PluginManifest {
    pub(crate) name: Option<String>,
    pub(crate) version: Option<String>,
    pub(crate) api_version: Option<String>,
    pub(crate) command: String,
    pub(crate) args: Option<Vec<String>>,
    pub(crate) timeout_ms: Option<u64>,
    pub(crate) signature: Option<String>,
    pub(crate) healthcheck: Option<PluginHealthcheck>,
}

#[derive(Deserialize, Clone)]
pub(crate) struct PluginHealthcheck {
    pub(crate) command: Option<String>,
    pub(crate) args: Option<Vec<String>>,
    pub(crate) timeout_ms: Option<u64>,
}
