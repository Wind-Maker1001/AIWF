use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
pub(crate) struct NormalizeSchemaReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) schema: Value,
}

#[derive(Serialize)]
pub(crate) struct NormalizeSchemaResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) stats: Value,
}

#[derive(Deserialize)]
pub(crate) struct EntityExtractReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) text: Option<String>,
    pub(crate) text_field: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct EntityExtractResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) run_id: Option<String>,
    pub(crate) entities: Value,
}

#[derive(Deserialize)]
pub(crate) struct RulesCompileReq {
    pub(crate) dsl: String,
}

#[derive(Deserialize)]
pub(crate) struct RulesPackagePublishReq {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) dsl: Option<String>,
    pub(crate) rules: Option<Value>,
}

#[derive(Deserialize)]
pub(crate) struct RulesPackageGetReq {
    pub(crate) name: String,
    pub(crate) version: String,
}

#[derive(Serialize)]
pub(crate) struct RulesCompileResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) rules: Value,
}

#[derive(Serialize)]
pub(crate) struct RulesPackageResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) rules: Value,
    pub(crate) fingerprint: String,
}

#[derive(Deserialize)]
pub(crate) struct LoadRowsReq {
    pub(crate) source_type: String,
    pub(crate) source: String,
    pub(crate) query: Option<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Serialize)]
pub(crate) struct LoadRowsResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) rows: Vec<Value>,
    pub(crate) stats: Value,
}

#[derive(Deserialize)]
pub(crate) struct SaveRowsReq {
    pub(crate) sink_type: String,
    pub(crate) sink: String,
    pub(crate) table: Option<String>,
    pub(crate) parquet_mode: Option<String>,
    pub(crate) rows: Vec<Value>,
}

#[derive(Serialize)]
pub(crate) struct SaveRowsResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) written_rows: usize,
}

#[derive(Deserialize)]
pub(crate) struct TransformRowsStreamReq {
    pub(crate) run_id: Option<String>,
    pub(crate) tenant_id: Option<String>,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) input_uri: Option<String>,
    pub(crate) output_uri: Option<String>,
    pub(crate) chunk_size: Option<usize>,
    pub(crate) rules: Option<Value>,
    pub(crate) rules_dsl: Option<String>,
    pub(crate) quality_gates: Option<Value>,
    pub(crate) checkpoint_key: Option<String>,
    pub(crate) resume: Option<bool>,
    pub(crate) watermark_field: Option<String>,
    pub(crate) watermark_value: Option<Value>,
    pub(crate) max_chunks_per_run: Option<usize>,
}

#[derive(Serialize)]
pub(crate) struct TransformRowsStreamResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) chunks: usize,
    pub(crate) has_more: bool,
    pub(crate) next_checkpoint: Option<usize>,
    pub(crate) stats: Value,
}

#[derive(Deserialize)]
pub(crate) struct LoadRowsV2Req {
    pub(crate) source_type: String,
    pub(crate) source: String,
    pub(crate) query: Option<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct LoadRowsV3Req {
    pub(crate) source_type: String,
    pub(crate) source: String,
    pub(crate) query: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) max_retries: Option<usize>,
    pub(crate) retry_backoff_ms: Option<u64>,
    pub(crate) resume_token: Option<String>,
    pub(crate) connector_options: Option<Value>,
}
