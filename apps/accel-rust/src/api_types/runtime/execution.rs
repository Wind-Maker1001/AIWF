use super::*;

#[derive(Deserialize)]
pub(crate) struct WindowRowsV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) partition_by: Option<Vec<String>>,
    pub(crate) order_by: String,
    pub(crate) functions: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct OptimizerV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) row_count_hint: Option<usize>,
    pub(crate) prefer_arrow: Option<bool>,
    pub(crate) join_hint: Option<Value>,
    pub(crate) aggregate_hint: Option<Value>,
}

#[derive(Deserialize)]
pub(crate) struct ParquetIoV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) path: String,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) parquet_mode: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) columns: Option<Vec<String>>,
    pub(crate) predicate_field: Option<String>,
    pub(crate) predicate_eq: Option<Value>,
    pub(crate) partition_by: Option<Vec<String>>,
    pub(crate) compression: Option<String>,
    pub(crate) recursive: Option<bool>,
    pub(crate) schema_mode: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct StreamStateV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) stream_key: String,
    pub(crate) state: Option<Value>,
    pub(crate) offset: Option<u64>,
    pub(crate) checkpoint_version: Option<u64>,
    pub(crate) expected_version: Option<u64>,
    pub(crate) backend: Option<String>,
    pub(crate) db_path: Option<String>,
    pub(crate) event_ts_ms: Option<i64>,
    pub(crate) max_late_ms: Option<u64>,
}

#[derive(Deserialize)]
pub(crate) struct UdfWasmV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) field: String,
    pub(crate) output_field: String,
    pub(crate) op: Option<String>,
    pub(crate) wasm_base64: Option<String>,
    pub(crate) max_output_bytes: Option<usize>,
    pub(crate) signed_token: Option<String>,
    pub(crate) allowed_ops: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub(crate) struct ExplainPlanV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) steps: Vec<Value>,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) actual_stats: Option<Vec<Value>>,
    pub(crate) persist_feedback: Option<bool>,
}

#[derive(Deserialize)]
pub(crate) struct ExplainPlanV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) steps: Vec<Value>,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) actual_stats: Option<Vec<Value>>,
    pub(crate) persist_feedback: Option<bool>,
    pub(crate) include_runtime_stats: Option<bool>,
}
