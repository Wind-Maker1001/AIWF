use super::*;

#[derive(Deserialize)]
pub(crate) struct StreamWindowV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) stream_key: String,
    pub(crate) rows: Vec<Value>,
    pub(crate) event_time_field: String,
    pub(crate) window_ms: u64,
    pub(crate) watermark_ms: Option<u64>,
    pub(crate) group_by: Option<Vec<String>>,
    pub(crate) value_field: Option<String>,
    pub(crate) trigger: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct StreamWindowV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) stream_key: String,
    pub(crate) rows: Vec<Value>,
    pub(crate) event_time_field: String,
    pub(crate) window_type: Option<String>,
    pub(crate) window_ms: u64,
    pub(crate) slide_ms: Option<u64>,
    pub(crate) session_gap_ms: Option<u64>,
    pub(crate) watermark_ms: Option<u64>,
    pub(crate) allowed_lateness_ms: Option<u64>,
    pub(crate) group_by: Option<Vec<String>>,
    pub(crate) value_field: Option<String>,
    pub(crate) trigger: Option<String>,
    pub(crate) emit_late_side: Option<bool>,
}

#[derive(Deserialize)]
pub(crate) struct ColumnarEvalV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) select_fields: Option<Vec<String>>,
    pub(crate) filter_eq: Option<Value>,
    pub(crate) limit: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct SketchV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) kind: Option<String>,
    pub(crate) state: Option<Value>,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) field: Option<String>,
    pub(crate) topk_n: Option<usize>,
    pub(crate) merge_state: Option<Value>,
}

#[derive(Deserialize)]
pub(crate) struct RuntimeStatsV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) operator: Option<String>,
    pub(crate) ok: Option<bool>,
    pub(crate) error_code: Option<String>,
    pub(crate) duration_ms: Option<u128>,
    pub(crate) rows_in: Option<usize>,
    pub(crate) rows_out: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct AggregatePushdownReq {
    pub(crate) run_id: Option<String>,
    pub(crate) source_type: String,
    pub(crate) source: String,
    pub(crate) from: Option<String>,
    pub(crate) group_by: Vec<String>,
    pub(crate) aggregates: Vec<Value>,
    pub(crate) where_sql: Option<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Serialize)]
pub(crate) struct AggregatePushdownResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) run_id: Option<String>,
    pub(crate) sql: String,
    pub(crate) rows: Vec<Value>,
    pub(crate) stats: Value,
}
