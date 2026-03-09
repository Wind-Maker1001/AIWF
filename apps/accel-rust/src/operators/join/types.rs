use super::*;

pub(super) type IndexedRow = (usize, Map<String, Value>);
pub(super) type JoinIndex = HashMap<String, Vec<IndexedRow>>;

#[derive(Deserialize)]
pub(crate) struct JoinRowsReq {
    pub run_id: Option<String>,
    pub left_rows: Vec<Value>,
    pub right_rows: Vec<Value>,
    pub left_on: String,
    pub right_on: String,
    pub join_type: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct JoinRowsV2Req {
    pub run_id: Option<String>,
    pub left_rows: Vec<Value>,
    pub right_rows: Vec<Value>,
    pub left_on: Value,
    pub right_on: Value,
    pub join_type: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct JoinRowsV3Req {
    pub run_id: Option<String>,
    pub left_rows: Vec<Value>,
    pub right_rows: Vec<Value>,
    pub left_on: Value,
    pub right_on: Value,
    pub join_type: Option<String>,
    pub strategy: Option<String>,
    pub spill_path: Option<String>,
    pub chunk_size: Option<usize>,
}

#[derive(Serialize)]
pub(crate) struct JoinRowsResp {
    pub ok: bool,
    pub operator: String,
    pub status: String,
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub stats: Value,
}

#[derive(Deserialize)]
pub(crate) struct JoinRowsV4Req {
    pub run_id: Option<String>,
    pub left_rows: Vec<Value>,
    pub right_rows: Vec<Value>,
    pub left_on: Value,
    pub right_on: Value,
    pub join_type: Option<String>,
    pub strategy: Option<String>,
    pub spill_path: Option<String>,
    pub chunk_size: Option<usize>,
    pub enable_bloom: Option<bool>,
    pub bloom_field: Option<String>,
}
