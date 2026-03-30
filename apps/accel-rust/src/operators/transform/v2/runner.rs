use super::*;
use crate::api_types::CompiledFilter;
use std::time::Instant;

struct TransformPrepared {
    rows_in: Vec<Value>,
    input_rows: usize,
    estimated_bytes: usize,
    max_rows: usize,
    max_bytes: usize,
    rules: Value,
    gates: Value,
    null_values: Vec<String>,
    trim_strings: bool,
    rename_map: HashMap<String, String>,
    casts: HashMap<String, String>,
    compiled_filters: Vec<CompiledFilter>,
    required_fields: Vec<String>,
    default_values: Map<String, Value>,
    include_fields: Vec<String>,
    exclude_fields: Vec<String>,
    deduplicate_by: Vec<String>,
    dedup_keep: String,
    sort_by: Vec<Value>,
    requested_engine: String,
    engine: String,
    engine_reason: String,
    use_columnar: bool,
}

struct TransformExecution {
    rows: Vec<Map<String, Value>>,
    invalid_rows: usize,
    filtered_rows: usize,
    duplicate_rows_removed: usize,
    numeric_cells_total: usize,
    numeric_cells_parsed: usize,
    date_cells_total: usize,
    date_cells_parsed: usize,
    rule_hits: HashMap<String, usize>,
}

#[path = "runner/execute.rs"]
mod execute;
#[path = "runner/finalize.rs"]
mod finalize;
#[path = "runner/prepare.rs"]
mod prepare;

pub(crate) fn run_transform_rows_v2(req: TransformRowsReq) -> Result<TransformRowsResp, String> {
    run_transform_rows_v2_with_cancel(req, None)
}

pub(crate) fn run_transform_rows_v2_with_cancel(
    req: TransformRowsReq,
    cancel_flag: Option<Arc<AtomicBool>>,
) -> Result<TransformRowsResp, String> {
    let started = Instant::now();
    verify_request_signature(&req)?;
    let prepared = prepare::prepare_transform_request(&req)?;
    let executed = execute::execute_transform_rows(&prepared, &cancel_flag)?;
    finalize::finalize_transform_response(req, prepared, executed, started)
}
