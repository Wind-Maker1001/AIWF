use super::lineage::summarize_value;
use super::*;

pub(super) fn workflow_error(code: &str, operator: &str, message: impl Into<String>) -> String {
    AccelError::new(code, message)
        .with_operator(operator)
        .to_string()
}

pub(super) fn record_workflow_runtime_stat(
    run_id: Option<String>,
    op: &str,
    ok: bool,
    error: Option<&str>,
    duration_ms: u128,
) {
    let _ = run_runtime_stats_v1(RuntimeStatsV1Req {
        run_id,
        op: "record".to_string(),
        operator: Some(op.to_string()),
        ok: Some(ok),
        error_code: error.map(normalize_error_code),
        duration_ms: Some(duration_ms),
        rows_in: None,
        rows_out: None,
    });
}

pub(super) fn push_failed_workflow_step(
    trace: &mut Vec<WorkflowStepReplay>,
    id: &str,
    op: &str,
    resolution: Value,
    started_at: String,
    duration_ms: u128,
    input_summary: Value,
    error: String,
) {
    trace.push(WorkflowStepReplay {
        id: id.to_string(),
        operator: op.to_string(),
        resolution,
        status: "failed".to_string(),
        started_at,
        finished_at: utc_now_iso(),
        duration_ms,
        input_summary,
        output_summary: None,
        error: Some(error),
    });
}

pub(super) fn push_success_workflow_step(
    trace: &mut Vec<WorkflowStepReplay>,
    step_key: (&str, &str),
    resolution: Value,
    started_at: String,
    finished_at: String,
    duration_ms: u128,
    input_summary: Value,
    output: &Value,
) {
    let (id, op) = step_key;
    trace.push(WorkflowStepReplay {
        id: id.to_string(),
        operator: op.to_string(),
        resolution,
        status: "done".to_string(),
        started_at,
        finished_at,
        duration_ms,
        input_summary,
        output_summary: Some(summarize_value(output)),
        error: None,
    });
}

pub(super) fn parse_workflow_input<T: DeserializeOwned>(input: Value) -> Result<T, String> {
    serde_json::from_value::<T>(input).map_err(|e| e.to_string())
}

pub(super) fn serialize_workflow_output<T: Serialize>(output: T) -> Result<Value, String> {
    serde_json::to_value(output).map_err(|e| e.to_string())
}

pub(super) fn run_stateless_workflow_step<T, R, F>(input: Value, runner: F) -> Result<Value, String>
where
    T: DeserializeOwned,
    R: Serialize,
    F: FnOnce(T) -> Result<R, String>,
{
    let req = parse_workflow_input::<T>(input)?;
    let resp = runner(req)?;
    serialize_workflow_output(resp)
}

pub(super) fn run_stateful_workflow_step<T, R, F>(
    state: &AppState,
    input: Value,
    runner: F,
) -> Result<Value, String>
where
    T: DeserializeOwned,
    R: Serialize,
    F: FnOnce(&AppState, T) -> Result<R, String>,
{
    let req = parse_workflow_input::<T>(input)?;
    let resp = runner(state, req)?;
    serialize_workflow_output(resp)
}
