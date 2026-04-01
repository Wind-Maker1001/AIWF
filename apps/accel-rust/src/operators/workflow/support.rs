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

pub(super) fn prepare_workflow_step_input(input: Value) -> Value {
    apply_workflow_input_map(input)
}

pub(super) fn parse_workflow_input<T: DeserializeOwned>(input: Value) -> Result<T, String> {
    serde_json::from_value::<T>(prepare_workflow_step_input(input)).map_err(|e| e.to_string())
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

fn apply_workflow_input_map(input: Value) -> Value {
    let Value::Object(mut root) = input else {
        return input;
    };
    let Some(input_map) = root.get("input_map").and_then(Value::as_object).cloned() else {
        return Value::Object(root);
    };
    let workflow_inputs = root
        .get("workflow_inputs")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let predecessor_ids = workflow_inputs.keys().cloned().collect::<Vec<_>>();
    for (target_key, expr) in input_map {
        if let Some(value) = resolve_workflow_input_map_value(&expr, &workflow_inputs, &predecessor_ids) {
            root.insert(target_key, value);
        }
    }
    Value::Object(root)
}

fn resolve_workflow_input_map_value(
    expr: &Value,
    workflow_inputs: &serde_json::Map<String, Value>,
    predecessor_ids: &[String],
) -> Option<Value> {
    if let Some(obj) = expr.as_object() {
        let from = obj
            .get("from")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| predecessor_ids.first().map(String::as_str).unwrap_or(""));
        let path = obj
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        return workflow_inputs
            .get(from)
            .and_then(|value| pick_workflow_value_path(value, path))
            .cloned();
    }

    let raw = expr.as_str().map(str::trim).unwrap_or("");
    if raw.is_empty() {
        return None;
    }
    if let Some(path) = raw.strip_prefix("$prev.") {
        for predecessor_id in predecessor_ids {
            if let Some(value) = workflow_inputs
                .get(predecessor_id)
                .and_then(|output| pick_workflow_value_path(output, path))
            {
                return Some(value.clone());
            }
        }
        return None;
    }
    if let Some((node_id, path)) = raw.split_once('.')
        && let Some(value) = workflow_inputs
            .get(node_id)
            .and_then(|output| pick_workflow_value_path(output, path))
    {
        return Some(value.clone());
    }
    for predecessor_id in predecessor_ids {
        if let Some(value) = workflow_inputs
            .get(predecessor_id)
            .and_then(|output| pick_workflow_value_path(output, raw))
        {
            return Some(value.clone());
        }
    }
    None
}

fn pick_workflow_value_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Some(value);
    }
    let mut current = value;
    for segment in trimmed.split('.') {
        let obj = current.as_object()?;
        current = obj.get(segment)?;
    }
    Some(current)
}

#[cfg(test)]
mod input_map_tests {
    use super::*;

    #[test]
    fn parse_workflow_input_applies_input_map_from_predecessors() {
        #[derive(Deserialize)]
        struct DummyReq {
            rows: Vec<Value>,
        }

        let payload = json!({
            "rows": [],
            "input_map": { "rows": "$prev.rows" },
            "workflow_inputs": {
                "load_a": {
                    "rows": [{ "id": 1 }]
                }
            }
        });

        let parsed = parse_workflow_input::<DummyReq>(payload).expect("parse req");
        assert_eq!(parsed.rows.len(), 1);
    }
}
