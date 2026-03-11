use super::engine::{execute_workflow_step, workflow_resolution_metadata};
use super::lineage::summarize_value;
use super::support::{
    push_failed_workflow_step, push_success_workflow_step, record_workflow_runtime_stat,
    workflow_error,
};
use super::*;

#[cfg(test)]
fn workflow_local_state() -> AppState {
    AppState {
        service: "workflow_local".to_string(),
        tasks: Arc::new(Mutex::new(HashMap::new())),
        metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
        task_cfg: Arc::new(Mutex::new(task_store_config_from_env())),
        cancel_flags: Arc::new(Mutex::new(HashMap::new())),
        tenant_running: Arc::new(Mutex::new(HashMap::new())),
        idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        transform_cache: Arc::new(Mutex::new(HashMap::new())),
        schema_registry: Arc::new(Mutex::new(load_schema_registry_store())),
    }
}

#[cfg(test)]
pub(crate) fn run_workflow(req: WorkflowRunReq) -> Result<WorkflowRunResp, String> {
    let state = workflow_local_state();
    run_workflow_with_state(&state, req)
}

pub(crate) fn run_workflow_with_state(
    state: &AppState,
    req: WorkflowRunReq,
) -> Result<WorkflowRunResp, String> {
    let step_limit = tenant_max_workflow_steps_for(req.tenant_id.as_deref());
    if req.steps.len() > step_limit {
        return Err(workflow_error(
            "workflow_step_quota",
            "workflow_run",
            format!(
                "workflow step quota exceeded: {} > {}",
                req.steps.len(),
                step_limit
            ),
        ));
    }
    let trace_id = resolve_trace_id(
        req.trace_id.as_deref(),
        req.traceparent.as_deref(),
        &format!(
            "wf:{}:{}:{}",
            req.run_id.clone().unwrap_or_default(),
            req.tenant_id
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            req.steps.len()
        ),
    );
    let mut ctx = req.context.unwrap_or_else(|| json!({}));
    let mut trace: Vec<WorkflowStepReplay> = Vec::new();
    let mut failed_step: Option<String> = None;
    let mut failed_error: Option<String> = None;
    for step in &req.steps {
        let Some(obj) = step.as_object() else {
            return Err(workflow_error(
                "invalid_workflow_step",
                "workflow_run",
                "workflow step must be object",
            ));
        };
        let id = obj.get("id").and_then(|v| v.as_str()).unwrap_or("step");
        let op = obj.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        let input = obj.get("input").cloned().unwrap_or_else(|| json!({}));
        if !operator_allowed_for_tenant(op, req.tenant_id.as_deref()) {
            return Err(workflow_error(
                "operator_forbidden",
                "workflow_run",
                format!(
                    "operator_forbidden: tenant={} operator={}",
                    req.tenant_id.as_deref().unwrap_or("default"),
                    op
                ),
            ));
        }
        let started_at = utc_now_iso();
        let begin = Instant::now();
        let input_summary = summarize_value(&input);
        let resolution = workflow_resolution_metadata(op);
        let step_result = execute_workflow_step(state, op, input);
        let output = match step_result {
            Ok(v) => v,
            Err(err) => {
                let duration_ms = begin.elapsed().as_millis();
                record_workflow_runtime_stat(
                    req.run_id.clone(),
                    op,
                    false,
                    Some(&err),
                    duration_ms,
                );
                push_failed_workflow_step(
                    &mut trace,
                    id,
                    op,
                    resolution,
                    started_at,
                    duration_ms,
                    input_summary,
                    err.clone(),
                );
                failed_step = Some(id.to_string());
                failed_error = Some(err);
                break;
            }
        };
        let finished_at = utc_now_iso();
        if let Some(map) = ctx.as_object_mut()
            && failed_step.is_none()
        {
            map.insert(id.to_string(), output.clone());
        }
        let duration_ms = begin.elapsed().as_millis();
        push_success_workflow_step(
            &mut trace,
            (id, op),
            resolution,
            started_at,
            finished_at,
            duration_ms,
            input_summary,
            &output,
        );
        record_workflow_runtime_stat(req.run_id.clone(), op, true, None, duration_ms);
    }
    let status = if failed_step.is_some() {
        "failed"
    } else {
        "done"
    };
    Ok(WorkflowRunResp {
        ok: failed_step.is_none(),
        operator: "workflow_run".to_string(),
        status: status.to_string(),
        trace_id,
        run_id: req.run_id,
        context: ctx,
        steps: trace,
        failed_step,
        error: failed_error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_trace_includes_resolution_metadata_for_success_and_failure() {
        let success = run_workflow(WorkflowRunReq {
            run_id: Some("wf-meta-ok".to_string()),
            trace_id: None,
            traceparent: None,
            tenant_id: None,
            context: None,
            steps: vec![json!({
                "id": "caps",
                "operator": "capabilities_v1",
                "input": {}
            })],
        })
        .expect("successful workflow");
        assert_eq!(success.steps.len(), 1);
        assert_eq!(
            success.steps[0]
                .resolution
                .get("domain")
                .and_then(|v| v.as_str()),
            Some("governance")
        );
        assert_eq!(
            success.steps[0]
                .resolution
                .get("workflow")
                .and_then(|v| v.get("supported"))
                .and_then(|v| v.as_bool()),
            Some(true)
        );

        let failed = run_workflow(WorkflowRunReq {
            run_id: Some("wf-meta-failed".to_string()),
            trace_id: None,
            traceparent: None,
            tenant_id: None,
            context: None,
            steps: vec![json!({
                "id": "bad",
                "operator": "missing_operator",
                "input": {}
            })],
        })
        .expect("failed workflow response");
        assert_eq!(failed.steps.len(), 1);
        assert_eq!(
            failed.steps[0]
                .resolution
                .get("workflow")
                .and_then(|v| v.get("supported"))
                .and_then(|v| v.as_bool()),
            Some(false)
        );
    }
}
