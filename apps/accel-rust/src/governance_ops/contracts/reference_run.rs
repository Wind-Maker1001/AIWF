use crate::{
    api_types::{WorkflowDraftRunV1Req, WorkflowReferenceRunV1Req},
    operator_catalog::resolve_operator_metadata,
    operators::workflow::{WorkflowRunReq, run_workflow_with_state, workflow_step_operator_names},
};
use accel_rust::app_state::AppState;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

pub(crate) struct CompiledWorkflowReferencePlan {
    pub(crate) steps: Vec<Value>,
    pub(crate) operator_resolutions: Vec<Value>,
    pub(crate) ordered_node_ids: Vec<String>,
    pub(crate) last_step_id: String,
}

struct WorkflowCompileEnvelope {
    run_id: String,
    trace_id: String,
    tenant_id: String,
    job_id: String,
    published_version_id: String,
    version_id: String,
    job_context: Value,
    params_obj: Map<String, Value>,
    workflow_definition_source: &'static str,
    run_request_kind: &'static str,
}

fn compile_workflow_plan(
    envelope: &WorkflowCompileEnvelope,
    normalized_workflow_definition: &Value,
) -> Result<CompiledWorkflowReferencePlan, String> {
    let workflow = normalized_workflow_definition
        .as_object()
        .ok_or_else(|| "workflow_definition must be an object".to_string())?;
    let nodes = workflow
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| "workflow.nodes must be an array".to_string())?;
    let edges = workflow
        .get("edges")
        .and_then(Value::as_array)
        .ok_or_else(|| "workflow.edges must be an array".to_string())?;
    if nodes.is_empty() {
        return Err("workflow.nodes must contain at least one node".to_string());
    }

    let supported = workflow_step_operator_names()
        .into_iter()
        .map(|item| item.to_string())
        .collect::<HashSet<_>>();

    let mut node_map: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
    let mut indegree: HashMap<String, usize> = HashMap::new();
    let mut outgoing: HashMap<String, Vec<String>> = HashMap::new();
    let mut incoming: HashMap<String, Vec<String>> = HashMap::new();
    let mut conditions: HashMap<String, Vec<Value>> = HashMap::new();
    let mut operator_resolutions = Vec::new();

    for (index, node) in nodes.iter().enumerate() {
        let node_obj = node
            .as_object()
            .ok_or_else(|| format!("workflow.nodes[{index}] must be an object"))?;
        let node_id = node_obj
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if node_id.is_empty() {
            return Err(format!("workflow.nodes[{index}].id is required"));
        }
        if node_map.contains_key(node_id) {
            return Err(format!("duplicate node id: {node_id}"));
        }
        let node_type = node_obj
            .get("type")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if node_type.is_empty() {
            return Err(format!("workflow.nodes[{index}].type is required"));
        }
        if !supported.contains(node_type) {
            return Err(format!("unsupported workflow operator: {node_type}"));
        }
        let resolution = resolve_operator_metadata(node_type)
            .map(|entry| entry.to_workflow_resolution_metadata())
            .unwrap_or_else(|| json!({"operator": node_type}));
        operator_resolutions.push(json!({
            "node_id": node_id,
            "node_type": node_type,
            "resolution": resolution,
        }));
        indegree.insert(node_id.to_string(), 0);
        outgoing.insert(node_id.to_string(), Vec::new());
        incoming.insert(node_id.to_string(), Vec::new());
        conditions.insert(node_id.to_string(), Vec::new());
        node_map.insert(node_id.to_string(), node_obj.clone());
    }

    for (index, edge) in edges.iter().enumerate() {
        let edge_obj = edge
            .as_object()
            .ok_or_else(|| format!("workflow.edges[{index}] must be an object"))?;
        let from = edge_obj
            .get("from")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        let to = edge_obj
            .get("to")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if from.is_empty() {
            return Err(format!("workflow.edges[{index}].from is required"));
        }
        if to.is_empty() {
            return Err(format!("workflow.edges[{index}].to is required"));
        }
        if !node_map.contains_key(from) {
            return Err(format!("edge from does not exist: {from}"));
        }
        if !node_map.contains_key(to) {
            return Err(format!("edge to does not exist: {to}"));
        }
        if let Some(when) = edge_obj.get("when")
            && !when.is_null()
        {
            let when_obj = when.as_object().ok_or_else(|| format!("edge.when type unsupported: {from}->{to}"))?;
            let field = when_obj.get("field").and_then(Value::as_str).map(str::trim).unwrap_or("");
            let op = when_obj.get("op").and_then(Value::as_str).map(str::trim).unwrap_or("");
            if field.is_empty() || op != "eq" || !when_obj.contains_key("value") {
                return Err(format!("edge.when type unsupported: {from}->{to}"));
            }
            conditions.entry(to.to_string()).or_default().push(json!({
                "source_id": from,
                "field": field,
                "op": op,
                "value": when_obj.get("value").cloned().unwrap_or(Value::Null),
            }));
        }
        outgoing.entry(from.to_string()).or_default().push(to.to_string());
        incoming.entry(to.to_string()).or_default().push(from.to_string());
        *indegree.entry(to.to_string()).or_default() += 1;
    }

    let mut queue = indegree
        .iter()
        .filter_map(|(node_id, degree)| if *degree == 0 { Some(node_id.clone()) } else { None })
        .collect::<VecDeque<_>>();
    let mut ordered_node_ids = Vec::new();
    while let Some(node_id) = queue.pop_front() {
        ordered_node_ids.push(node_id.clone());
        for next in outgoing.get(&node_id).cloned().unwrap_or_default() {
            if let Some(degree) = indegree.get_mut(&next) {
                *degree -= 1;
                if *degree == 0 {
                    queue.push_back(next);
                }
            }
        }
    }
    if ordered_node_ids.len() != node_map.len() {
        return Err("workflow has cycle; only DAG is supported".to_string());
    }

    let steps = ordered_node_ids
        .iter()
        .map(|node_id| {
            let node_obj = node_map.get(node_id).expect("compiled node");
            let operator = node_obj
                .get("type")
                .and_then(Value::as_str)
                .map(str::trim)
                .unwrap_or("");
            let mut input = node_obj
                .get("config")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            if !envelope.trace_id.is_empty() && !input.contains_key("trace_id") {
                input.insert("trace_id".to_string(), json!(envelope.trace_id));
            }
            if !envelope.tenant_id.is_empty() && !input.contains_key("tenant_id") {
                input.insert("tenant_id".to_string(), json!(envelope.tenant_id));
            }
            if !envelope.run_id.is_empty() && !input.contains_key("run_id") {
                input.insert(
                    "run_id".to_string(),
                    json!(envelope.run_id),
                );
            }
            if !envelope.job_context.is_null() && !input.contains_key("job_context") {
                input.insert("job_context".to_string(), envelope.job_context.clone());
            }
            let mut params = envelope.params_obj.clone();
            if let Some(config) = node_obj.get("config").and_then(Value::as_object) {
                for (key, value) in config {
                    params.insert(key.clone(), value.clone());
                }
            }
            params.insert(
                "workflow_execution".to_string(),
                json!({
                    "version_id": envelope.version_id,
                    "published_version_id": envelope.published_version_id,
                    "workflow_definition_source": envelope.workflow_definition_source,
                    "run_request_kind": envelope.run_request_kind,
                }),
            );
            if operator == "cleaning" {
                input.insert("job_id".to_string(), json!(envelope.job_id));
                input.insert("step_id".to_string(), json!(node_id));
                if let Some(job_root) = envelope.job_context
                    .as_object()
                    .and_then(|ctx| ctx.get("job_root"))
                    .cloned()
                {
                    input.insert("job_root".to_string(), job_root);
                }
                input.insert("params".to_string(), Value::Object(params));
            } else if !params.is_empty() && !input.contains_key("params") {
                input.insert("params".to_string(), Value::Object(params));
            }
            let upstream = incoming
                .get(node_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|source_id| (source_id.clone(), json!({ "$context_ref": source_id })))
                .collect::<Map<_, _>>();
            if !upstream.is_empty() {
                input.insert("workflow_inputs".to_string(), Value::Object(upstream));
            }
            let node_conditions = conditions.get(node_id).cloned().unwrap_or_default();
            if !node_conditions.is_empty() {
                input.insert("workflow_conditions".to_string(), Value::Array(node_conditions));
            }
            json!({
                "id": node_id,
                "operator": operator,
                "input": Value::Object(input),
            })
        })
        .collect::<Vec<_>>();

    let last_step_id = ordered_node_ids.last().cloned().unwrap_or_default();
    Ok(CompiledWorkflowReferencePlan {
        steps,
        operator_resolutions,
        ordered_node_ids,
        last_step_id,
    })
}

pub(crate) fn run_workflow_reference_run_v1(
    state: &AppState,
    req: WorkflowReferenceRunV1Req,
    normalized_workflow_definition: Value,
) -> Result<Value, String> {
    let workflow_id = normalized_workflow_definition
        .get("workflow_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("")
        .to_string();
    let envelope = WorkflowCompileEnvelope {
        run_id: req.run_id.clone().unwrap_or_default(),
        trace_id: req.trace_id.clone().unwrap_or_default(),
        tenant_id: req.tenant_id.clone().unwrap_or_default(),
        job_id: req.job_id.clone().unwrap_or_default(),
        published_version_id: req
            .published_version_id
            .clone()
            .unwrap_or_else(|| req.version_id.clone().unwrap_or_default()),
        version_id: req.version_id.clone().unwrap_or_default(),
        job_context: req.job_context.clone().unwrap_or_else(|| json!({})),
        params_obj: req
            .params
            .clone()
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default(),
        workflow_definition_source: "version_reference",
        run_request_kind: "reference",
    };
    let compiled_plan = compile_workflow_plan(&envelope, &normalized_workflow_definition)?;
    let execution = run_workflow_with_state(
        state,
        WorkflowRunReq {
            run_id: req.run_id.clone(),
            trace_id: req.trace_id.clone(),
            traceparent: req.traceparent.clone(),
            tenant_id: req.tenant_id.clone(),
            context: Some(json!({
                "workflow_execution": {
                    "version_id": envelope.version_id,
                    "published_version_id": envelope.published_version_id,
                    "workflow_definition_source": "version_reference",
                    "run_request_kind": "reference",
                    "workflow_id": workflow_id,
                }
            })),
            steps: compiled_plan.steps.clone(),
        },
    )
    .map_err(|err| err.to_string())?;
    let execution_value =
        serde_json::to_value(&execution).map_err(|err| format!("serialize workflow execution: {err}"))?;
    let final_output = execution
        .context
        .as_object()
        .and_then(|ctx| ctx.get(&compiled_plan.last_step_id))
        .cloned()
        .unwrap_or(Value::Null);
    Ok(json!({
        "ok": execution.ok,
        "operator": "workflow_reference_run_v1",
        "status": execution.status,
        "workflow_id": workflow_id,
        "version_id": req.version_id.clone().unwrap_or_default(),
        "published_version_id": req.published_version_id.clone().unwrap_or_else(|| req.version_id.clone().unwrap_or_default()),
        "workflow_definition_source": "version_reference",
        "run_id": req.run_id,
        "trace_id": req.trace_id.unwrap_or_default(),
        "traceparent": req.traceparent,
        "tenant_id": req.tenant_id,
        "job_id": req.job_id,
        "compiled_plan": {
            "ordered_node_ids": compiled_plan.ordered_node_ids,
            "last_step_id": compiled_plan.last_step_id,
            "operator_resolutions": compiled_plan.operator_resolutions,
            "steps": compiled_plan.steps,
        },
        "execution": execution_value,
        "final_output": final_output,
    }))
}

pub(crate) fn run_workflow_draft_run_v1(
    state: &AppState,
    req: WorkflowDraftRunV1Req,
    normalized_workflow_definition: Value,
) -> Result<Value, String> {
    let workflow_id = normalized_workflow_definition
        .get("workflow_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("")
        .to_string();
    let envelope = WorkflowCompileEnvelope {
        run_id: req.run_id.clone().unwrap_or_default(),
        trace_id: req.trace_id.clone().unwrap_or_default(),
        tenant_id: req.tenant_id.clone().unwrap_or_default(),
        job_id: req.job_id.clone().unwrap_or_default(),
        published_version_id: String::new(),
        version_id: String::new(),
        job_context: req.job_context.clone().unwrap_or_else(|| json!({})),
        params_obj: req
            .params
            .clone()
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default(),
        workflow_definition_source: "draft_inline",
        run_request_kind: "draft",
    };
    let compiled_plan = compile_workflow_plan(&envelope, &normalized_workflow_definition)?;
    let execution = run_workflow_with_state(
        state,
        WorkflowRunReq {
            run_id: req.run_id.clone(),
            trace_id: req.trace_id.clone(),
            traceparent: req.traceparent.clone(),
            tenant_id: req.tenant_id.clone(),
            context: Some(json!({
                "workflow_execution": {
                    "workflow_definition_source": "draft_inline",
                    "run_request_kind": "draft",
                    "workflow_id": workflow_id,
                }
            })),
            steps: compiled_plan.steps.clone(),
        },
    )
    .map_err(|err| err.to_string())?;
    let execution_value =
        serde_json::to_value(&execution).map_err(|err| format!("serialize workflow execution: {err}"))?;
    let final_output = execution
        .context
        .as_object()
        .and_then(|ctx| ctx.get(&compiled_plan.last_step_id))
        .cloned()
        .unwrap_or(Value::Null);
    Ok(json!({
        "ok": execution.ok,
        "operator": "workflow_draft_run_v1",
        "status": execution.status,
        "workflow_id": workflow_id,
        "workflow_definition_source": "draft_inline",
        "run_id": req.run_id,
        "trace_id": req.trace_id.unwrap_or_default(),
        "traceparent": req.traceparent,
        "tenant_id": req.tenant_id,
        "job_id": req.job_id,
        "compiled_plan": {
            "ordered_node_ids": compiled_plan.ordered_node_ids,
            "last_step_id": compiled_plan.last_step_id,
            "operator_resolutions": compiled_plan.operator_resolutions,
            "steps": compiled_plan.steps,
        },
        "execution": execution_value,
        "final_output": final_output,
    }))
}
