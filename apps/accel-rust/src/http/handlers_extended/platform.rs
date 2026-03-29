use crate::{
    api_types::{
        CapabilitiesV1Req, ColumnarEvalV1Req, FailurePolicyV1Req, IncrementalPlanV1Req,
        IoContractV1Req, OperatorPolicyV1Req, OptimizerAdaptiveV2Req, RuntimeStatsV1Req,
        SketchV1Req, StreamWindowV1Req, StreamWindowV2Req, TenantIsolationV1Req,
        WorkflowContractV1Req, WorkflowDraftRunV1Req, WorkflowReferenceRunV1Req,
    },
    governance_ops::{
        run_capabilities_v1, run_failure_policy_v1, run_incremental_plan_v1, run_io_contract_v1,
        run_operator_policy_v1, run_optimizer_adaptive_v2, run_runtime_stats_v1,
        run_tenant_isolation_v1, run_workflow_contract_v1, run_workflow_draft_run_v1,
        run_workflow_reference_run_v1,
    },
    run_columnar_eval_v1, run_sketch_v1, run_stream_window_v1, run_stream_window_v2,
};
use accel_rust::app_state::AppState;
use axum::{Json, extract::State, response::IntoResponse};

#[path = "platform_support.rs"]
mod support;

pub(crate) async fn columnar_eval_v1_operator(
    Json(req): Json<ColumnarEvalV1Req>,
) -> impl IntoResponse {
    match run_columnar_eval_v1(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("columnar_eval_v1", e),
    }
}

pub(crate) async fn stream_window_v1_operator(
    Json(req): Json<StreamWindowV1Req>,
) -> impl IntoResponse {
    match run_stream_window_v1(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("stream_window_v1", e),
    }
}

pub(crate) async fn stream_window_v2_operator(
    Json(req): Json<StreamWindowV2Req>,
) -> impl IntoResponse {
    match run_stream_window_v2(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("stream_window_v2", e),
    }
}

pub(crate) async fn sketch_v1_operator(Json(req): Json<SketchV1Req>) -> impl IntoResponse {
    match run_sketch_v1(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("sketch_v1", e),
    }
}

pub(crate) async fn runtime_stats_v1_operator(
    Json(req): Json<RuntimeStatsV1Req>,
) -> impl IntoResponse {
    match run_runtime_stats_v1(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("runtime_stats_v1", e),
    }
}

pub(crate) async fn capabilities_v1_operator(
    Json(req): Json<CapabilitiesV1Req>,
) -> impl IntoResponse {
    match run_capabilities_v1(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("capabilities_v1", e),
    }
}

pub(crate) async fn capabilities_operator() -> impl IntoResponse {
    match run_capabilities_v1(CapabilitiesV1Req {
        run_id: None,
        include_ops: None,
    }) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("capabilities_v1", e),
    }
}

pub(crate) async fn io_contract_v1_operator(Json(req): Json<IoContractV1Req>) -> impl IntoResponse {
    match run_io_contract_v1(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("io_contract_v1", e),
    }
}

pub(crate) async fn workflow_contract_v1_operator(
    Json(req): Json<WorkflowContractV1Req>,
) -> impl IntoResponse {
    match run_workflow_contract_v1(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("workflow_contract_v1", e),
    }
}

pub(crate) async fn workflow_reference_run_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<WorkflowReferenceRunV1Req>,
) -> impl IntoResponse {
    match run_workflow_contract_v1(WorkflowContractV1Req {
        workflow_definition: req.workflow_definition.clone(),
        allow_version_migration: Some(false),
        require_non_empty_nodes: Some(true),
        validation_scope: Some("run".to_string()),
    }) {
        Ok(v) if !v.valid => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "operator": "workflow_reference_run_v1",
                "status": "invalid",
                "graph_contract": v.graph_contract,
                "error_item_contract": v.error_item_contract,
                "error_items": v.error_items,
                "notes": v.notes,
            })),
        ).into_response(),
        Ok(v) => match run_workflow_reference_run_v1(&state, req, v.normalized_workflow_definition) {
            Ok(payload) => support::ok_json(payload),
            Err(e) => support::bad_request("workflow_reference_run_v1", e),
        },
        Err(e) => support::bad_request("workflow_reference_run_v1", e),
    }
}

pub(crate) async fn workflow_draft_run_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<WorkflowDraftRunV1Req>,
) -> impl IntoResponse {
    match run_workflow_contract_v1(WorkflowContractV1Req {
        workflow_definition: req.workflow_definition.clone(),
        allow_version_migration: Some(false),
        require_non_empty_nodes: Some(true),
        validation_scope: Some("run".to_string()),
    }) {
        Ok(v) if !v.valid => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "operator": "workflow_draft_run_v1",
                "status": "invalid",
                "graph_contract": v.graph_contract,
                "error_item_contract": v.error_item_contract,
                "error_items": v.error_items,
                "notes": v.notes,
            })),
        ).into_response(),
        Ok(v) => match run_workflow_draft_run_v1(&state, req, v.normalized_workflow_definition) {
            Ok(payload) => support::ok_json(payload),
            Err(e) => support::bad_request("workflow_draft_run_v1", e),
        },
        Err(e) => support::bad_request("workflow_draft_run_v1", e),
    }
}

pub(crate) async fn failure_policy_v1_operator(
    Json(req): Json<FailurePolicyV1Req>,
) -> impl IntoResponse {
    match run_failure_policy_v1(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("failure_policy_v1", e),
    }
}

pub(crate) async fn incremental_plan_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<IncrementalPlanV1Req>,
) -> impl IntoResponse {
    match run_incremental_plan_v1(&state, req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("incremental_plan_v1", e),
    }
}

pub(crate) async fn tenant_isolation_v1_operator(
    Json(req): Json<TenantIsolationV1Req>,
) -> impl IntoResponse {
    match run_tenant_isolation_v1(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("tenant_isolation_v1", e),
    }
}

pub(crate) async fn operator_policy_v1_operator(
    Json(req): Json<OperatorPolicyV1Req>,
) -> impl IntoResponse {
    match run_operator_policy_v1(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("operator_policy_v1", e),
    }
}

pub(crate) async fn optimizer_adaptive_v2_operator(
    Json(req): Json<OptimizerAdaptiveV2Req>,
) -> impl IntoResponse {
    match run_optimizer_adaptive_v2(req) {
        Ok(v) => support::ok_json(v),
        Err(e) => support::bad_request("optimizer_adaptive_v2", e),
    }
}
