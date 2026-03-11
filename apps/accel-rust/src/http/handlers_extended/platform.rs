use crate::{
    api_types::{
        CapabilitiesV1Req, ColumnarEvalV1Req, ErrResp, FailurePolicyV1Req, IncrementalPlanV1Req,
        IoContractV1Req, OperatorPolicyV1Req, OptimizerAdaptiveV2Req, RuntimeStatsV1Req,
        SketchV1Req, StreamWindowV1Req, StreamWindowV2Req, TenantIsolationV1Req,
    },
    governance_ops::{
        run_capabilities_v1, run_failure_policy_v1, run_incremental_plan_v1, run_io_contract_v1,
        run_operator_policy_v1, run_optimizer_adaptive_v2, run_runtime_stats_v1,
        run_tenant_isolation_v1,
    },
    run_columnar_eval_v1, run_sketch_v1, run_stream_window_v1, run_stream_window_v2,
};
use accel_rust::app_state::AppState;
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};

pub(crate) async fn columnar_eval_v1_operator(
    Json(req): Json<ColumnarEvalV1Req>,
) -> impl IntoResponse {
    match run_columnar_eval_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "columnar_eval_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn stream_window_v1_operator(
    Json(req): Json<StreamWindowV1Req>,
) -> impl IntoResponse {
    match run_stream_window_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_window_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn stream_window_v2_operator(
    Json(req): Json<StreamWindowV2Req>,
) -> impl IntoResponse {
    match run_stream_window_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_window_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn sketch_v1_operator(Json(req): Json<SketchV1Req>) -> impl IntoResponse {
    match run_sketch_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "sketch_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn runtime_stats_v1_operator(
    Json(req): Json<RuntimeStatsV1Req>,
) -> impl IntoResponse {
    match run_runtime_stats_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "runtime_stats_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn capabilities_v1_operator(
    Json(req): Json<CapabilitiesV1Req>,
) -> impl IntoResponse {
    match run_capabilities_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "capabilities_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn capabilities_operator() -> impl IntoResponse {
    match run_capabilities_v1(CapabilitiesV1Req {
        run_id: None,
        include_ops: None,
    }) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "capabilities_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn io_contract_v1_operator(Json(req): Json<IoContractV1Req>) -> impl IntoResponse {
    match run_io_contract_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "io_contract_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn failure_policy_v1_operator(
    Json(req): Json<FailurePolicyV1Req>,
) -> impl IntoResponse {
    match run_failure_policy_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "failure_policy_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn incremental_plan_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<IncrementalPlanV1Req>,
) -> impl IntoResponse {
    match run_incremental_plan_v1(&state, req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "incremental_plan_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn tenant_isolation_v1_operator(
    Json(req): Json<TenantIsolationV1Req>,
) -> impl IntoResponse {
    match run_tenant_isolation_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "tenant_isolation_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn operator_policy_v1_operator(
    Json(req): Json<OperatorPolicyV1Req>,
) -> impl IntoResponse {
    match run_operator_policy_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "operator_policy_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn optimizer_adaptive_v2_operator(
    Json(req): Json<OptimizerAdaptiveV2Req>,
) -> impl IntoResponse {
    match run_optimizer_adaptive_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "optimizer_adaptive_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}
