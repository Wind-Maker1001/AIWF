use crate::*;

pub(crate) async fn vector_index_build_v2_operator(
    Json(req): Json<VectorIndexBuildV2Req>,
) -> impl IntoResponse {
    match run_vector_index_build_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v2_build".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn vector_index_search_v2_operator(
    Json(req): Json<VectorIndexSearchV2Req>,
) -> impl IntoResponse {
    match run_vector_index_search_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v2_search".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn vector_index_eval_v2_operator(
    Json(req): Json<VectorIndexEvalV2Req>,
) -> impl IntoResponse {
    match run_vector_index_eval_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v2_eval".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn stream_reliability_v1_operator(
    Json(req): Json<StreamReliabilityV1Req>,
) -> impl IntoResponse {
    match run_stream_reliability_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_reliability_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn lineage_provenance_v1_operator(
    Json(req): Json<LineageProvenanceV1Req>,
) -> impl IntoResponse {
    match run_lineage_provenance_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "lineage_provenance_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn contract_regression_v1_operator(
    Json(req): Json<ContractRegressionV1Req>,
) -> impl IntoResponse {
    match run_contract_regression_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "contract_regression_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn perf_baseline_v1_operator(
    Json(req): Json<PerfBaselineV1Req>,
) -> impl IntoResponse {
    match run_perf_baseline_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "perf_baseline_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn window_rows_v1_operator(Json(req): Json<WindowRowsV1Req>) -> impl IntoResponse {
    match run_window_rows_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "window_rows_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn optimizer_v1_operator(Json(req): Json<OptimizerV1Req>) -> impl IntoResponse {
    match run_optimizer_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "optimizer_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn join_rows_v4_operator(Json(req): Json<JoinRowsV4Req>) -> impl IntoResponse {
    match run_join_rows_v4(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "join_rows_v4".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn aggregate_rows_v4_operator(
    Json(req): Json<AggregateRowsV4Req>,
) -> impl IntoResponse {
    match run_aggregate_rows_v4(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "aggregate_rows_v4".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn quality_check_v4_operator(
    Json(req): Json<QualityCheckV4Req>,
) -> impl IntoResponse {
    match run_quality_check_v4(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "quality_check_v4".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn lineage_v3_operator(Json(req): Json<LineageV3Req>) -> impl IntoResponse {
    match run_lineage_v3(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "lineage_v3".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn parquet_io_v2_operator(Json(req): Json<ParquetIoV2Req>) -> impl IntoResponse {
    match run_parquet_io_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "parquet_io_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn stream_state_v2_operator(
    Json(req): Json<StreamStateV2Req>,
) -> impl IntoResponse {
    match run_stream_state_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_state_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn udf_wasm_v2_operator(Json(req): Json<UdfWasmV2Req>) -> impl IntoResponse {
    match run_udf_wasm_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "udf_wasm_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn explain_plan_v1_operator(
    Json(req): Json<ExplainPlanV1Req>,
) -> impl IntoResponse {
    match run_explain_plan_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "explain_plan_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn explain_plan_v2_operator(
    Json(req): Json<ExplainPlanV2Req>,
) -> impl IntoResponse {
    match run_explain_plan_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "explain_plan_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn save_rows_v1_operator(Json(req): Json<SaveRowsReq>) -> impl IntoResponse {
    match run_save_rows_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "save_rows_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn transform_rows_v2_stream_operator(
    State(state): State<AppState>,
    Json(req): Json<TransformRowsStreamReq>,
) -> impl IntoResponse {
    if let Some(rows) = req.rows.as_ref() {
        let bytes = serde_json::to_vec(rows).map(|v| v.len()).unwrap_or(0);
        if let Err(e) =
            enforce_tenant_payload_quota(Some(&state), req.tenant_id.as_deref(), rows.len(), bytes)
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(ErrResp {
                    ok: false,
                    operator: "transform_rows_v2_stream".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response();
        }
    }
    match run_transform_rows_v2_stream(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "transform_rows_v2_stream".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn workflow_run_operator(
    State(state): State<AppState>,
    Json(req): Json<WorkflowRunReq>,
) -> impl IntoResponse {
    let step_limit = tenant_max_workflow_steps_for(req.tenant_id.as_deref());
    if req.steps.len() > step_limit {
        if let Ok(mut m) = state.metrics.lock() {
            m.quota_reject_total += 1;
        }
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrResp {
                ok: false,
                operator: "workflow_run".to_string(),
                status: "failed".to_string(),
                error: format!(
                    "workflow step quota exceeded: {} > {}",
                    req.steps.len(),
                    step_limit
                ),
            }),
        )
            .into_response();
    }
    match crate::operators::workflow::run_workflow_with_state(&state, req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "workflow_run".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}
