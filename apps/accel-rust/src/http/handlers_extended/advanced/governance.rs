use super::*;

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
