use super::*;

pub(crate) async fn vector_index_build_v1_operator(
    Json(req): Json<VectorIndexBuildReq>,
) -> impl IntoResponse {
    match run_vector_index_build_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v1_build".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn vector_index_search_v1_operator(
    Json(req): Json<VectorIndexSearchReq>,
) -> impl IntoResponse {
    match run_vector_index_search_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v1_search".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn evidence_rank_v1_operator(
    Json(req): Json<EvidenceRankReq>,
) -> impl IntoResponse {
    match run_evidence_rank_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "evidence_rank_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn fact_crosscheck_v1_operator(
    Json(req): Json<FactCrosscheckReq>,
) -> impl IntoResponse {
    match run_fact_crosscheck_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "fact_crosscheck_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn timeseries_forecast_v1_operator(
    Json(req): Json<TimeSeriesForecastReq>,
) -> impl IntoResponse {
    match run_timeseries_forecast_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "timeseries_forecast_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn finance_ratio_v1_operator(
    Json(req): Json<FinanceRatioReq>,
) -> impl IntoResponse {
    match run_finance_ratio_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "finance_ratio_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn anomaly_explain_v1_operator(
    Json(req): Json<AnomalyExplainReq>,
) -> impl IntoResponse {
    match run_anomaly_explain_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "anomaly_explain_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn template_bind_v1_operator(
    Json(req): Json<TemplateBindReq>,
) -> impl IntoResponse {
    match run_template_bind_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "template_bind_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn provenance_sign_v1_operator(
    Json(req): Json<ProvenanceSignReq>,
) -> impl IntoResponse {
    match run_provenance_sign_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "provenance_sign_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}
