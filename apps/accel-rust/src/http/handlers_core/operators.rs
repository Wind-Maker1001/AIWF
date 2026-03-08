use crate::*;

pub(crate) async fn join_rows_v1_operator(Json(req): Json<JoinRowsReq>) -> impl IntoResponse {
    match run_join_rows_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "join_rows_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn join_rows_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<JoinRowsV2Req>,
) -> impl IntoResponse {
    if let Ok(mut m) = state.metrics.lock() {
        m.join_rows_v2_calls += 1;
    }
    match run_join_rows_v2(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "join_rows_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn join_rows_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<JoinRowsV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.join_rows_v3_calls += 1;
    }
    match run_join_rows_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "join_rows_v3",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "join_rows_v3".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn rules_package_publish_v1_operator(
    Json(req): Json<RulesPackagePublishReq>,
) -> impl IntoResponse {
    match run_rules_package_publish_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "rules_package_publish_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn rules_package_get_v1_operator(
    Json(req): Json<RulesPackageGetReq>,
) -> impl IntoResponse {
    match run_rules_package_get_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "rules_package_get_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn normalize_schema_v1_operator(
    Json(req): Json<NormalizeSchemaReq>,
) -> impl IntoResponse {
    match run_normalize_schema_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "normalize_schema_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn entity_extract_v1_operator(
    Json(req): Json<EntityExtractReq>,
) -> impl IntoResponse {
    match run_entity_extract_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "entity_extract_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn aggregate_rows_v1_operator(
    Json(req): Json<AggregateRowsReq>,
) -> impl IntoResponse {
    match run_aggregate_rows_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "aggregate_rows_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn aggregate_rows_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<AggregateRowsV2Req>,
) -> impl IntoResponse {
    if let Ok(mut m) = state.metrics.lock() {
        m.aggregate_rows_v2_calls += 1;
    }
    match run_aggregate_rows_v2(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "aggregate_rows_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn aggregate_rows_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<AggregateRowsV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.aggregate_rows_v3_calls += 1;
    }
    match run_aggregate_rows_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "aggregate_rows_v3",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "aggregate_rows_v3".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn quality_check_v1_operator(
    Json(req): Json<QualityCheckReq>,
) -> impl IntoResponse {
    match run_quality_check_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "quality_check_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn quality_check_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<QualityCheckV2Req>,
) -> impl IntoResponse {
    if let Ok(mut m) = state.metrics.lock() {
        m.quality_check_v2_calls += 1;
    }
    match run_quality_check_v2(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "quality_check_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn quality_check_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<QualityCheckV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.quality_check_v3_calls += 1;
    }
    match run_quality_check_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "quality_check_v3",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "quality_check_v3".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn aggregate_pushdown_v1_operator(
    Json(req): Json<AggregatePushdownReq>,
) -> impl IntoResponse {
    match run_aggregate_pushdown_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "aggregate_pushdown_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}
