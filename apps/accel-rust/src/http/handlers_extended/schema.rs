use crate::*;

pub(crate) async fn schema_registry_register_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<SchemaRegisterReq>,
) -> impl IntoResponse {
    if let Ok(mut m) = state.metrics.lock() {
        m.schema_registry_register_total += 1;
    }
    match run_schema_registry_register_v1(&state, req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "schema_registry_v1_register".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn schema_registry_get_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<SchemaGetReq>,
) -> impl IntoResponse {
    if let Ok(mut m) = state.metrics.lock() {
        m.schema_registry_get_total += 1;
    }
    match run_schema_registry_get_v1(&state, req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "schema_registry_v1_get".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn schema_registry_infer_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<SchemaInferReq>,
) -> impl IntoResponse {
    if let Ok(mut m) = state.metrics.lock() {
        m.schema_registry_infer_total += 1;
    }
    match run_schema_registry_infer_v1(&state, req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "schema_registry_v1_infer".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn schema_registry_register_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<SchemaRegisterReq>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.schema_registry_v2_calls += 1;
    }
    match run_schema_registry_register_v1(&state, req) {
        Ok(mut resp) => {
            resp.operator = "schema_registry_v2_register".to_string();
            observe_operator_latency_v2(
                &state.metrics,
                "schema_registry_v2_register",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "schema_registry_v2_register".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn schema_registry_get_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<SchemaGetReq>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.schema_registry_v2_calls += 1;
    }
    match run_schema_registry_get_v1(&state, req) {
        Ok(mut resp) => {
            resp.operator = "schema_registry_v2_get".to_string();
            observe_operator_latency_v2(
                &state.metrics,
                "schema_registry_v2_get",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "schema_registry_v2_get".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn schema_registry_infer_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<SchemaInferReq>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.schema_registry_v2_calls += 1;
    }
    match run_schema_registry_infer_v1(&state, req) {
        Ok(mut resp) => {
            resp.operator = "schema_registry_v2_infer".to_string();
            observe_operator_latency_v2(
                &state.metrics,
                "schema_registry_v2_infer",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "schema_registry_v2_infer".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn schema_registry_check_compat_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<SchemaCompatReq>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.schema_registry_v2_calls += 1;
    }
    match run_schema_registry_check_compat_v2(&state, req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "schema_registry_v2_check_compat",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "schema_registry_v2_check_compat".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn schema_registry_suggest_migration_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<SchemaMigrationSuggestReq>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.schema_registry_v2_calls += 1;
    }
    match run_schema_registry_suggest_migration_v2(&state, req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "schema_registry_v2_suggest_migration",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "schema_registry_v2_suggest_migration".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}
