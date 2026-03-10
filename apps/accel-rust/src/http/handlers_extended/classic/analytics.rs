use super::*;

pub(crate) async fn time_series_v1_operator(Json(req): Json<TimeSeriesReq>) -> impl IntoResponse {
    match run_time_series_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "time_series_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn stats_v1_operator(Json(req): Json<StatsReq>) -> impl IntoResponse {
    match run_stats_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stats_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn entity_linking_v1_operator(
    Json(req): Json<EntityLinkReq>,
) -> impl IntoResponse {
    match run_entity_linking_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "entity_linking_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn table_reconstruct_v1_operator(
    Json(req): Json<TableReconstructReq>,
) -> impl IntoResponse {
    match run_table_reconstruct_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "table_reconstruct_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn feature_store_upsert_v1_operator(
    Json(req): Json<FeatureStoreUpsertReq>,
) -> impl IntoResponse {
    match run_feature_store_upsert_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "feature_store_v1_upsert".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn feature_store_get_v1_operator(
    Json(req): Json<FeatureStoreGetReq>,
) -> impl IntoResponse {
    match run_feature_store_get_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "feature_store_v1_get".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn lineage_v2_operator(Json(req): Json<LineageV2Req>) -> impl IntoResponse {
    match run_lineage_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "lineage_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn rule_simulator_v1_operator(
    Json(req): Json<RuleSimulatorReq>,
) -> impl IntoResponse {
    match run_rule_simulator_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "rule_simulator_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn constraint_solver_v1_operator(
    Json(req): Json<ConstraintSolverReq>,
) -> impl IntoResponse {
    match run_constraint_solver_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "constraint_solver_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn chart_data_prep_v1_operator(
    Json(req): Json<ChartDataPrepReq>,
) -> impl IntoResponse {
    match run_chart_data_prep_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "chart_data_prep_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

pub(crate) async fn diff_audit_v1_operator(Json(req): Json<DiffAuditReq>) -> impl IntoResponse {
    match run_diff_audit_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "diff_audit_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}
