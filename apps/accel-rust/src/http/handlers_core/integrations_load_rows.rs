use crate::{
    api_types::{LoadRowsReq, LoadRowsV2Req, LoadRowsV3Req},
    load_ops::{run_load_rows_v1, run_load_rows_v2, run_load_rows_v3},
};
use accel_rust::{app_state::AppState, metrics::observe_operator_latency_v2};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use std::time::Instant;

use super::support;

pub(crate) async fn load_rows_v1_operator(Json(req): Json<LoadRowsReq>) -> impl IntoResponse {
    match run_load_rows_v1(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::error_json(StatusCode::BAD_REQUEST, "load_rows_v1", error),
    }
}

pub(crate) async fn load_rows_v2_operator(Json(req): Json<LoadRowsV2Req>) -> impl IntoResponse {
    match run_load_rows_v2(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::error_json(StatusCode::BAD_REQUEST, "load_rows_v2", error),
    }
}

pub(crate) async fn load_rows_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<LoadRowsV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut metrics) = state.metrics.lock() {
        metrics.load_rows_v3_calls += 1;
    }
    match run_load_rows_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(&state.metrics, "load_rows_v3", begin.elapsed().as_millis());
            support::ok_json(resp)
        }
        Err(error) => support::error_json(StatusCode::BAD_REQUEST, "load_rows_v3", error),
    }
}
