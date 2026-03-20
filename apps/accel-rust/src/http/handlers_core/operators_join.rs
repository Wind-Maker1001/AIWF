use crate::operators::join::{
    JoinRowsReq, JoinRowsV2Req, JoinRowsV3Req, run_join_rows_v1, run_join_rows_v2, run_join_rows_v3,
};
use accel_rust::{app_state::AppState, metrics::observe_operator_latency_v2};
use axum::{Json, extract::State, response::IntoResponse};
use std::time::Instant;

use super::support;

pub(crate) async fn join_rows_v1_operator(Json(req): Json<JoinRowsReq>) -> impl IntoResponse {
    match run_join_rows_v1(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::bad_request("join_rows_v1", error),
    }
}

pub(crate) async fn join_rows_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<JoinRowsV2Req>,
) -> impl IntoResponse {
    if let Ok(mut metrics) = state.metrics.lock() {
        metrics.join_rows_v2_calls += 1;
    }
    match run_join_rows_v2(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::bad_request("join_rows_v2", error),
    }
}

pub(crate) async fn join_rows_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<JoinRowsV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut metrics) = state.metrics.lock() {
        metrics.join_rows_v3_calls += 1;
    }
    match run_join_rows_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(&state.metrics, "join_rows_v3", begin.elapsed().as_millis());
            support::ok_json(resp)
        }
        Err(error) => support::bad_request("join_rows_v3", error),
    }
}
