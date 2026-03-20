use crate::operators::analytics::{
    AggregateRowsReq, AggregateRowsV2Req, AggregateRowsV3Req, QualityCheckReq, QualityCheckV2Req,
    QualityCheckV3Req, run_aggregate_rows_v1, run_aggregate_rows_v2, run_aggregate_rows_v3,
    run_quality_check_v1, run_quality_check_v2, run_quality_check_v3,
};
use accel_rust::{app_state::AppState, metrics::observe_operator_latency_v2};
use axum::{Json, extract::State, response::IntoResponse};
use std::time::Instant;

use super::support;

pub(crate) async fn aggregate_rows_v1_operator(
    Json(req): Json<AggregateRowsReq>,
) -> impl IntoResponse {
    match run_aggregate_rows_v1(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::bad_request("aggregate_rows_v1", error),
    }
}

pub(crate) async fn aggregate_rows_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<AggregateRowsV2Req>,
) -> impl IntoResponse {
    if let Ok(mut metrics) = state.metrics.lock() {
        metrics.aggregate_rows_v2_calls += 1;
    }
    match run_aggregate_rows_v2(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::bad_request("aggregate_rows_v2", error),
    }
}

pub(crate) async fn aggregate_rows_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<AggregateRowsV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut metrics) = state.metrics.lock() {
        metrics.aggregate_rows_v3_calls += 1;
    }
    match run_aggregate_rows_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "aggregate_rows_v3",
                begin.elapsed().as_millis(),
            );
            support::ok_json(resp)
        }
        Err(error) => support::bad_request("aggregate_rows_v3", error),
    }
}

pub(crate) async fn quality_check_v1_operator(
    Json(req): Json<QualityCheckReq>,
) -> impl IntoResponse {
    match run_quality_check_v1(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::bad_request("quality_check_v1", error),
    }
}

pub(crate) async fn quality_check_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<QualityCheckV2Req>,
) -> impl IntoResponse {
    if let Ok(mut metrics) = state.metrics.lock() {
        metrics.quality_check_v2_calls += 1;
    }
    match run_quality_check_v2(req) {
        Ok(resp) => support::ok_json(resp),
        Err(error) => support::bad_request("quality_check_v2", error),
    }
}

pub(crate) async fn quality_check_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<QualityCheckV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut metrics) = state.metrics.lock() {
        metrics.quality_check_v3_calls += 1;
    }
    match run_quality_check_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "quality_check_v3",
                begin.elapsed().as_millis(),
            );
            support::ok_json(resp)
        }
        Err(error) => support::bad_request("quality_check_v3", error),
    }
}
