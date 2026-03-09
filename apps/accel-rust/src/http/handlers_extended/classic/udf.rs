use super::*;

pub(crate) async fn udf_wasm_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<UdfWasmReq>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.udf_wasm_v1_calls += 1;
    }
    match run_udf_wasm_v1(req) {
        Ok(resp) => {
            observe_operator_latency_v2(&state.metrics, "udf_wasm_v1", begin.elapsed().as_millis());
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "udf_wasm_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}
