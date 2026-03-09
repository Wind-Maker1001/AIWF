use accel_rust::app_state::{ServiceMetrics, TransformRowsResp};
use serde::Deserialize;
use serde_json::Value;
use std::sync::{Arc, Mutex};

#[derive(Clone, Deserialize)]
pub(crate) struct TransformRowsReq {
    pub run_id: Option<String>,
    pub tenant_id: Option<String>,
    pub trace_id: Option<String>,
    pub traceparent: Option<String>,
    pub rows: Option<Vec<Value>>,
    pub rules: Option<Value>,
    pub rules_dsl: Option<String>,
    pub quality_gates: Option<Value>,
    pub schema_hint: Option<Value>,
    pub input_uri: Option<String>,
    pub output_uri: Option<String>,
    pub request_signature: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Clone, Deserialize)]
pub(crate) struct TransformRowsV3Req {
    pub run_id: Option<String>,
    pub tenant_id: Option<String>,
    pub trace_id: Option<String>,
    pub traceparent: Option<String>,
    pub rows: Option<Vec<Value>>,
    pub rules: Option<Value>,
    pub rules_dsl: Option<String>,
    pub quality_gates: Option<Value>,
    pub schema_hint: Option<Value>,
    pub input_uri: Option<String>,
    pub output_uri: Option<String>,
    pub request_signature: Option<String>,
    pub idempotency_key: Option<String>,
    pub computed_fields_v3: Option<Vec<Value>>,
    pub filter_expr_v3: Option<Value>,
}

pub(crate) fn observe_transform_success(
    metrics: &Arc<Mutex<ServiceMetrics>>,
    resp: &TransformRowsResp,
) {
    if let Ok(mut m) = metrics.lock() {
        m.transform_rows_v2_success_total += 1;
        let engine = resp
            .audit
            .get("engine")
            .and_then(|v| v.as_str())
            .unwrap_or("row_v1");
        if engine == "columnar_v1" || engine == "columnar_arrow_v1" {
            m.transform_rows_v2_columnar_success_total += 1;
        }
        m.transform_rows_v2_latency_ms_sum += resp.stats.latency_ms;
        m.transform_rows_v2_latency_ms_max = m
            .transform_rows_v2_latency_ms_max
            .max(resp.stats.latency_ms);
        m.transform_rows_v2_output_rows_sum += resp.stats.output_rows as u64;
        let ms = resp.stats.latency_ms;
        if ms <= 10 {
            m.latency_le_10ms += 1;
        } else if ms <= 50 {
            m.latency_le_50ms += 1;
        } else if ms <= 200 {
            m.latency_le_200ms += 1;
        } else {
            m.latency_gt_200ms += 1;
        }
    }
}
