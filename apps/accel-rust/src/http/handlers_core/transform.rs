use crate::{
    api_types::{
        CleaningReq, ComputeReq, ErrResp, RulesCompileReq, RulesCompileResp, TextPreprocessReq,
    },
    cleaning_runtime::{run_cleaning_operator, run_compute_metrics},
    current_task_cfg,
    misc_ops::{compile_rules_dsl, run_text_preprocess_v2},
    operators::transform::{
        TransformRowsReq, TransformRowsV3Req, observe_transform_success,
        run_transform_rows_v2_with_cache, run_transform_rows_v3,
    },
    transform_support::{
        cleanup_task_flag, enforce_tenant_payload_quota, release_tenant_slot,
        request_prefers_columnar, transform_cache_enabled, transform_cache_max_entries,
        transform_cache_ttl_sec, try_acquire_tenant_slot, unique_trace, unix_now_sec, utc_now_iso,
        verify_request_signature,
    },
};
use accel_rust::{
    app_state::{AppState, TaskState, TransformRowsResp},
    metrics::observe_operator_latency_v2,
    task_store::{persist_tasks_to_store, prune_tasks, task_store_upsert_task},
};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde_json::json;
use std::{
    env,
    sync::{Arc, atomic::AtomicBool},
    time::Instant,
};

fn normalized_key(value: Option<String>) -> Option<String> {
    value.and_then(|item| {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn effective_submit_idempotency_key(req: &TransformRowsReq, tenant_id: &str) -> String {
    normalized_key(req.idempotency_key.clone())
        .or_else(|| normalized_key(req.run_id.clone()))
        .unwrap_or_else(|| unique_trace(&format!("tenant:{tenant_id}:submit")))
}

fn next_submit_task_id(req: &TransformRowsReq, id_full: &str) -> String {
    let run_id = normalized_key(req.run_id.clone()).unwrap_or_default();
    unique_trace(&format!("task:{run_id}:{id_full}"))
}

#[path = "transform/sync_handlers.rs"]
mod sync_handlers;
pub(crate) use sync_handlers::{
    cleaning_operator, compute_metrics_operator, transform_rows_v2_cache_clear_operator,
    transform_rows_v2_cache_stats_operator, transform_rows_v2_operator, transform_rows_v3_operator,
};

#[path = "transform/submit.rs"]
mod submit;
pub(crate) use submit::transform_rows_v2_submit_operator;

#[path = "transform/misc.rs"]
mod misc;
pub(crate) use misc::{rules_compile_v1_operator, text_preprocess_v2_operator};

#[cfg(test)]
mod tests {
    use super::{effective_submit_idempotency_key, next_submit_task_id};
    use crate::TransformRowsReq;

    fn blank_req() -> TransformRowsReq {
        TransformRowsReq {
            run_id: Some("   ".to_string()),
            tenant_id: None,
            trace_id: None,
            traceparent: None,
            rows: None,
            rules: None,
            rules_dsl: None,
            quality_gates: None,
            schema_hint: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: Some("".to_string()),
        }
    }

    #[test]
    fn submit_idempotency_fallback_is_unique_when_inputs_are_blank() {
        let first = effective_submit_idempotency_key(&blank_req(), "tenant-a");
        let second = effective_submit_idempotency_key(&blank_req(), "tenant-a");
        assert_ne!(first, second);
    }

    #[test]
    fn submit_task_id_is_unique_for_same_request_shape() {
        let req = blank_req();
        let first = next_submit_task_id(&req, "tenant-a:key");
        let second = next_submit_task_id(&req, "tenant-a:key");
        assert_ne!(first, second);
    }
}
