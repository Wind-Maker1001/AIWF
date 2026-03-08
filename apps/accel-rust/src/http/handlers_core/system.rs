use crate::*;

pub(crate) async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let resp = HealthResp {
        ok: true,
        service: state.service,
    };
    (StatusCode::OK, Json(resp))
}

pub(crate) async fn reload_runtime_config(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = resolve_task_store_backend(task_store_config_from_env());
    if let Ok(mut guard) = state.task_cfg.lock() {
        *guard = cfg.clone();
    }
    refresh_remote_task_store_probe_once(&state);
    let resp = json!({
        "ok": true,
        "task_store_remote": cfg.remote_enabled,
        "task_store_backend": cfg.backend,
        "ttl_sec": cfg.ttl_sec,
        "max_tasks": cfg.max_tasks,
    });
    (StatusCode::OK, Json(resp))
}

pub(crate) async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let (
        t_calls,
        t_err,
        t_ok,
        t_col_calls,
        t_col_ok,
        t_latency_sum,
        t_latency_max,
        t_rows_sum,
        p_calls,
        p_err,
        remote_ok,
        remote_failures,
        remote_probe_epoch,
        cancel_requested,
        cancel_effective,
        flag_cleanup,
        tasks_active,
        task_retry_total,
        tenant_reject_total,
        quota_reject_total,
        lat_10,
        lat_50,
        lat_200,
        lat_gt_200,
        cache_hit_total,
        cache_miss_total,
        cache_evict_total,
        join_v2_calls,
        agg_v2_calls,
        qc_v2_calls,
        schema_reg_total,
        schema_get_total,
        schema_infer_total,
    ) = if let Ok(m) = state.metrics.lock() {
        (
            m.transform_rows_v2_calls,
            m.transform_rows_v2_errors,
            m.transform_rows_v2_success_total,
            m.transform_rows_v2_columnar_calls,
            m.transform_rows_v2_columnar_success_total,
            m.transform_rows_v2_latency_ms_sum,
            m.transform_rows_v2_latency_ms_max,
            m.transform_rows_v2_output_rows_sum,
            m.text_preprocess_v2_calls,
            m.text_preprocess_v2_errors,
            m.task_store_remote_ok,
            m.task_store_remote_probe_failures,
            m.task_store_remote_last_probe_epoch,
            m.task_cancel_requested_total,
            m.task_cancel_effective_total,
            m.task_flag_cleanup_total,
            m.tasks_active,
            m.task_retry_total,
            m.tenant_reject_total,
            m.quota_reject_total,
            m.latency_le_10ms,
            m.latency_le_50ms,
            m.latency_le_200ms,
            m.latency_gt_200ms,
            m.transform_cache_hit_total,
            m.transform_cache_miss_total,
            m.transform_cache_evict_total,
            m.join_rows_v2_calls,
            m.aggregate_rows_v2_calls,
            m.quality_check_v2_calls,
            m.schema_registry_register_total,
            m.schema_registry_get_total,
            m.schema_registry_infer_total,
        )
    } else {
        (
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
        )
    };
    let cfg = current_task_cfg(&state);
    let remote_enabled = if task_store_remote_enabled(&cfg) {
        1
    } else {
        0
    };
    let remote_ok_num = if remote_ok { 1 } else { 0 };
    let body = format!(
        "aiwf_transform_rows_v2_calls_total {t_calls}\naiwf_transform_rows_v2_errors_total {t_err}\naiwf_transform_rows_v2_success_total {t_ok}\naiwf_transform_rows_v2_columnar_calls_total {t_col_calls}\naiwf_transform_rows_v2_columnar_success_total {t_col_ok}\naiwf_transform_rows_v2_latency_ms_sum {t_latency_sum}\naiwf_transform_rows_v2_latency_ms_max {t_latency_max}\naiwf_transform_rows_v2_output_rows_sum {t_rows_sum}\naiwf_text_preprocess_v2_calls_total {p_calls}\naiwf_text_preprocess_v2_errors_total {p_err}\naiwf_task_store_remote_enabled {remote_enabled}\naiwf_task_store_remote_ok {remote_ok_num}\naiwf_task_store_remote_probe_failures_total {remote_failures}\naiwf_task_store_remote_last_probe_epoch {remote_probe_epoch}\naiwf_task_cancel_requested_total {cancel_requested}\naiwf_task_cancel_effective_total {cancel_effective}\naiwf_task_flag_cleanup_total {flag_cleanup}\naiwf_tasks_active {tasks_active}\naiwf_task_retry_total {task_retry_total}\naiwf_tenant_reject_total {tenant_reject_total}\naiwf_quota_reject_total {quota_reject_total}\naiwf_transform_rows_v2_latency_bucket_le_10ms {lat_10}\naiwf_transform_rows_v2_latency_bucket_le_50ms {lat_50}\naiwf_transform_rows_v2_latency_bucket_le_200ms {lat_200}\naiwf_transform_rows_v2_latency_bucket_gt_200ms {lat_gt_200}\naiwf_transform_rows_v2_cache_hit_total {cache_hit_total}\naiwf_transform_rows_v2_cache_miss_total {cache_miss_total}\naiwf_transform_rows_v2_cache_evict_total {cache_evict_total}\naiwf_join_rows_v2_calls_total {join_v2_calls}\naiwf_aggregate_rows_v2_calls_total {agg_v2_calls}\naiwf_quality_check_v2_calls_total {qc_v2_calls}\naiwf_schema_registry_register_total {schema_reg_total}\naiwf_schema_registry_get_total {schema_get_total}\naiwf_schema_registry_infer_total {schema_infer_total}\n"
    );
    (StatusCode::OK, body)
}

pub(crate) async fn metrics_v2(State(state): State<AppState>) -> impl IntoResponse {
    let mut out = BTreeMap::new();
    if let Ok(m) = state.metrics.lock() {
        for (op, samples) in &m.operator_latency_samples {
            let mut sorted = samples.clone();
            sorted.sort_unstable();
            let p50 = percentile_from_sorted(&sorted, 0.50);
            let p95 = percentile_from_sorted(&sorted, 0.95);
            let p99 = percentile_from_sorted(&sorted, 0.99);
            out.insert(
                op.clone(),
                json!({
                    "count": sorted.len(),
                    "p50_ms": p50,
                    "p95_ms": p95,
                    "p99_ms": p99,
                    "max_ms": sorted.last().copied().unwrap_or(0),
                }),
            );
        }
    }
    (
        StatusCode::OK,
        Json(json!({"ok": true, "operator": "metrics_v2", "latency": out})),
    )
}

pub(crate) async fn metrics_v2_prom(State(state): State<AppState>) -> impl IntoResponse {
    let mut lines = Vec::new();
    if let Ok(m) = state.metrics.lock() {
        for (op, samples) in &m.operator_latency_samples {
            let mut sorted = samples.clone();
            sorted.sort_unstable();
            let p50 = percentile_from_sorted(&sorted, 0.50);
            let p95 = percentile_from_sorted(&sorted, 0.95);
            let p99 = percentile_from_sorted(&sorted, 0.99);
            let name = op.replace('-', "_");
            lines.push(format!(
                "aiwf_operator_latency_count{{operator=\"{name}\"}} {}",
                sorted.len()
            ));
            lines.push(format!(
                "aiwf_operator_latency_p50_ms{{operator=\"{name}\"}} {p50}"
            ));
            lines.push(format!(
                "aiwf_operator_latency_p95_ms{{operator=\"{name}\"}} {p95}"
            ));
            lines.push(format!(
                "aiwf_operator_latency_p99_ms{{operator=\"{name}\"}} {p99}"
            ));
        }
    }
    (StatusCode::OK, lines.join("\n"))
}
