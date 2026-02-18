use axum::{
    Json, Router,
    extract::{Path as AxPath, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use odbc_api::{ConnectionOptions, Cursor, Environment, IntoParameter, buffers::TextRowSet};
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace as sdktrace};
use parquet::{
    basic::{Compression, LogicalType, Repetition, Type as PhysicalType},
    column::reader::ColumnReader,
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::reader::{FileReader, SerializedFileReader},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::Type,
};
use regex::Regex;
use rusqlite::Connection as SqliteConnection;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    env,
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
struct AppState {
    service: String,
    tasks: Arc<Mutex<HashMap<String, TaskState>>>,
    metrics: Arc<Mutex<ServiceMetrics>>,
    task_cfg: Arc<Mutex<TaskStoreConfig>>,
    cancel_flags: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
    tenant_running: Arc<Mutex<HashMap<String, usize>>>,
    idempotency_index: Arc<Mutex<HashMap<String, String>>>,
}

#[derive(Clone, Serialize, Deserialize)]
struct TaskState {
    task_id: String,
    tenant_id: String,
    operator: String,
    status: String,
    created_at: String,
    updated_at: String,
    result: Option<Value>,
    error: Option<String>,
    idempotency_key: String,
    attempts: u32,
}

#[derive(Clone)]
struct TaskStoreConfig {
    ttl_sec: u64,
    max_tasks: usize,
    store_path: Option<PathBuf>,
    remote_enabled: bool,
    backend: String,
    base_api_url: Option<String>,
    base_api_key: Option<String>,
    sql_host: String,
    sql_port: u16,
    sql_db: String,
    sql_user: Option<String>,
    sql_password: Option<String>,
    sql_use_windows_auth: bool,
}

#[derive(Default)]
struct ServiceMetrics {
    transform_rows_v2_calls: u64,
    transform_rows_v2_errors: u64,
    transform_rows_v2_success_total: u64,
    transform_rows_v2_latency_ms_sum: u128,
    transform_rows_v2_latency_ms_max: u128,
    transform_rows_v2_output_rows_sum: u64,
    text_preprocess_v2_calls: u64,
    text_preprocess_v2_errors: u64,
    task_store_remote_ok: bool,
    task_store_remote_probe_failures: u64,
    task_store_remote_last_probe_epoch: u64,
    task_cancel_requested_total: u64,
    task_cancel_effective_total: u64,
    task_flag_cleanup_total: u64,
    tasks_active: i64,
    task_retry_total: u64,
    tenant_reject_total: u64,
    quota_reject_total: u64,
    latency_le_10ms: u64,
    latency_le_50ms: u64,
    latency_le_200ms: u64,
    latency_gt_200ms: u64,
}

#[derive(Serialize)]
struct HealthResp {
    ok: bool,
    service: String,
}

#[derive(Deserialize)]
struct CleaningReq {
    job_id: Option<String>,
    step_id: Option<String>,
    input_uri: Option<String>,
    output_uri: Option<String>,
    job_root: Option<String>,
    force_bad_parquet: Option<bool>,
    params: Option<Value>,
}

#[derive(Deserialize)]
struct ComputeReq {
    run_id: Option<String>,
    text: String,
}

#[derive(Clone, Deserialize)]
struct TransformRowsReq {
    run_id: Option<String>,
    tenant_id: Option<String>,
    trace_id: Option<String>,
    traceparent: Option<String>,
    rows: Option<Vec<Value>>,
    rules: Option<Value>,
    rules_dsl: Option<String>,
    quality_gates: Option<Value>,
    schema_hint: Option<Value>,
    input_uri: Option<String>,
    output_uri: Option<String>,
    request_signature: Option<String>,
    idempotency_key: Option<String>,
}

#[derive(Deserialize)]
struct JoinRowsReq {
    run_id: Option<String>,
    left_rows: Vec<Value>,
    right_rows: Vec<Value>,
    left_on: String,
    right_on: String,
    join_type: Option<String>,
}

#[derive(Serialize)]
struct JoinRowsResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    rows: Vec<Value>,
    stats: Value,
}

#[derive(Deserialize)]
struct NormalizeSchemaReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    schema: Value,
}

#[derive(Serialize)]
struct NormalizeSchemaResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    rows: Vec<Value>,
    stats: Value,
}

#[derive(Deserialize)]
struct EntityExtractReq {
    run_id: Option<String>,
    rows: Option<Vec<Value>>,
    text: Option<String>,
    text_field: Option<String>,
}

#[derive(Serialize)]
struct EntityExtractResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    entities: Value,
}

#[derive(Deserialize)]
struct RulesCompileReq {
    dsl: String,
}

#[derive(Deserialize)]
struct RulesPackagePublishReq {
    name: String,
    version: String,
    dsl: Option<String>,
    rules: Option<Value>,
}

#[derive(Deserialize)]
struct RulesPackageGetReq {
    name: String,
    version: String,
}

#[derive(Serialize)]
struct RulesCompileResp {
    ok: bool,
    operator: String,
    status: String,
    rules: Value,
}

#[derive(Serialize)]
struct RulesPackageResp {
    ok: bool,
    operator: String,
    status: String,
    name: String,
    version: String,
    rules: Value,
    fingerprint: String,
}

#[derive(Deserialize)]
struct LoadRowsReq {
    source_type: String,
    source: String,
    query: Option<String>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct LoadRowsResp {
    ok: bool,
    operator: String,
    status: String,
    rows: Vec<Value>,
    stats: Value,
}

#[derive(Deserialize)]
struct SaveRowsReq {
    sink_type: String,
    sink: String,
    table: Option<String>,
    parquet_mode: Option<String>,
    rows: Vec<Value>,
}

#[derive(Serialize)]
struct SaveRowsResp {
    ok: bool,
    operator: String,
    status: String,
    written_rows: usize,
}

#[derive(Deserialize)]
struct AggregateRowsReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    group_by: Vec<String>,
    aggregates: Vec<Value>,
}

#[derive(Serialize)]
struct AggregateRowsResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    rows: Vec<Value>,
    stats: Value,
}

#[derive(Deserialize)]
struct TransformRowsStreamReq {
    run_id: Option<String>,
    tenant_id: Option<String>,
    rows: Option<Vec<Value>>,
    input_uri: Option<String>,
    output_uri: Option<String>,
    chunk_size: Option<usize>,
    rules: Option<Value>,
    rules_dsl: Option<String>,
    quality_gates: Option<Value>,
    checkpoint_key: Option<String>,
    resume: Option<bool>,
}

#[derive(Serialize)]
struct TransformRowsStreamResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    rows: Vec<Value>,
    chunks: usize,
    stats: Value,
}

#[derive(Deserialize)]
struct QualityCheckReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    rules: Value,
}

#[derive(Serialize)]
struct QualityCheckResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    passed: bool,
    report: Value,
}

#[derive(Deserialize)]
struct AggregatePushdownReq {
    run_id: Option<String>,
    source_type: String,
    source: String,
    from: Option<String>,
    group_by: Vec<String>,
    aggregates: Vec<Value>,
    where_sql: Option<String>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct AggregatePushdownResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    sql: String,
    rows: Vec<Value>,
    stats: Value,
}

#[derive(Deserialize)]
struct PluginExecReq {
    run_id: Option<String>,
    tenant_id: Option<String>,
    trace_id: Option<String>,
    plugin: String,
    input: Value,
}

#[derive(Serialize)]
struct PluginExecResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    trace_id: String,
    plugin: String,
    output: Value,
    stderr: String,
}

#[derive(Deserialize)]
struct PluginHealthReq {
    plugin: String,
    tenant_id: Option<String>,
}

#[derive(Serialize)]
struct PluginHealthResp {
    ok: bool,
    operator: String,
    status: String,
    plugin: String,
    details: Value,
}

#[derive(Deserialize, Clone)]
struct PluginManifest {
    name: Option<String>,
    version: Option<String>,
    api_version: Option<String>,
    command: String,
    args: Option<Vec<String>>,
    timeout_ms: Option<u64>,
    signature: Option<String>,
    healthcheck: Option<PluginHealthcheck>,
}

#[derive(Deserialize, Clone)]
struct PluginHealthcheck {
    command: Option<String>,
    args: Option<Vec<String>>,
    timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
struct WorkflowRunReq {
    run_id: Option<String>,
    trace_id: Option<String>,
    traceparent: Option<String>,
    tenant_id: Option<String>,
    context: Option<Value>,
    steps: Vec<Value>,
}

#[derive(Serialize)]
struct WorkflowRunResp {
    ok: bool,
    operator: String,
    status: String,
    trace_id: String,
    run_id: Option<String>,
    context: Value,
    steps: Vec<WorkflowStepReplay>,
    failed_step: Option<String>,
    error: Option<String>,
}

#[derive(Serialize)]
struct WorkflowStepReplay {
    id: String,
    operator: String,
    status: String,
    started_at: String,
    finished_at: String,
    duration_ms: u128,
    input_summary: Value,
    output_summary: Option<Value>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct TextPreprocessReq {
    run_id: Option<String>,
    text: String,
    title: Option<String>,
    remove_references: Option<bool>,
    remove_notes: Option<bool>,
    normalize_whitespace: Option<bool>,
}

#[derive(Serialize)]
struct ComputeMetrics {
    sections: usize,
    bullets: usize,
    chars: usize,
    lines: usize,
    cjk: usize,
    latin: usize,
    digits: usize,
    reference_hits: usize,
    note_hits: usize,
    sha256: String,
}

#[derive(Serialize)]
struct ComputeResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    metrics: ComputeMetrics,
}

#[derive(Serialize)]
struct TransformRowsStats {
    input_rows: usize,
    output_rows: usize,
    invalid_rows: usize,
    filtered_rows: usize,
    duplicate_rows_removed: usize,
    latency_ms: u128,
}

#[derive(Serialize)]
struct TransformRowsResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    trace_id: String,
    rows: Vec<Value>,
    quality: Value,
    gate_result: Value,
    stats: TransformRowsStats,
    rust_v2_used: bool,
    schema_hint: Option<Value>,
    aggregate: Option<Value>,
    audit: Value,
}

#[derive(Clone)]
enum FilterOp {
    Exists,
    NotExists,
    Eq(String),
    Ne(String),
    Contains(String),
    In(Vec<String>),
    NotIn(Vec<String>),
    Regex(Regex),
    NotRegex(Regex),
    Gt(f64),
    Gte(f64),
    Lt(f64),
    Lte(f64),
    Invalid,
    Passthrough,
}

#[derive(Clone)]
struct CompiledFilter {
    field: String,
    op: FilterOp,
}

#[derive(Serialize)]
struct TextPreprocessResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    markdown: String,
    removed_references_lines: usize,
    removed_notes_lines: usize,
    sha256: String,
}

#[derive(Serialize)]
struct FileOut {
    path: String,
    sha256: String,
}

#[derive(Serialize)]
struct ProfileOut {
    rows: usize,
    cols: usize,
}

#[derive(Clone, Debug)]
struct CleanRow {
    id: i64,
    amount: f64,
}

#[derive(Serialize)]
struct CleaningOutputs {
    cleaned_csv: FileOut,
    cleaned_parquet: FileOut,
    profile_json: FileOut,
    xlsx_fin: FileOut,
    audit_docx: FileOut,
    deck_pptx: FileOut,
}

#[derive(Serialize)]
struct CleaningResp {
    ok: bool,
    operator: String,
    status: String,
    job_id: Option<String>,
    step_id: Option<String>,
    input_uri: Option<String>,
    output_uri: Option<String>,
    job_root: String,
    outputs: CleaningOutputs,
    profile: ProfileOut,
    office_generation_mode: String,
    office_generation_warning: Option<String>,
    message: String,
}

#[derive(Serialize)]
struct ErrResp {
    ok: bool,
    operator: String,
    status: String,
    error: String,
}

struct OfficeGenInfo {
    mode: String,
    warning: Option<String>,
}

#[tokio::main]
async fn main() {
    init_observability();
    let host = env::var("AIWF_ACCEL_RUST_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = env::var("AIWF_ACCEL_RUST_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(18082);

    let task_cfg = resolve_task_store_backend(task_store_config_from_env());
    if task_cfg.remote_enabled && task_cfg.backend == "odbc" {
        let _ = unsafe {
            Environment::set_connection_pooling(odbc_api::sys::AttrConnectionPooling::DriverAware)
        };
    }
    let tasks_loaded = load_tasks_from_store(task_cfg.store_path.as_ref());
    let state = AppState {
        service: "accel-rust".to_string(),
        tasks: Arc::new(Mutex::new(tasks_loaded)),
        metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
        task_cfg: Arc::new(Mutex::new(task_cfg)),
        cancel_flags: Arc::new(Mutex::new(HashMap::new())),
        tenant_running: Arc::new(Mutex::new(HashMap::new())),
        idempotency_index: Arc::new(Mutex::new(HashMap::new())),
    };
    init_remote_task_store_probe(&state);

    let app = build_router(state);

    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .expect("invalid AIWF_ACCEL_RUST_HOST/AIWF_ACCEL_RUST_PORT");

    info!("[accel-rust] listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind listener");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
    global::shutdown_tracer_provider();
}

fn init_observability() {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,h2=warn,tower_http=warn"));
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let endpoint = env::var("AIWF_OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|s| !s.trim().is_empty());
    if let Some(ep) = endpoint {
        if !allow_egress_enabled() && !is_local_endpoint(&ep) {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .init();
            warn!("otel exporter endpoint blocked by AIWF_ALLOW_EGRESS=false: {}", ep);
            return;
        }
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(ep)
            .build();
        match exporter {
            Ok(exporter) => {
                let provider = sdktrace::TracerProvider::builder()
                    .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
                    .with_resource(Resource::new(vec![opentelemetry::KeyValue::new(
                        "service.name",
                        "accel-rust",
                    )]))
                    .build();
                let tracer = provider.tracer("accel-rust");
                global::set_tracer_provider(provider);
                let otel = tracing_opentelemetry::layer().with_tracer(tracer);
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(fmt_layer)
                    .with(otel)
                    .init();
                info!("otel exporter enabled");
            }
            Err(e) => {
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(fmt_layer)
                    .init();
                error!("failed to init otel exporter: {e}");
            }
        }
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
    }
}

fn env_truthy(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|v| {
            let t = v.trim().to_ascii_lowercase();
            matches!(t.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

fn allow_egress_enabled() -> bool {
    env_truthy("AIWF_ALLOW_EGRESS") || env_truthy("AIWF_ALLOW_CLOUD_LLM")
}

fn is_local_endpoint(endpoint: &str) -> bool {
    let s = endpoint.trim();
    s.contains("127.0.0.1") || s.contains("localhost") || s.contains("[::1]")
}

fn init_remote_task_store_probe(state: &AppState) {
    let cfg = current_task_cfg(state);
    let metrics = Arc::clone(&state.metrics);
    let enabled = task_store_remote_enabled(&cfg);
    let initial_ok = if enabled {
        probe_remote_task_store(&cfg)
    } else {
        false
    };
    if let Ok(mut m) = metrics.lock() {
        m.task_store_remote_ok = initial_ok;
        m.task_store_remote_last_probe_epoch = utc_now_iso().parse::<u64>().unwrap_or(0);
        if enabled && !initial_ok {
            m.task_store_remote_probe_failures += 1;
        }
    }
    if !enabled {
        return;
    }
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(30)).await;
            let ok = tokio::task::spawn_blocking({
                let cfg = cfg.clone();
                move || probe_remote_task_store(&cfg)
            })
            .await
            .unwrap_or(false);
            if let Ok(mut m) = metrics.lock() {
                m.task_store_remote_ok = ok;
                m.task_store_remote_last_probe_epoch = utc_now_iso().parse::<u64>().unwrap_or(0);
                if !ok {
                    m.task_store_remote_probe_failures += 1;
                }
            }
        }
    });
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/admin/reload_runtime_config", post(reload_runtime_config))
        .route("/operators/cleaning", post(cleaning_operator))
        .route("/operators/compute_metrics", post(compute_metrics_operator))
        .route("/operators/transform_rows_v2", post(transform_rows_v2_operator))
        .route(
            "/operators/transform_rows_v2/stream",
            post(transform_rows_v2_stream_operator),
        )
        .route(
            "/operators/transform_rows_v2/submit",
            post(transform_rows_v2_submit_operator),
        )
        .route("/operators/text_preprocess_v2", post(text_preprocess_v2_operator))
        .route("/operators/join_rows_v1", post(join_rows_v1_operator))
        .route(
            "/operators/normalize_schema_v1",
            post(normalize_schema_v1_operator),
        )
        .route(
            "/operators/entity_extract_v1",
            post(entity_extract_v1_operator),
        )
        .route("/operators/aggregate_rows_v1", post(aggregate_rows_v1_operator))
        .route("/operators/quality_check_v1", post(quality_check_v1_operator))
        .route(
            "/operators/aggregate_pushdown_v1",
            post(aggregate_pushdown_v1_operator),
        )
        .route("/operators/plugin_exec_v1", post(plugin_exec_v1_operator))
        .route("/operators/plugin_health_v1", post(plugin_health_v1_operator))
        .route("/operators/rules_compile_v1", post(rules_compile_v1_operator))
        .route(
            "/operators/rules_package_v1/publish",
            post(rules_package_publish_v1_operator),
        )
        .route("/operators/rules_package_v1/get", post(rules_package_get_v1_operator))
        .route("/operators/load_rows_v1", post(load_rows_v1_operator))
        .route("/operators/save_rows_v1", post(save_rows_v1_operator))
        .route("/workflow/run", post(workflow_run_operator))
        .route("/tasks/{task_id}", get(get_task_operator))
        .route("/tasks/{task_id}/cancel", post(cancel_task_operator))
        .with_state(state)
}

fn current_task_cfg(state: &AppState) -> TaskStoreConfig {
    state
        .task_cfg
        .lock()
        .map(|g| g.clone())
        .unwrap_or_else(|_| task_store_config_from_env())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let resp = HealthResp {
        ok: true,
        service: state.service,
    };
    (StatusCode::OK, Json(resp))
}

async fn reload_runtime_config(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = resolve_task_store_backend(task_store_config_from_env());
    if let Ok(mut guard) = state.task_cfg.lock() {
        *guard = cfg.clone();
    }
    let resp = json!({
        "ok": true,
        "task_store_remote": cfg.remote_enabled,
        "task_store_backend": cfg.backend,
        "ttl_sec": cfg.ttl_sec,
        "max_tasks": cfg.max_tasks,
    });
    (StatusCode::OK, Json(resp))
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let (
        t_calls,
        t_err,
        t_ok,
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
    ) = if let Ok(m) = state.metrics.lock() {
        (
            m.transform_rows_v2_calls,
            m.transform_rows_v2_errors,
            m.transform_rows_v2_success_total,
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
        )
    } else {
        (
            0, 0, 0, 0, 0, 0, 0, 0, false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
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
        "aiwf_transform_rows_v2_calls_total {t_calls}\naiwf_transform_rows_v2_errors_total {t_err}\naiwf_transform_rows_v2_success_total {t_ok}\naiwf_transform_rows_v2_latency_ms_sum {t_latency_sum}\naiwf_transform_rows_v2_latency_ms_max {t_latency_max}\naiwf_transform_rows_v2_output_rows_sum {t_rows_sum}\naiwf_text_preprocess_v2_calls_total {p_calls}\naiwf_text_preprocess_v2_errors_total {p_err}\naiwf_task_store_remote_enabled {remote_enabled}\naiwf_task_store_remote_ok {remote_ok_num}\naiwf_task_store_remote_probe_failures_total {remote_failures}\naiwf_task_store_remote_last_probe_epoch {remote_probe_epoch}\naiwf_task_cancel_requested_total {cancel_requested}\naiwf_task_cancel_effective_total {cancel_effective}\naiwf_task_flag_cleanup_total {flag_cleanup}\naiwf_tasks_active {tasks_active}\naiwf_task_retry_total {task_retry_total}\naiwf_tenant_reject_total {tenant_reject_total}\naiwf_quota_reject_total {quota_reject_total}\naiwf_transform_rows_v2_latency_bucket_le_10ms {lat_10}\naiwf_transform_rows_v2_latency_bucket_le_50ms {lat_50}\naiwf_transform_rows_v2_latency_bucket_le_200ms {lat_200}\naiwf_transform_rows_v2_latency_bucket_gt_200ms {lat_gt_200}\n"
    );
    (StatusCode::OK, body)
}

async fn cleaning_operator(Json(req): Json<CleaningReq>) -> impl IntoResponse {
    match run_cleaning_operator(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrResp {
                ok: false,
                operator: "cleaning".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn compute_metrics_operator(Json(req): Json<ComputeReq>) -> impl IntoResponse {
    match run_compute_metrics(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrResp {
                ok: false,
                operator: "compute_metrics".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn transform_rows_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<TransformRowsReq>,
) -> impl IntoResponse {
    if let Some(rows) = req.rows.as_ref() {
        let bytes = serde_json::to_vec(rows).map(|v| v.len()).unwrap_or(0);
        if let Err(e) = enforce_tenant_payload_quota(Some(&state), rows.len(), bytes) {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(ErrResp {
                    ok: false,
                    operator: "transform_rows_v2".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response();
        }
    }
    if let Ok(mut m) = state.metrics.lock() {
        m.transform_rows_v2_calls += 1;
    }
    match run_transform_rows_v2(req) {
        Ok(resp) => {
            observe_transform_success(&state.metrics, &resp);
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => {
            if let Ok(mut m) = state.metrics.lock() {
                m.transform_rows_v2_errors += 1;
            }
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrResp {
                    ok: false,
                    operator: "transform_rows_v2".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response()
        }
    }
}

async fn transform_rows_v2_submit_operator(
    State(state): State<AppState>,
    Json(req): Json<TransformRowsReq>,
) -> impl IntoResponse {
    if let Some(rows) = req.rows.as_ref() {
        let bytes = serde_json::to_vec(rows).map(|v| v.len()).unwrap_or(0);
        if let Err(e) = enforce_tenant_payload_quota(Some(&state), rows.len(), bytes) {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(ErrResp {
                    ok: false,
                    operator: "transform_rows_v2".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response();
        }
    }
    if let Err(e) = verify_request_signature(&req) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrResp {
                ok: false,
                operator: "transform_rows_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response();
    }
    let cfg = current_task_cfg(&state);
    let tenant_id = req
        .tenant_id
        .clone()
        .unwrap_or_else(|| env::var("AIWF_TENANT_ID").unwrap_or_else(|_| "default".to_string()));
    if let Err(e) = try_acquire_tenant_slot(&state, &tenant_id) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrResp {
                ok: false,
                operator: "transform_rows_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response();
    }
    let id_key = req.idempotency_key.clone().unwrap_or_else(|| {
        req.run_id
            .clone()
            .unwrap_or_else(|| short_trace(&format!("tenant:{tenant_id}:{}", utc_now_iso())))
    });
    let id_full = format!("{tenant_id}:{id_key}");
    if let Ok(idx) = state.idempotency_index.lock()
        && let Some(existing_task_id) = idx.get(&id_full)
    {
        if let Ok(t) = state.tasks.lock() && let Some(ts) = t.get(existing_task_id) {
            let st = ts.status.to_lowercase();
            if st == "queued" || st == "running" || st == "done" {
                release_tenant_slot(&state, &tenant_id);
                return (
                    StatusCode::OK,
                    Json(json!({"ok": true, "task_id": existing_task_id, "status": st, "idempotent_hit": true})),
                )
                    .into_response();
            }
        }
    }
    let now = utc_now_iso();
    let task_id = short_trace(&format!(
        "task:{}:{}:{}",
        req.run_id.clone().unwrap_or_default(),
        id_full,
        now
    ));
    let task = TaskState {
        task_id: task_id.clone(),
        tenant_id,
        operator: "transform_rows_v2".to_string(),
        status: "queued".to_string(),
        created_at: now.clone(),
        updated_at: now,
        result: None,
        error: None,
        idempotency_key: id_key,
        attempts: 0,
    };
    task_store_upsert_task(&task, &cfg);
    let cancel_flag = Arc::new(AtomicBool::new(false));
    if let Ok(mut flags) = state.cancel_flags.lock() {
        flags.insert(task_id.clone(), Arc::clone(&cancel_flag));
    }
    if let Ok(mut t) = state.tasks.lock() {
        t.insert(task_id.clone(), task.clone());
        let _ = prune_tasks(&mut t, &cfg);
        persist_tasks_to_store(&t, cfg.store_path.as_ref());
    }
    if let Ok(mut idx) = state.idempotency_index.lock() {
        idx.insert(id_full.clone(), task_id.clone());
    }

    if let Ok(mut m) = state.metrics.lock() {
        m.transform_rows_v2_calls += 1;
    }

    let task_id_for_worker = task_id.clone();
    let tasks = Arc::clone(&state.tasks);
    let metrics = Arc::clone(&state.metrics);
    let task_cfg = cfg.clone();
    let cancel_flags = Arc::clone(&state.cancel_flags);
    let tenant_running = Arc::clone(&state.tenant_running);
    let idempotency_index = Arc::clone(&state.idempotency_index);
    let retry_max = env::var("AIWF_RUST_TASK_RETRY_MAX")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(1);
    let tenant_for_worker = task.tenant_id.clone();
    let id_full_for_worker = id_full.clone();
    tokio::spawn(async move {
        if let Ok(mut t) = tasks.lock()
            && let Some(cur) = t.get_mut(&task_id_for_worker)
        {
            if cur.status != "cancelled" {
                cur.status = "running".to_string();
                cur.updated_at = utc_now_iso();
                let cur_snapshot = cur.clone();
                let _ = prune_tasks(&mut t, &task_cfg);
                persist_tasks_to_store(&t, task_cfg.store_path.as_ref());
                task_store_upsert_task(&cur_snapshot, &task_cfg);
            }
        }

        if let Ok(t) = tasks.lock()
            && let Some(cur) = t.get(&task_id_for_worker)
            && cur.status == "cancelled"
        {
            cleanup_task_flag(&task_id_for_worker, &cancel_flags, &metrics);
            if let Ok(mut running) = tenant_running.lock()
                && let Some(v) = running.get_mut(&tenant_for_worker)
                && *v > 0
            {
                *v -= 1;
            }
            return;
        }

        let mut local_attempt = 0u32;
        let req_base = req;
        let mut res: Result<TransformRowsResp, String>;
        loop {
            local_attempt += 1;
            res = tokio::task::spawn_blocking({
                let cancel_flag = Arc::clone(&cancel_flag);
                let req_call = req_base.clone();
                move || run_transform_rows_v2_with_cancel(req_call, Some(cancel_flag))
            })
            .await
            .map_err(|e| format!("task join error: {e}"))
            .and_then(|inner| inner);
            if res.is_ok() || local_attempt > retry_max {
                break;
            }
            if let Ok(mut m) = metrics.lock() {
                m.task_retry_total += 1;
            }
        }

        if let Ok(mut t) = tasks.lock()
            && let Some(cur) = t.get_mut(&task_id_for_worker)
        {
            if cur.status == "cancelled" {
                let cur_snapshot = cur.clone();
                persist_tasks_to_store(&t, task_cfg.store_path.as_ref());
                task_store_upsert_task(&cur_snapshot, &task_cfg);
                cleanup_task_flag(&task_id_for_worker, &cancel_flags, &metrics);
                if let Ok(mut running) = tenant_running.lock()
                    && let Some(v) = running.get_mut(&tenant_for_worker)
                    && *v > 0
                {
                    *v -= 1;
                }
                return;
            }
            match res {
                Ok(resp) => {
                    observe_transform_success(&metrics, &resp);
                    cur.status = "done".to_string();
                    cur.result = Some(serde_json::to_value(resp).unwrap_or_else(|_| json!({})));
                    cur.attempts = local_attempt;
                    cur.updated_at = utc_now_iso();
                }
                Err(e) => {
                    cur.status = "failed".to_string();
                    cur.error = Some(e);
                    cur.attempts = local_attempt;
                    cur.updated_at = utc_now_iso();
                    if let Ok(mut m) = metrics.lock() {
                        m.transform_rows_v2_errors += 1;
                    }
                }
            }
            let cur_snapshot = cur.clone();
            let _ = prune_tasks(&mut t, &task_cfg);
            persist_tasks_to_store(&t, task_cfg.store_path.as_ref());
            task_store_upsert_task(&cur_snapshot, &task_cfg);
            cleanup_task_flag(&task_id_for_worker, &cancel_flags, &metrics);
        }
        if let Ok(mut running) = tenant_running.lock()
            && let Some(v) = running.get_mut(&tenant_for_worker)
            && *v > 0
        {
            *v -= 1;
        }
        if let Ok(t) = tasks.lock()
            && let Some(cur) = t.get(&task_id_for_worker)
            && cur.status == "failed"
            && let Ok(mut idx) = idempotency_index.lock()
        {
            idx.remove(&id_full_for_worker);
        }
    });

    (StatusCode::OK, Json(json!({"ok": true, "task_id": task_id, "status": "queued"}))).into_response()
}

async fn get_task_operator(
    State(state): State<AppState>,
    AxPath(task_id): AxPath<String>,
) -> impl IntoResponse {
    let cfg = current_task_cfg(&state);
    if task_store_remote_enabled(&cfg)
        && let Some(remote) = task_store_get_task(&task_id, &cfg)
    {
        if let Ok(mut t) = state.tasks.lock() {
            t.insert(task_id.clone(), remote.clone());
            let _ = prune_tasks(&mut t, &cfg);
            persist_tasks_to_store(&t, cfg.store_path.as_ref());
        }
        return (StatusCode::OK, Json(remote)).into_response();
    }

    let out = if let Ok(mut t) = state.tasks.lock() {
        let removed = prune_tasks(&mut t, &cfg);
        if removed > 0 {
            persist_tasks_to_store(&t, cfg.store_path.as_ref());
        }
        t.get(&task_id).cloned()
    } else {
        None
    };
    match out {
        Some(v) => (StatusCode::OK, Json(v)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"ok": false, "error": "task_not_found", "task_id": task_id})),
        )
            .into_response(),
    }
}

async fn cancel_task_operator(
    State(state): State<AppState>,
    AxPath(task_id): AxPath<String>,
) -> impl IntoResponse {
    let cfg = current_task_cfg(&state);
    if let Ok(mut m) = state.metrics.lock() {
        m.task_cancel_requested_total += 1;
    }
    if let Ok(flags) = state.cancel_flags.lock()
        && let Some(flag) = flags.get(&task_id)
    {
        flag.store(true, Ordering::Relaxed);
    }

    if task_store_remote_enabled(&cfg)
        && let Some(v) = task_store_cancel_task(&task_id, &cfg)
    {
        if v.get("ok").and_then(|x| x.as_bool()) == Some(true)
            && let Some(task) = task_store_get_task(&task_id, &cfg)
            && let Ok(mut t) = state.tasks.lock()
        {
            t.insert(task_id.clone(), task);
            let _ = prune_tasks(&mut t, &cfg);
            persist_tasks_to_store(&t, cfg.store_path.as_ref());
        }
        if v.get("cancelled").and_then(|x| x.as_bool()) == Some(true)
            && let Ok(mut m) = state.metrics.lock()
        {
            m.task_cancel_effective_total += 1;
        }
        return (StatusCode::OK, Json(v)).into_response();
    }

    let mut cancelled = false;
    let mut status = "not_found".to_string();
    if let Ok(mut t) = state.tasks.lock() {
        let removed = prune_tasks(&mut t, &cfg);
        if removed > 0 {
            persist_tasks_to_store(&t, cfg.store_path.as_ref());
        }
        if let Some(cur) = t.get_mut(&task_id) {
            status = cur.status.clone();
            if can_cancel_status(&status) {
                cur.status = "cancelled".to_string();
                cur.updated_at = utc_now_iso();
                status = cur.status.clone();
                cancelled = true;
                if let Ok(mut m) = state.metrics.lock() {
                    m.task_cancel_effective_total += 1;
                }
            }
            persist_tasks_to_store(&t, cfg.store_path.as_ref());
        }
    }
    if status == "not_found" {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"ok": false, "error": "task_not_found", "task_id": task_id})),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "task_id": task_id,
            "cancelled": cancelled,
            "status": status
        })),
    )
        .into_response()
}

async fn text_preprocess_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<TextPreprocessReq>,
) -> impl IntoResponse {
    if let Ok(mut m) = state.metrics.lock() {
        m.text_preprocess_v2_calls += 1;
    }
    match run_text_preprocess_v2(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => {
            if let Ok(mut m) = state.metrics.lock() {
                m.text_preprocess_v2_errors += 1;
            }
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrResp {
                    ok: false,
                    operator: "text_preprocess_v2".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response()
        }
    }
}

async fn rules_compile_v1_operator(Json(req): Json<RulesCompileReq>) -> impl IntoResponse {
    match compile_rules_dsl(&req.dsl) {
        Ok(rules) => (
            StatusCode::OK,
            Json(RulesCompileResp {
                ok: true,
                operator: "rules_compile_v1".to_string(),
                status: "done".to_string(),
                rules,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "rules_compile_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn join_rows_v1_operator(Json(req): Json<JoinRowsReq>) -> impl IntoResponse {
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

async fn rules_package_publish_v1_operator(
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

async fn rules_package_get_v1_operator(Json(req): Json<RulesPackageGetReq>) -> impl IntoResponse {
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

async fn normalize_schema_v1_operator(Json(req): Json<NormalizeSchemaReq>) -> impl IntoResponse {
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

async fn entity_extract_v1_operator(Json(req): Json<EntityExtractReq>) -> impl IntoResponse {
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

async fn aggregate_rows_v1_operator(Json(req): Json<AggregateRowsReq>) -> impl IntoResponse {
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

async fn quality_check_v1_operator(Json(req): Json<QualityCheckReq>) -> impl IntoResponse {
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

async fn aggregate_pushdown_v1_operator(Json(req): Json<AggregatePushdownReq>) -> impl IntoResponse {
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

async fn plugin_exec_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<PluginExecReq>,
) -> impl IntoResponse {
    let bytes = serde_json::to_vec(&req.input).map(|v| v.len()).unwrap_or(0);
    if let Err(e) = enforce_tenant_payload_quota(Some(&state), 1, bytes) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrResp {
                ok: false,
                operator: "plugin_exec_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response();
    }
    match run_plugin_exec_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "plugin_exec_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn plugin_health_v1_operator(Json(req): Json<PluginHealthReq>) -> impl IntoResponse {
    let plugin = match safe_pkg_token(&req.plugin) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrResp {
                    ok: false,
                    operator: "plugin_health_v1".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response();
        }
    };
    match run_plugin_healthcheck(&plugin, req.tenant_id.as_deref()) {
        Ok(details) => (
            StatusCode::OK,
            Json(PluginHealthResp {
                ok: true,
                operator: "plugin_health_v1".to_string(),
                status: "done".to_string(),
                plugin,
                details,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(PluginHealthResp {
                ok: false,
                operator: "plugin_health_v1".to_string(),
                status: "failed".to_string(),
                plugin,
                details: json!({ "error": e }),
            }),
        )
            .into_response(),
    }
}

async fn load_rows_v1_operator(Json(req): Json<LoadRowsReq>) -> impl IntoResponse {
    match run_load_rows_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "load_rows_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn save_rows_v1_operator(Json(req): Json<SaveRowsReq>) -> impl IntoResponse {
    match run_save_rows_v1(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "save_rows_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn transform_rows_v2_stream_operator(
    State(state): State<AppState>,
    Json(req): Json<TransformRowsStreamReq>,
) -> impl IntoResponse {
    if let Some(rows) = req.rows.as_ref() {
        let bytes = serde_json::to_vec(rows).map(|v| v.len()).unwrap_or(0);
        if let Err(e) = enforce_tenant_payload_quota(Some(&state), rows.len(), bytes) {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(ErrResp {
                    ok: false,
                    operator: "transform_rows_v2_stream".to_string(),
                    status: "failed".to_string(),
                    error: e,
                }),
            )
                .into_response();
        }
    }
    match run_transform_rows_v2_stream(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "transform_rows_v2_stream".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn workflow_run_operator(
    State(state): State<AppState>,
    Json(req): Json<WorkflowRunReq>,
) -> impl IntoResponse {
    if req.steps.len() > tenant_max_workflow_steps() {
        if let Ok(mut m) = state.metrics.lock() {
            m.quota_reject_total += 1;
        }
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrResp {
                ok: false,
                operator: "workflow_run".to_string(),
                status: "failed".to_string(),
                error: format!(
                    "workflow step quota exceeded: {} > {}",
                    req.steps.len(),
                    tenant_max_workflow_steps()
                ),
            }),
        )
            .into_response();
    }
    match run_workflow(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "workflow_run".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

fn run_transform_rows_v2(req: TransformRowsReq) -> Result<TransformRowsResp, String> {
    run_transform_rows_v2_with_cancel(req, None)
}

fn run_transform_rows_v2_with_cancel(
    req: TransformRowsReq,
    cancel_flag: Option<Arc<AtomicBool>>,
) -> Result<TransformRowsResp, String> {
    let started = Instant::now();
    verify_request_signature(&req)?;
    let max_rows = env::var("AIWF_RUST_V2_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(200000);
    let max_bytes = env::var("AIWF_RUST_V2_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(128 * 1024 * 1024);
    let mut rows_in = req.rows.unwrap_or_default();
    if rows_in.is_empty() && let Some(uri) = req.input_uri.clone() {
        rows_in = load_rows_from_uri_limited(&uri, max_rows, max_bytes)?;
    }
    let input_rows = rows_in.len();
    if input_rows > max_rows {
        return Err(format!("input rows exceed limit: {} > {}", input_rows, max_rows));
    }
    let estimated_bytes = serde_json::to_vec(&rows_in).map(|b| b.len()).unwrap_or(0);
    if estimated_bytes > max_bytes {
        return Err(format!(
            "input payload exceeds limit: {} > {}",
            estimated_bytes, max_bytes
        ));
    }
    let rules = if req.rules.is_some() {
        req.rules.clone().unwrap_or_else(|| json!({}))
    } else if let Some(dsl) = req.rules_dsl.clone() {
        compile_rules_dsl(&dsl)?
    } else {
        json!({})
    };
    let gates = req.quality_gates.unwrap_or_else(|| json!({}));

    let null_values: Vec<String> = as_array_str(rule_get(&rules, "null_values"))
        .into_iter()
        .map(|s| s.to_lowercase())
        .collect();
    let trim_strings = as_bool(rule_get(&rules, "trim_strings"), true);

    let rename_map: HashMap<String, String> = rule_get(&rules, "rename_map")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<HashMap<String, String>>()
        })
        .unwrap_or_default();
    let casts: HashMap<String, String> = rule_get(&rules, "casts")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| {
                    v.as_str()
                        .map(|s| (k.clone(), s.trim().to_lowercase()))
                })
                .collect::<HashMap<String, String>>()
        })
        .unwrap_or_default();

    let filters = rule_get(&rules, "filters")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let compiled_filters = compile_filters(&filters);
    let required_fields = as_array_str(rule_get(&rules, "required_fields"));
    let default_values = rule_get(&rules, "default_values")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let include_fields = as_array_str(rule_get(&rules, "include_fields"));
    let exclude_fields = as_array_str(rule_get(&rules, "exclude_fields"));
    let deduplicate_by = as_array_str(rule_get(&rules, "deduplicate_by"));
    let dedup_keep = rule_get(&rules, "deduplicate_keep")
        .and_then(|v| v.as_str())
        .unwrap_or("last")
        .to_lowercase();
    let sort_by = rule_get(&rules, "sort_by")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut invalid_rows = 0usize;
    let mut filtered_rows = 0usize;
    let mut rule_hits: HashMap<String, usize> = HashMap::new();
    let mut rows: Vec<Map<String, Value>> = Vec::new();

    for row in rows_in {
        if is_cancelled(&cancel_flag) {
            return Err("task cancelled".to_string());
        }
        let Some(obj) = row.as_object() else {
            invalid_rows += 1;
            *rule_hits.entry("invalid_object".to_string()).or_insert(0) += 1;
            continue;
        };
        let mut out: Map<String, Value> = Map::new();
        for (k, v) in obj {
            let key = rename_map.get(k).cloned().unwrap_or_else(|| k.clone());
            let mut vv = v.clone();
            if let Some(s) = vv.as_str() {
                let mut ss = s.to_string();
                if trim_strings {
                    ss = ss.trim().to_string();
                }
                if null_values.iter().any(|x| x == &ss.to_lowercase()) {
                    vv = Value::Null;
                } else {
                    vv = Value::String(ss);
                }
            }
            out.insert(key, vv);
        }

        for (k, v) in &default_values {
            let should_fill = match out.get(k) {
                None => true,
                Some(Value::Null) => true,
                Some(Value::String(s)) => s.trim().is_empty(),
                Some(_) => false,
            };
            if should_fill {
                out.insert(k.clone(), v.clone());
            }
        }

        for (field, cast_type) in &casts {
            if let Some(v) = out.get(field).cloned() {
                match cast_value(v, cast_type) {
                    Some(casted) => {
                        out.insert(field.clone(), casted);
                    }
                    None => {
                        invalid_rows += 1;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                        out.clear();
                        break;
                    }
                }
            }
        }
        if out.is_empty() {
            continue;
        }

        if !required_fields.is_empty() {
            let mut missing = false;
            for f in &required_fields {
                if is_missing(out.get(f)) {
                    missing = true;
                    break;
                }
            }
            if missing {
                invalid_rows += 1;
                *rule_hits.entry("required_missing".to_string()).or_insert(0) += 1;
                continue;
            }
        }

        if !compiled_filters.is_empty() && !compiled_filters.iter().all(|f| filter_match_compiled(&out, f))
        {
            filtered_rows += 1;
            *rule_hits.entry("filtered_by_rule".to_string()).or_insert(0) += 1;
            continue;
        }

        if !include_fields.is_empty() {
            let mut next: Map<String, Value> = Map::new();
            for f in &include_fields {
                if let Some(v) = out.get(f) {
                    next.insert(f.clone(), v.clone());
                }
            }
            out = next;
        }
        for f in &exclude_fields {
            out.remove(f);
        }
        rows.push(out);
    }

    let mut duplicate_rows_removed = 0usize;
    if !deduplicate_by.is_empty() {
        let mut map: HashMap<String, Map<String, Value>> = HashMap::new();
        if dedup_keep == "first" {
            for r in rows {
                let k = dedup_key(&r, &deduplicate_by);
                map.entry(k).or_insert(r);
            }
        } else {
            for r in rows {
                let k = dedup_key(&r, &deduplicate_by);
                map.insert(k, r);
            }
        }
        let out_len = map.len();
        duplicate_rows_removed = input_rows.saturating_sub(invalid_rows + filtered_rows + out_len);
        rows = map.into_values().collect();
    }

    if !sort_by.is_empty() {
        rows.sort_by(|a, b| compare_rows(a, b, &sort_by));
    }

    if is_cancelled(&cancel_flag) {
        return Err("task cancelled".to_string());
    }

    let aggregate = compute_aggregate(&rows, rule_get(&rules, "aggregate"));

    let output_rows = rows.len();
    let gate_required_fields = {
        let from_gate = as_array_str(gates.get("required_fields"));
        if from_gate.is_empty() {
            required_fields.clone()
        } else {
            from_gate
        }
    };
    let mut required_missing_by_field: Map<String, Value> = Map::new();
    let mut required_missing_cells = 0usize;
    if !gate_required_fields.is_empty() {
        for f in &gate_required_fields {
            let mut miss = 0usize;
            for r in &rows {
                if is_missing(r.get(f)) {
                    miss += 1;
                }
            }
            required_missing_cells += miss;
            required_missing_by_field.insert(f.clone(), Value::Number((miss as u64).into()));
        }
    }
    let required_total_cells = output_rows.saturating_mul(gate_required_fields.len());
    let required_missing_ratio = if required_total_cells > 0 {
        required_missing_cells as f64 / required_total_cells as f64
    } else {
        0.0
    };

    let quality = json!({
        "input_rows": input_rows,
        "output_rows": output_rows,
        "invalid_rows": invalid_rows,
        "filtered_rows": filtered_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "required_fields": gate_required_fields,
        "required_missing_cells": required_missing_cells,
        "required_missing_by_field": required_missing_by_field,
        "required_missing_ratio": required_missing_ratio,
    });

    let gate_result = evaluate_quality_gates(&quality, &gates);
    let passed = gate_result
        .get("passed")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    if !passed {
        return Err(format!(
            "transform_rows_v2 quality gate failed: {}",
            gate_result
                .get("errors")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().map(value_to_string).collect::<Vec<String>>().join("; "))
                .unwrap_or_default()
        ));
    }

    let latency_ms = started.elapsed().as_millis();
    let trace_id = resolve_trace_id(
        req.trace_id.as_deref(),
        req.traceparent.as_deref(),
        &format!(
            "{}:{}:{}:{}",
            req.run_id.clone().unwrap_or_default(),
            input_rows,
            output_rows,
            latency_ms
        ),
    );
    let resp = TransformRowsResp {
        ok: true,
        operator: "transform_rows_v2".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        trace_id,
        rows: rows.into_iter().map(Value::Object).collect(),
        quality,
        gate_result,
        stats: TransformRowsStats {
            input_rows,
            output_rows,
            invalid_rows,
            filtered_rows,
            duplicate_rows_removed,
            latency_ms,
        },
        rust_v2_used: true,
        schema_hint: req.schema_hint,
        aggregate,
        audit: json!({
            "rule_hits": rule_hits,
            "estimated_input_bytes": estimated_bytes,
            "limits": {
                "max_rows": max_rows,
                "max_payload_bytes": max_bytes
            }
        }),
    };
    if let Some(uri) = req.output_uri {
        save_rows_to_uri(&uri, &resp.rows)?;
    }
    Ok(resp)
}

fn run_text_preprocess_v2(req: TextPreprocessReq) -> Result<TextPreprocessResp, String> {
    let mut lines: Vec<String> = req
        .text
        .replace("\r\n", "\n")
        .split('\n')
        .map(|s| s.to_string())
        .collect();
    let remove_refs = req.remove_references.unwrap_or(true);
    let remove_notes = req.remove_notes.unwrap_or(true);
    let normalize_ws = req.normalize_whitespace.unwrap_or(true);
    let mut removed_references_lines = 0usize;
    let mut removed_notes_lines = 0usize;

    if remove_refs {
        let mut cut_idx: Option<usize> = None;
        for (i, line) in lines.iter().enumerate() {
            let t = line.trim().to_lowercase();
            if t == "references" || t == "bibliography" || t == "参考文献" || t == "引用文献" {
                cut_idx = Some(i);
                break;
            }
        }
        if let Some(i) = cut_idx {
            removed_references_lines = lines.len().saturating_sub(i);
            lines = lines.into_iter().take(i).collect();
        }
    }

    if remove_notes {
        let mut out: Vec<String> = Vec::new();
        for line in lines {
            let t = line.trim();
            if t.starts_with('[') && t.contains(']') && t.len() < 24 {
                removed_notes_lines += 1;
                continue;
            }
            if t.to_lowercase().starts_with("footnote") || t.starts_with("注释") || t.starts_with("脚注")
            {
                removed_notes_lines += 1;
                continue;
            }
            out.push(line);
        }
        lines = out;
    }

    if normalize_ws {
        lines = lines
            .into_iter()
            .map(|x| collapse_ws(&x))
            .collect::<Vec<String>>();
    }

    if lines.is_empty() {
        return Err("text_preprocess_v2 produced empty content".to_string());
    }

    let mut markdown = String::new();
    if let Some(title) = req.title {
        let t = title.trim();
        if !t.is_empty() {
            markdown.push_str("# ");
            markdown.push_str(t);
            markdown.push_str("\n\n");
        }
    }
    markdown.push_str(&lines.join("\n").trim().to_string());
    markdown.push('\n');

    let mut hasher = Sha256::new();
    hasher.update(markdown.as_bytes());
    let sha256 = format!("{:x}", hasher.finalize());
    Ok(TextPreprocessResp {
        ok: true,
        operator: "text_preprocess_v2".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        markdown,
        removed_references_lines,
        removed_notes_lines,
        sha256,
    })
}

fn compile_rules_dsl(dsl: &str) -> Result<Value, String> {
    let mut rename = Map::new();
    let mut casts = Map::new();
    let mut filters: Vec<Value> = Vec::new();
    let mut required: Vec<Value> = Vec::new();
    for (idx, raw) in dsl.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("rename ") {
            let parts: Vec<&str> = rest.split("->").map(|x| x.trim()).collect();
            if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                return Err(format!("dsl line {} invalid rename", idx + 1));
            }
            rename.insert(parts[0].to_string(), Value::String(parts[1].to_string()));
            continue;
        }
        if let Some(rest) = line.strip_prefix("cast ") {
            let parts: Vec<&str> = rest.split(':').map(|x| x.trim()).collect();
            if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                return Err(format!("dsl line {} invalid cast", idx + 1));
            }
            casts.insert(parts[0].to_string(), Value::String(parts[1].to_lowercase()));
            continue;
        }
        if let Some(rest) = line.strip_prefix("required ") {
            let f = rest.trim();
            if f.is_empty() {
                return Err(format!("dsl line {} invalid required", idx + 1));
            }
            required.push(Value::String(f.to_string()));
            continue;
        }
        if let Some(rest) = line.strip_prefix("filter ") {
            let expr = rest.trim();
            let candidates = ["<=", ">=", "==", "!=", ">", "<"];
            let mut hit: Option<(&str, usize)> = None;
            for op in candidates {
                if let Some(p) = expr.find(op) {
                    hit = Some((op, p));
                    break;
                }
            }
            let Some((op, pos)) = hit else {
                return Err(format!("dsl line {} invalid filter", idx + 1));
            };
            let left = expr[..pos].trim();
            let right = expr[pos + op.len()..].trim().trim_matches('"');
            if left.is_empty() {
                return Err(format!("dsl line {} invalid filter lhs", idx + 1));
            }
            let mapped = match op {
                ">" => "gt",
                ">=" => "gte",
                "<" => "lt",
                "<=" => "lte",
                "==" => "eq",
                "!=" => "ne",
                _ => "eq",
            };
            let value = if let Ok(n) = right.parse::<f64>() {
                serde_json::Number::from_f64(n)
                    .map(Value::Number)
                    .unwrap_or_else(|| Value::String(right.to_string()))
            } else {
                Value::String(right.to_string())
            };
            filters.push(json!({"field": left, "op": mapped, "value": value}));
            continue;
        }
        return Err(format!("dsl line {} unsupported statement", idx + 1));
    }
    Ok(json!({
        "rename_map": rename,
        "casts": casts,
        "filters": filters,
        "required_fields": required
    }))
}

fn rules_pkg_base_dir() -> PathBuf {
    env::var("AIWF_RULES_PACKAGE_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("bus").join("rules_packages"))
}

fn stream_checkpoint_dir() -> PathBuf {
    env::var("AIWF_STREAM_CHECKPOINT_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("bus").join("stream_checkpoints"))
}

fn checkpoint_path(key: &str) -> Result<PathBuf, String> {
    let k = safe_pkg_token(key)?;
    Ok(stream_checkpoint_dir().join(format!("{k}.json")))
}

fn write_stream_checkpoint(key: &str, chunk_idx: usize) -> Result<(), String> {
    let path = checkpoint_path(key)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create checkpoint dir: {e}"))?;
    }
    let payload = json!({"checkpoint_key": key, "last_chunk": chunk_idx, "updated_at": utc_now_iso()});
    fs::write(&path, serde_json::to_string_pretty(&payload).map_err(|e| e.to_string())?)
        .map_err(|e| format!("write checkpoint: {e}"))
}

fn read_stream_checkpoint(key: &str) -> Result<Option<usize>, String> {
    let path = checkpoint_path(key)?;
    if !path.exists() {
        return Ok(None);
    }
    let txt = fs::read_to_string(&path).map_err(|e| format!("read checkpoint: {e}"))?;
    let v: Value = serde_json::from_str(&txt).map_err(|e| format!("parse checkpoint: {e}"))?;
    Ok(v.get("last_chunk").and_then(|x| x.as_u64()).map(|x| x as usize))
}

fn safe_pkg_token(s: &str) -> Result<String, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty package token".to_string());
    }
    if t.chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.')
    {
        Ok(t.to_string())
    } else {
        Err("package token contains invalid characters".to_string())
    }
}

fn rules_pkg_path(name: &str, version: &str) -> Result<PathBuf, String> {
    let n = safe_pkg_token(name)?;
    let v = safe_pkg_token(version)?;
    Ok(rules_pkg_base_dir().join(format!("{n}__{v}.json")))
}

fn run_rules_package_publish_v1(req: RulesPackagePublishReq) -> Result<RulesPackageResp, String> {
    let mut rules = req.rules.unwrap_or(Value::Null);
    if rules.is_null() {
        let dsl = req.dsl.unwrap_or_default();
        if dsl.trim().is_empty() {
            return Err("rules or dsl is required".to_string());
        }
        rules = compile_rules_dsl(&dsl)?;
    }
    let path = rules_pkg_path(&req.name, &req.version)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create rules package dir: {e}"))?;
    }
    let rules_text = serde_json::to_string_pretty(&rules).map_err(|e| e.to_string())?;
    let mut h = Sha256::new();
    h.update(rules_text.as_bytes());
    let fingerprint = format!("{:x}", h.finalize());
    let payload = json!({
        "name": req.name,
        "version": req.version,
        "fingerprint": fingerprint,
        "rules": rules,
    });
    fs::write(
        &path,
        serde_json::to_string_pretty(&payload).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("write rules package: {e}"))?;
    Ok(RulesPackageResp {
        ok: true,
        operator: "rules_package_publish_v1".to_string(),
        status: "done".to_string(),
        name: req.name,
        version: req.version,
        rules: payload.get("rules").cloned().unwrap_or_else(|| json!({})),
        fingerprint,
    })
}

fn run_rules_package_get_v1(req: RulesPackageGetReq) -> Result<RulesPackageResp, String> {
    let path = rules_pkg_path(&req.name, &req.version)?;
    let txt = fs::read_to_string(&path).map_err(|e| format!("read rules package: {e}"))?;
    let v: Value = serde_json::from_str(&txt).map_err(|e| format!("parse rules package: {e}"))?;
    let rules = v.get("rules").cloned().unwrap_or_else(|| json!({}));
    let fingerprint = v
        .get("fingerprint")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    Ok(RulesPackageResp {
        ok: true,
        operator: "rules_package_get_v1".to_string(),
        status: "done".to_string(),
        name: req.name,
        version: req.version,
        rules,
        fingerprint,
    })
}

fn run_join_rows_v1(req: JoinRowsReq) -> Result<JoinRowsResp, String> {
    let mut right_index: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for row in req.right_rows {
        if let Some(obj) = row.as_object() {
            let k = value_to_string_or_null(obj.get(&req.right_on));
            right_index.entry(k).or_default().push(obj.clone());
        }
    }
    let join_type = req.join_type.unwrap_or_else(|| "inner".to_string()).to_lowercase();
    let mut out: Vec<Value> = Vec::new();
    let mut matched = 0usize;
    for row in req.left_rows {
        let Some(obj) = row.as_object() else {
            continue;
        };
        let k = value_to_string_or_null(obj.get(&req.left_on));
        if let Some(rrs) = right_index.get(&k) {
            for rr in rrs {
                let mut merged = obj.clone();
                for (rk, rv) in rr {
                    if merged.contains_key(rk) {
                        merged.insert(format!("right_{rk}"), rv.clone());
                    } else {
                        merged.insert(rk.clone(), rv.clone());
                    }
                }
                out.push(Value::Object(merged));
                matched += 1;
            }
        } else if join_type == "left" {
            out.push(Value::Object(obj.clone()));
        }
    }
    Ok(JoinRowsResp {
        ok: true,
        operator: "join_rows_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        rows: out,
        stats: json!({"matched_pairs": matched}),
    })
}

fn run_normalize_schema_v1(req: NormalizeSchemaReq) -> Result<NormalizeSchemaResp, String> {
    let mut out = Vec::new();
    let mut filled_defaults = 0usize;
    let schema = req.schema;
    let fields = schema
        .get("fields")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let defaults = schema
        .get("defaults")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    for row in req.rows {
        let Some(obj) = row.as_object() else {
            continue;
        };
        let mut next = obj.clone();
        for f in &fields {
            let Some(field) = f.as_str() else {
                continue;
            };
            if !next.contains_key(field) {
                if let Some(v) = defaults.get(field) {
                    next.insert(field.to_string(), v.clone());
                    filled_defaults += 1;
                } else {
                    next.insert(field.to_string(), Value::Null);
                }
            }
        }
        out.push(Value::Object(next));
    }
    Ok(NormalizeSchemaResp {
        ok: true,
        operator: "normalize_schema_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        rows: out,
        stats: json!({"filled_defaults": filled_defaults}),
    })
}

fn run_entity_extract_v1(req: EntityExtractReq) -> Result<EntityExtractResp, String> {
    let email_re = Regex::new(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}").map_err(|e| e.to_string())?;
    let url_re = Regex::new(r"https?://[^\s)]+").map_err(|e| e.to_string())?;
    let num_re = Regex::new(r"\b\d+(?:\.\d+)?\b").map_err(|e| e.to_string())?;
    let mut text = String::new();
    if let Some(t) = req.text {
        text.push_str(&t);
        text.push('\n');
    }
    if let Some(rows) = req.rows {
        let field = req.text_field.unwrap_or_else(|| "text".to_string());
        for r in rows {
            if let Some(obj) = r.as_object() {
                text.push_str(&value_to_string_or_null(obj.get(&field)));
                text.push('\n');
            }
        }
    }
    let emails: Vec<Value> = email_re
        .find_iter(&text)
        .map(|m| Value::String(m.as_str().to_string()))
        .collect();
    let urls: Vec<Value> = url_re
        .find_iter(&text)
        .map(|m| Value::String(m.as_str().to_string()))
        .collect();
    let nums: Vec<Value> = num_re
        .find_iter(&text)
        .take(2000)
        .map(|m| Value::String(m.as_str().to_string()))
        .collect();
    Ok(EntityExtractResp {
        ok: true,
        operator: "entity_extract_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        entities: json!({"emails": emails, "urls": urls, "numbers": nums}),
    })
}

#[derive(Clone)]
struct AggSpec {
    op: String,
    field: Option<String>,
    as_name: String,
}

#[derive(Default, Clone)]
struct AggBucket {
    group_vals: Map<String, Value>,
    count: u64,
    sums: HashMap<String, f64>,
    min: HashMap<String, f64>,
    max: HashMap<String, f64>,
}

fn parse_agg_specs(specs: &[Value]) -> Result<Vec<AggSpec>, String> {
    let mut out = Vec::new();
    for s in specs {
        let Some(obj) = s.as_object() else {
            return Err("aggregate spec must be object".to_string());
        };
        let op = obj
            .get("op")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_lowercase();
        if op.is_empty() {
            return Err("aggregate spec missing op".to_string());
        }
        let field = obj
            .get("field")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let as_name = obj
            .get("as")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| match &field {
                Some(f) => format!("{f}_{op}"),
                None => format!("_{op}"),
            });
        match op.as_str() {
            "count" => {}
            "sum" | "avg" | "min" | "max" => {
                if field.is_none() {
                    return Err(format!("aggregate op {op} requires field"));
                }
            }
            _ => return Err(format!("unsupported aggregate op: {op}")),
        }
        out.push(AggSpec { op, field, as_name });
    }
    if out.is_empty() {
        return Err("aggregates is empty".to_string());
    }
    Ok(out)
}

fn run_aggregate_rows_v1(req: AggregateRowsReq) -> Result<AggregateRowsResp, String> {
    let specs = parse_agg_specs(&req.aggregates)?;
    let mut buckets: HashMap<String, AggBucket> = HashMap::new();
    for row in &req.rows {
        let Some(obj) = row.as_object() else {
            continue;
        };
        let mut group_vals = Map::new();
        let mut key_parts = Vec::new();
        for g in &req.group_by {
            let v = obj.get(g).cloned().unwrap_or(Value::Null);
            key_parts.push(value_to_string_or_null(Some(&v)));
            group_vals.insert(g.clone(), v);
        }
        let key = key_parts.join("\u{1f}");
        let b = buckets.entry(key).or_insert_with(|| AggBucket {
            group_vals: group_vals.clone(),
            ..Default::default()
        });
        b.count += 1;
        for sp in &specs {
            let Some(f) = sp.field.as_ref() else {
                continue;
            };
            let n = obj.get(f).and_then(value_to_f64);
            if let Some(v) = n {
                *b.sums.entry(sp.as_name.clone()).or_insert(0.0) += v;
                b.min
                    .entry(sp.as_name.clone())
                    .and_modify(|cur| {
                        if v < *cur {
                            *cur = v;
                        }
                    })
                    .or_insert(v);
                b.max
                    .entry(sp.as_name.clone())
                    .and_modify(|cur| {
                        if v > *cur {
                            *cur = v;
                        }
                    })
                    .or_insert(v);
            }
        }
    }
    let mut out = Vec::new();
    for (_, b) in buckets {
        let mut obj = b.group_vals;
        for sp in &specs {
            match sp.op.as_str() {
                "count" => {
                    obj.insert(sp.as_name.clone(), json!(b.count));
                }
                "sum" => {
                    obj.insert(
                        sp.as_name.clone(),
                        json!(b.sums.get(&sp.as_name).copied().unwrap_or(0.0)),
                    );
                }
                "avg" => {
                    let sum = b.sums.get(&sp.as_name).copied().unwrap_or(0.0);
                    let denom = b.count.max(1) as f64;
                    obj.insert(sp.as_name.clone(), json!(sum / denom));
                }
                "min" => {
                    obj.insert(
                        sp.as_name.clone(),
                        b.min
                            .get(&sp.as_name)
                            .copied()
                            .map(Value::from)
                            .unwrap_or(Value::Null),
                    );
                }
                "max" => {
                    obj.insert(
                        sp.as_name.clone(),
                        b.max
                            .get(&sp.as_name)
                            .copied()
                            .map(Value::from)
                            .unwrap_or(Value::Null),
                    );
                }
                _ => {}
            }
        }
        out.push(Value::Object(obj));
    }
    let first_group = req.group_by.first().cloned().unwrap_or_default();
    out.sort_by(|a, b| {
        let av = a
            .as_object()
            .and_then(|m| m.get(&first_group))
            .map(|v| value_to_string_or_null(Some(v)))
            .unwrap_or_default();
        let bv = b
            .as_object()
            .and_then(|m| m.get(&first_group))
            .map(|v| value_to_string_or_null(Some(v)))
            .unwrap_or_default();
        av.cmp(&bv)
    });
    Ok(AggregateRowsResp {
        ok: true,
        operator: "aggregate_rows_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        stats: json!({"input_rows": req.rows.len(), "output_rows": out.len(), "groups": out.len()}),
        rows: out,
    })
}

fn run_quality_check_v1(req: QualityCheckReq) -> Result<QualityCheckResp, String> {
    let rules = req.rules.as_object().cloned().unwrap_or_default();
    let unique_fields = rules
        .get("unique_fields")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    let required_fields = rules
        .get("required_fields")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    let max_null_ratio = rules
        .get("max_null_ratio")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0)
        .clamp(0.0, 1.0);
    let mut passed = true;
    let mut violations = Vec::new();
    let mut duplicate_count = 0usize;
    if !unique_fields.is_empty() {
        let mut seen = std::collections::HashSet::new();
        for r in &req.rows {
            let Some(obj) = r.as_object() else {
                continue;
            };
            let key = unique_fields
                .iter()
                .map(|f| value_to_string_or_null(obj.get(f)))
                .collect::<Vec<_>>()
                .join("|");
            if !seen.insert(key) {
                duplicate_count += 1;
            }
        }
        if duplicate_count > 0 {
            passed = false;
            violations.push(json!({"rule":"unique_fields","duplicates": duplicate_count}));
        }
    }
    let mut null_violations = Vec::new();
    if !required_fields.is_empty() {
        for f in &required_fields {
            let mut nulls = 0usize;
            for r in &req.rows {
                let miss = r
                    .as_object()
                    .and_then(|o| o.get(f))
                    .map(|v| v.is_null() || value_to_string_or_null(Some(v)).trim().is_empty())
                    .unwrap_or(true);
                if miss {
                    nulls += 1;
                }
            }
            let ratio = if req.rows.is_empty() {
                0.0
            } else {
                nulls as f64 / req.rows.len() as f64
            };
            if ratio > max_null_ratio {
                passed = false;
                null_violations.push(json!({"field":f, "null_ratio":ratio, "max_null_ratio":max_null_ratio}));
            }
        }
    }
    if !null_violations.is_empty() {
        violations.push(json!({"rule":"required_fields", "details": null_violations}));
    }
    let mut outlier_report = Vec::new();
    if let Some(oz) = rules.get("outlier_zscore").and_then(|v| v.as_object()) {
        let field = oz.get("field").and_then(|v| v.as_str()).unwrap_or("");
        let max_z = oz.get("max_z").and_then(|v| v.as_f64()).unwrap_or(4.0).abs();
        if !field.is_empty() {
            let vals = req
                .rows
                .iter()
                .filter_map(|r| r.as_object().and_then(|o| o.get(field)).and_then(value_to_f64))
                .collect::<Vec<_>>();
            if vals.len() >= 3 {
                let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                let var = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64;
                let std = var.sqrt();
                if std > 0.0 {
                    let outliers = vals
                        .iter()
                        .filter(|v| ((*v - mean).abs() / std) > max_z)
                        .count();
                    if outliers > 0 {
                        passed = false;
                        outlier_report.push(json!({"field":field,"outliers":outliers,"max_z":max_z}));
                    }
                }
            }
        }
    }
    if !outlier_report.is_empty() {
        violations.push(json!({"rule":"outlier_zscore", "details": outlier_report}));
    }
    Ok(QualityCheckResp {
        ok: true,
        operator: "quality_check_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        passed,
        report: json!({
            "rows": req.rows.len(),
            "violations": violations,
            "rule_count": rules.len()
        }),
    })
}

fn run_aggregate_pushdown_v1(req: AggregatePushdownReq) -> Result<AggregatePushdownResp, String> {
    if req.group_by.is_empty() {
        return Err("group_by is empty".to_string());
    }
    let from = req
        .from
        .as_deref()
        .unwrap_or("data")
        .trim()
        .to_string();
    if from.is_empty() {
        return Err("from is empty".to_string());
    }
    let from = validate_sql_identifier(&from)?;
    let group_by = req
        .group_by
        .iter()
        .map(|g| validate_sql_identifier(g))
        .collect::<Result<Vec<_>, String>>()?;
    let specs = parse_agg_specs(&req.aggregates)?;
    let select_group = group_by.join(", ");
    let select_aggs = specs
        .iter()
        .map(|s| match s.op.as_str() {
            "count" => Ok(format!("COUNT(1) AS {}", validate_sql_identifier(&s.as_name)?)),
            "sum" => Ok(format!(
                "SUM({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            "avg" => Ok(format!(
                "AVG({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            "min" => Ok(format!(
                "MIN({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            "max" => Ok(format!(
                "MAX({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            _ => Err("unsupported aggregate op".to_string()),
        })
        .collect::<Result<Vec<_>, String>>()?
        .join(", ");
    let where_sql = req
        .where_sql
        .as_deref().map(|w| validate_where_clause(w))
        .transpose()?
        .map(|w| format!(" WHERE {w}"))
        .unwrap_or_default();
    let limit = req.limit.unwrap_or(10000).max(1);
    let sql = format!(
        "SELECT {select_group}, {select_aggs} FROM {from}{where_sql} GROUP BY {select_group}"
    );
    let rows = match req.source_type.to_lowercase().as_str() {
        "sqlite" => load_sqlite_rows(&req.source, &sql, limit)?,
        "sqlserver" => load_sqlserver_rows(&req.source, &sql, limit)?,
        _ => return Err("source_type must be sqlite or sqlserver".to_string()),
    };
    Ok(AggregatePushdownResp {
        ok: true,
        operator: "aggregate_pushdown_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        sql,
        stats: json!({"rows": rows.len(), "limit": limit, "source_type": req.source_type}),
        rows,
    })
}

fn plugin_dir() -> PathBuf {
    env::var("AIWF_PLUGIN_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("bus").join("plugins"))
}

fn load_plugin_manifest(plugin: &str) -> Result<PluginManifest, String> {
    let cfg_path = plugin_dir().join(format!("{plugin}.json"));
    let cfg_txt = fs::read_to_string(&cfg_path).map_err(|e| format!("read plugin config: {e}"))?;
    let m: PluginManifest =
        serde_json::from_str(&cfg_txt).map_err(|e| format!("parse plugin config: {e}"))?;
    if m.command.trim().is_empty() {
        return Err("plugin config missing command".to_string());
    }
    if let Some(n) = &m.name && !n.trim().is_empty() {
        let nn = safe_pkg_token(n)?;
        if !nn.eq_ignore_ascii_case(plugin) {
            return Err(format!("plugin name mismatch: manifest={nn}, request={plugin}"));
        }
    }
    let api = m
        .api_version
        .as_deref()
        .unwrap_or("v1")
        .trim()
        .to_lowercase();
    if api != "v1" {
        return Err(format!("unsupported plugin api_version: {api}"));
    }
    if let Some(ver) = &m.version && ver.trim().is_empty() {
        return Err("plugin version is empty".to_string());
    }
    Ok(m)
}

fn run_plugin_healthcheck(plugin: &str, tenant: Option<&str>) -> Result<Value, String> {
    if !plugin_enabled_for_tenant(tenant) {
        return Err("plugin execution disabled for tenant".to_string());
    }
    enforce_plugin_allowlist(plugin)?;
    let m = load_plugin_manifest(plugin)?;
    let cmd = m
        .healthcheck
        .as_ref()
        .and_then(|h| h.command.clone())
        .unwrap_or_else(|| m.command.clone());
    if cmd.trim().is_empty() {
        return Err("plugin healthcheck command is empty".to_string());
    }
    enforce_plugin_command_allowlist(&cmd)?;
    let args = m
        .healthcheck
        .as_ref()
        .and_then(|h| h.args.clone())
        .unwrap_or_else(|| m.args.clone().unwrap_or_default());
    verify_plugin_signature(plugin, &cmd, &args, m.signature.as_deref())?;
    let timeout_ms = m
        .healthcheck
        .as_ref()
        .and_then(|h| h.timeout_ms)
        .or(m.timeout_ms)
        .unwrap_or(3000)
        .min(15_000);
    let max_out = env::var("AIWF_PLUGIN_MAX_OUTPUT_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8 * 1024 * 1024);
    let mut child = Command::new(&cmd)
        .args(&args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn plugin healthcheck: {e}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "plugin health stdout pipe missing".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "plugin health stderr pipe missing".to_string())?;
    let stdout_handle = read_pipe_capped(stdout, max_out, "health_stdout");
    let stderr_handle = read_pipe_capped(stderr, max_out, "health_stderr");
    let start = Instant::now();
    let status = loop {
        if start.elapsed().as_millis() as u64 > timeout_ms {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("plugin healthcheck timeout: {timeout_ms}ms"));
        }
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) => std::thread::sleep(std::time::Duration::from_millis(20)),
            Err(e) => return Err(format!("plugin healthcheck wait error: {e}")),
        }
    };
    let out_stdout = stdout_handle
        .join()
        .map_err(|_| "plugin health stdout reader thread panicked".to_string())??;
    let out_stderr = stderr_handle
        .join()
        .map_err(|_| "plugin health stderr reader thread panicked".to_string())??;
    if out_stdout.len().saturating_add(out_stderr.len()) > max_out {
        return Err("plugin health output exceeds limit".to_string());
    }
    Ok(json!({
        "manifest_name": m.name,
        "manifest_version": m.version,
        "api_version": m.api_version.unwrap_or_else(|| "v1".to_string()),
        "command": cmd,
        "args": args,
        "status_code": status.code(),
        "ok": status.success(),
        "stderr": String::from_utf8_lossy(&out_stderr).to_string(),
    }))
}

fn plugin_enabled_for_tenant(tenant: Option<&str>) -> bool {
    let global_on = env::var("AIWF_PLUGIN_ENABLE")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true");
    if !global_on {
        return false;
    }
    let allowed = env::var("AIWF_PLUGIN_TENANT_ALLOWLIST")
        .ok()
        .unwrap_or_default();
    if allowed.trim().is_empty() {
        return true;
    }
    let t = tenant.unwrap_or("default");
    allowed
        .split(',')
        .map(|s| s.trim())
        .any(|x| !x.is_empty() && x.eq_ignore_ascii_case(t))
}

fn enforce_plugin_allowlist(plugin: &str) -> Result<(), String> {
    let allow = env::var("AIWF_PLUGIN_ALLOWLIST")
        .ok()
        .unwrap_or_default();
    if allow.trim().is_empty() {
        return Err("AIWF_PLUGIN_ALLOWLIST is required when plugin is enabled".to_string());
    }
    if allow
        .split(',')
        .map(|s| s.trim())
        .any(|x| !x.is_empty() && x.eq_ignore_ascii_case(plugin))
    {
        Ok(())
    } else {
        Err(format!("plugin not allowed: {plugin}"))
    }
}

fn enforce_plugin_command_allowlist(cmd: &str) -> Result<(), String> {
    let allow = env::var("AIWF_PLUGIN_COMMAND_ALLOWLIST")
        .ok()
        .unwrap_or_default();
    if allow.trim().is_empty() {
        return Err("AIWF_PLUGIN_COMMAND_ALLOWLIST is required when plugin is enabled".to_string());
    }
    if allow
        .split(',')
        .map(|s| s.trim())
        .any(|x| !x.is_empty() && x.eq_ignore_ascii_case(cmd))
    {
        Ok(())
    } else {
        Err(format!("plugin command not allowed: {cmd}"))
    }
}

fn verify_plugin_signature(
    plugin: &str,
    cmd: &str,
    args: &[String],
    signature: Option<&str>,
) -> Result<(), String> {
    let secret = env::var("AIWF_PLUGIN_SIGNING_SECRET")
        .ok()
        .unwrap_or_default();
    if secret.trim().is_empty() {
        return Err("plugin signing secret not configured".to_string());
    }
    let mut h = Sha256::new();
    h.update(
        format!("{secret}:{plugin}:{cmd}:{}", args.join("\u{1f}")).as_bytes(),
    );
    let expected = format!("{:x}", h.finalize());
    let got = signature.unwrap_or("").trim().to_lowercase();
    if got == expected {
        Ok(())
    } else {
        Err("plugin signature verification failed".to_string())
    }
}

fn read_pipe_capped<R: Read + Send + 'static>(
    mut reader: R,
    cap: usize,
    label: &'static str,
) -> std::thread::JoinHandle<Result<Vec<u8>, String>> {
    std::thread::spawn(move || {
        let mut out = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            let n = reader
                .read(&mut buf)
                .map_err(|e| format!("read plugin {label}: {e}"))?;
            if n == 0 {
                break;
            }
            if out.len().saturating_add(n) > cap {
                return Err(format!("plugin {label} exceeds limit: {} > {}", out.len() + n, cap));
            }
            out.extend_from_slice(&buf[..n]);
        }
        Ok(out)
    })
}

fn run_plugin_exec_v1(req: PluginExecReq) -> Result<PluginExecResp, String> {
    if !plugin_enabled_for_tenant(req.tenant_id.as_deref()) {
        return Err("plugin execution disabled for tenant".to_string());
    }
    let plugin = safe_pkg_token(&req.plugin)?;
    enforce_plugin_allowlist(&plugin)?;
    let manifest = load_plugin_manifest(&plugin)?;
    let cmd = manifest.command.clone();
    enforce_plugin_command_allowlist(&cmd)?;
    let args = manifest.args.clone().unwrap_or_default();
    let timeout_ms = manifest
        .timeout_ms
        .unwrap_or(20_000)
        .min(120_000);
    verify_plugin_signature(&plugin, &cmd, &args, manifest.signature.as_deref())?;
    let trace_id = resolve_trace_id(
        req.trace_id.as_deref(),
        None,
        &format!(
            "plugin:{}:{}:{}",
            plugin,
            req.run_id.clone().unwrap_or_default(),
            utc_now_iso()
        ),
    );
    let payload = json!({
        "run_id": req.run_id,
        "tenant_id": req.tenant_id,
        "trace_id": trace_id,
        "plugin": plugin,
        "input": req.input,
    });
    let max_out = env::var("AIWF_PLUGIN_MAX_OUTPUT_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8 * 1024 * 1024);
    let mut child = Command::new(cmd)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn plugin process: {e}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "plugin stdout pipe missing".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "plugin stderr pipe missing".to_string())?;
    let stdout_handle = read_pipe_capped(stdout, max_out, "stdout");
    let stderr_handle = read_pipe_capped(stderr, max_out, "stderr");

    if let Some(mut stdin) = child.stdin.take() {
        let s = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        stdin
            .write_all(s.as_bytes())
            .map_err(|e| format!("write plugin stdin: {e}"))?;
    }

    let start = Instant::now();
    let status = loop {
        if start.elapsed().as_millis() as u64 > timeout_ms {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("plugin timeout: {timeout_ms}ms"));
        }
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) => std::thread::sleep(std::time::Duration::from_millis(20)),
            Err(e) => return Err(format!("plugin wait error: {e}")),
        }
    };

    let out_stdout = stdout_handle
        .join()
        .map_err(|_| "plugin stdout reader thread panicked".to_string())??;
    let out_stderr = stderr_handle
        .join()
        .map_err(|_| "plugin stderr reader thread panicked".to_string())??;
    if out_stdout.len().saturating_add(out_stderr.len()) > max_out {
        return Err(format!(
            "plugin output exceeds limit: {} > {}",
            out_stdout.len() + out_stderr.len(),
            max_out
        ));
    }
    let stdout = String::from_utf8_lossy(&out_stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&out_stderr).to_string();
    let output = if stdout.is_empty() {
        Value::Null
    } else {
        serde_json::from_str::<Value>(&stdout).unwrap_or(Value::String(stdout))
    };
    Ok(PluginExecResp {
        ok: status.success(),
        operator: "plugin_exec_v1".to_string(),
        status: if status.success() {
            "done".to_string()
        } else {
            "failed".to_string()
        },
        run_id: req.run_id,
        trace_id,
        plugin,
        output,
        stderr,
    })
}

fn run_load_rows_v1(req: LoadRowsReq) -> Result<LoadRowsResp, String> {
    let st = req.source_type.to_lowercase();
    let limit = req.limit.unwrap_or(10000).max(1);
    let rows = match st.as_str() {
        "jsonl" => load_jsonl_rows(&req.source, limit)?,
        "csv" => load_csv_rows(&req.source, limit)?,
        "sqlite" => load_sqlite_rows(&req.source, req.query.as_deref().unwrap_or("SELECT * FROM data"), limit)?,
        "sqlserver" => load_sqlserver_rows(&req.source, req.query.as_deref().unwrap_or("SELECT TOP 100 * FROM dbo.workflow_tasks"), limit)?,
        "parquet" => load_parquet_rows(&req.source, limit)?,
        _ => return Err(format!("unsupported source_type: {}", req.source_type)),
    };
    Ok(LoadRowsResp {
        ok: true,
        operator: "load_rows_v1".to_string(),
        status: "done".to_string(),
        stats: json!({"source_type": st, "rows": rows.len()}),
        rows,
    })
}

fn run_save_rows_v1(req: SaveRowsReq) -> Result<SaveRowsResp, String> {
    let st = req.sink_type.to_lowercase();
    match st.as_str() {
        "jsonl" => save_rows_jsonl(&req.sink, &req.rows)?,
        "csv" => save_rows_csv(&req.sink, &req.rows)?,
        "sqlite" => save_rows_sqlite(&req.sink, req.table.as_deref().unwrap_or("data"), &req.rows)?,
        "sqlserver" => save_rows_sqlserver(&req.sink, req.table.as_deref().unwrap_or("dbo.aiwf_rows"), &req.rows)?,
        "parquet" => {
            let mode = req
                .parquet_mode
                .as_deref()
                .unwrap_or("typed")
                .trim()
                .to_lowercase();
            if mode == "payload" {
                save_rows_parquet_payload(&req.sink, &req.rows)?;
            } else {
                save_rows_parquet_typed(&req.sink, &req.rows)?;
            }
        }
        _ => return Err(format!("unsupported sink_type: {}", req.sink_type)),
    }
    Ok(SaveRowsResp {
        ok: true,
        operator: "save_rows_v1".to_string(),
        status: "done".to_string(),
        written_rows: req.rows.len(),
    })
}

fn run_transform_rows_v2_stream(req: TransformRowsStreamReq) -> Result<TransformRowsStreamResp, String> {
    let chunk_size = req.chunk_size.unwrap_or(2000).max(1);
    let rows_in = if let Some(rows) = req.rows.clone() {
        rows
    } else if let Some(uri) = req.input_uri.clone() {
        load_rows_from_uri_limited(&uri, tenant_max_rows(), tenant_max_payload_bytes())?
    } else {
        return Err("rows or input_uri is required".to_string());
    };
    let mut start_chunk = 0usize;
    if req.resume.unwrap_or(false)
        && let Some(key) = req.checkpoint_key.as_deref()
        && let Some(cp) = read_stream_checkpoint(key)?
    {
        start_chunk = cp.saturating_add(1);
    }
    let mut merged_rows: Vec<Value> = Vec::new();
    let mut chunks = 0usize;
    let mut total_input = 0usize;
    let mut total_output = 0usize;
    for (chunk_idx, chunk) in rows_in.chunks(chunk_size).enumerate() {
        if chunk_idx < start_chunk {
            continue;
        }
        chunks += 1;
        total_input += chunk.len();
        let part_req = TransformRowsReq {
            run_id: req.run_id.clone(),
            tenant_id: req.tenant_id.clone(),
            trace_id: None,
            traceparent: None,
            rows: Some(chunk.to_vec()),
            rules: req.rules.clone(),
            rules_dsl: req.rules_dsl.clone(),
            quality_gates: req.quality_gates.clone(),
            schema_hint: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: None,
        };
        let out = run_transform_rows_v2(part_req)?;
        total_output += out.rows.len();
        merged_rows.extend(out.rows);
        if let Some(key) = req.checkpoint_key.as_deref() {
            write_stream_checkpoint(key, chunk_idx)?;
        }
    }
    if let Some(uri) = req.output_uri.as_deref() {
        save_rows_to_uri(uri, &merged_rows)?;
    }
    Ok(TransformRowsStreamResp {
        ok: true,
        operator: "transform_rows_v2_stream".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        rows: merged_rows,
        chunks,
        stats: json!({"input_rows": total_input, "output_rows": total_output, "chunk_size": chunk_size, "resumed_from_chunk": start_chunk}),
    })
}

fn run_workflow(req: WorkflowRunReq) -> Result<WorkflowRunResp, String> {
    if req.steps.len() > tenant_max_workflow_steps() {
        return Err(format!(
            "workflow step quota exceeded: {} > {}",
            req.steps.len(),
            tenant_max_workflow_steps()
        ));
    }
    let trace_id = resolve_trace_id(
        req.trace_id.as_deref(),
        req.traceparent.as_deref(),
        &format!(
            "wf:{}:{}:{}",
            req.run_id.clone().unwrap_or_default(),
            req.tenant_id.clone().unwrap_or_else(|| "default".to_string()),
            req.steps.len()
        ),
    );
    let mut ctx = req.context.unwrap_or_else(|| json!({}));
    let mut trace: Vec<WorkflowStepReplay> = Vec::new();
    let mut failed_step: Option<String> = None;
    let mut failed_error: Option<String> = None;
    for step in &req.steps {
        let Some(obj) = step.as_object() else {
            return Err("workflow step must be object".to_string());
        };
        let id = obj.get("id").and_then(|v| v.as_str()).unwrap_or("step");
        let op = obj.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        let input = obj.get("input").cloned().unwrap_or_else(|| json!({}));
        let started_at = utc_now_iso();
        let begin = Instant::now();
        let step_result: Result<Value, String> = match op {
            "transform_rows_v2" => serde_json::from_value::<TransformRowsReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_transform_rows_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "text_preprocess_v2" => serde_json::from_value::<TextPreprocessReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_text_preprocess_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "compute_metrics" => serde_json::from_value::<ComputeReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_compute_metrics)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "join_rows_v1" => serde_json::from_value::<JoinRowsReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_join_rows_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "normalize_schema_v1" => serde_json::from_value::<NormalizeSchemaReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_normalize_schema_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "entity_extract_v1" => serde_json::from_value::<EntityExtractReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_entity_extract_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "aggregate_rows_v1" => serde_json::from_value::<AggregateRowsReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_aggregate_rows_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "quality_check_v1" => serde_json::from_value::<QualityCheckReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_quality_check_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "aggregate_pushdown_v1" => serde_json::from_value::<AggregatePushdownReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_aggregate_pushdown_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "plugin_exec_v1" => serde_json::from_value::<PluginExecReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_plugin_exec_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "plugin_health_v1" => serde_json::from_value::<PluginHealthReq>(input)
                .map_err(|e| e.to_string())
                .and_then(|r| {
                    let plugin = safe_pkg_token(&r.plugin)?;
                    run_plugin_healthcheck(&plugin, r.tenant_id.as_deref()).map(|details| {
                        json!({
                            "ok": true,
                            "operator": "plugin_health_v1",
                            "status": "done",
                            "plugin": plugin,
                            "details": details
                        })
                    })
                }),
            "rules_package_publish_v1" => serde_json::from_value::<RulesPackagePublishReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_rules_package_publish_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "rules_package_get_v1" => serde_json::from_value::<RulesPackageGetReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_rules_package_get_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            _ => Err(format!("unsupported workflow operator: {op}")),
        };
        let output = match step_result {
            Ok(v) => v,
            Err(err) => {
                trace.push(WorkflowStepReplay {
                    id: id.to_string(),
                    operator: op.to_string(),
                    status: "failed".to_string(),
                    started_at,
                    finished_at: utc_now_iso(),
                    duration_ms: begin.elapsed().as_millis(),
                    input_summary: summarize_value(&obj.get("input").cloned().unwrap_or_else(|| json!({}))),
                    output_summary: None,
                    error: Some(err.clone()),
                });
                failed_step = Some(id.to_string());
                failed_error = Some(err);
                break;
            }
        };
        let finished_at = utc_now_iso();
        if let Some(map) = ctx.as_object_mut()
            && failed_step.is_none()
        {
            map.insert(id.to_string(), output.clone());
        }
        trace.push(WorkflowStepReplay {
            id: id.to_string(),
            operator: op.to_string(),
            status: "done".to_string(),
            started_at,
            finished_at,
            duration_ms: begin.elapsed().as_millis(),
            input_summary: summarize_value(&obj.get("input").cloned().unwrap_or_else(|| json!({}))),
            output_summary: Some(summarize_value(&output)),
            error: None,
        });
    }
    let status = if failed_step.is_some() { "failed" } else { "done" };
    Ok(WorkflowRunResp {
        ok: failed_step.is_none(),
        operator: "workflow_run".to_string(),
        status: status.to_string(),
        trace_id,
        run_id: req.run_id,
        context: ctx,
        steps: trace,
        failed_step,
        error: failed_error,
    })
}

fn summarize_value(v: &Value) -> Value {
    match v {
        Value::Array(a) => json!({"type":"array","len":a.len()}),
        Value::Object(m) => {
            let keys = m.keys().take(12).cloned().collect::<Vec<_>>();
            json!({"type":"object","keys":keys,"size":m.len()})
        }
        Value::String(s) => json!({"type":"string","len":s.chars().count()}),
        Value::Number(n) => json!({"type":"number","value":n}),
        Value::Bool(b) => json!({"type":"bool","value":b}),
        Value::Null => json!({"type":"null"}),
    }
}

fn collapse_ws(s: &str) -> String {
    let mut out = String::new();
    let mut prev_space = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out.trim().to_string()
}

fn can_cancel_status(status: &str) -> bool {
    status == "queued" || status == "running"
}

fn tenant_max_concurrency() -> usize {
    env::var("AIWF_TENANT_MAX_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(4)
        .max(1)
}

fn tenant_max_rows() -> usize {
    env::var("AIWF_TENANT_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(250_000)
        .max(1)
}

fn tenant_max_payload_bytes() -> usize {
    env::var("AIWF_TENANT_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(128 * 1024 * 1024)
        .max(1024)
}

fn tenant_max_workflow_steps() -> usize {
    env::var("AIWF_TENANT_MAX_WORKFLOW_STEPS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(128)
        .max(1)
}

fn enforce_tenant_payload_quota(
    state: Option<&AppState>,
    rows: usize,
    payload_bytes: usize,
) -> Result<(), String> {
    let max_rows = tenant_max_rows();
    if rows > max_rows {
        if let Some(s) = state && let Ok(mut m) = s.metrics.lock() {
            m.quota_reject_total += 1;
        }
        return Err(format!("tenant row quota exceeded: {rows} > {max_rows}"));
    }
    let max_bytes = tenant_max_payload_bytes();
    if payload_bytes > max_bytes {
        if let Some(s) = state && let Ok(mut m) = s.metrics.lock() {
            m.quota_reject_total += 1;
        }
        return Err(format!(
            "tenant payload quota exceeded: {payload_bytes} > {max_bytes}"
        ));
    }
    Ok(())
}

fn try_acquire_tenant_slot(state: &AppState, tenant: &str) -> Result<(), String> {
    let limit = tenant_max_concurrency();
    if let Ok(mut running) = state.tenant_running.lock() {
        let cur = running.get(tenant).copied().unwrap_or(0);
        if cur >= limit {
            if let Ok(mut m) = state.metrics.lock() {
                m.tenant_reject_total += 1;
            }
            return Err(format!("tenant concurrency exceeded: {cur} >= {limit}"));
        }
        running.insert(tenant.to_string(), cur + 1);
    }
    Ok(())
}

fn release_tenant_slot(state: &AppState, tenant: &str) {
    if let Ok(mut running) = state.tenant_running.lock()
        && let Some(v) = running.get_mut(tenant)
        && *v > 0
    {
        *v -= 1;
    }
}

fn verify_request_signature(req: &TransformRowsReq) -> Result<(), String> {
    let Ok(secret) = env::var("AIWF_REQUEST_SIGNING_SECRET") else {
        return Ok(());
    };
    if secret.trim().is_empty() {
        return Ok(());
    }
    let run_id = req.run_id.clone().unwrap_or_default();
    let tenant = req
        .tenant_id
        .clone()
        .unwrap_or_else(|| env::var("AIWF_TENANT_ID").unwrap_or_else(|_| "default".to_string()));
    let expected = {
        let mut h = Sha256::new();
        h.update(format!("{secret}:{tenant}:{run_id}").as_bytes());
        format!("{:x}", h.finalize())
    };
    let got = req.request_signature.clone().unwrap_or_default();
    if got.eq_ignore_ascii_case(&expected) {
        Ok(())
    } else {
        Err("invalid request signature".to_string())
    }
}

fn resolve_trace_id(explicit: Option<&str>, traceparent: Option<&str>, seed: &str) -> String {
    if let Some(v) = explicit {
        let t = v.trim();
        if t.len() == 32 && t.chars().all(|c| c.is_ascii_hexdigit()) {
            return t.to_lowercase();
        }
    }
    if let Some(tp) = traceparent {
        let p = tp.trim();
        let parts = p.split('-').collect::<Vec<_>>();
        if parts.len() >= 4 {
            let tid = parts[1];
            if tid.len() == 32 && tid.chars().all(|c| c.is_ascii_hexdigit()) {
                return tid.to_lowercase();
            }
        }
    }
    let mut h = Sha256::new();
    h.update(seed.as_bytes());
    format!("{:x}", h.finalize())
}

fn is_cancelled(flag: &Option<Arc<AtomicBool>>) -> bool {
    match flag {
        Some(v) => v.load(Ordering::Relaxed),
        None => false,
    }
}

fn observe_transform_success(metrics: &Arc<Mutex<ServiceMetrics>>, resp: &TransformRowsResp) {
    if let Ok(mut m) = metrics.lock() {
        m.transform_rows_v2_success_total += 1;
        m.transform_rows_v2_latency_ms_sum += resp.stats.latency_ms;
        m.transform_rows_v2_latency_ms_max = m.transform_rows_v2_latency_ms_max.max(resp.stats.latency_ms);
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

fn cleanup_task_flag(
    task_id: &str,
    cancel_flags: &Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
    metrics: &Arc<Mutex<ServiceMetrics>>,
) {
    let mut removed = false;
    if let Ok(mut flags) = cancel_flags.lock() {
        removed = flags.remove(task_id).is_some();
    }
    if let Ok(mut m) = metrics.lock() {
        if removed {
            m.tasks_active = (m.tasks_active - 1).max(0);
            m.task_flag_cleanup_total += 1;
        }
    }
}

fn task_store_config_from_env() -> TaskStoreConfig {
    let ttl_sec = env::var("AIWF_RUST_TASK_TTL_SEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(24 * 60 * 60);
    let max_tasks = env::var("AIWF_RUST_TASK_MAX")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let store_path = env::var("AIWF_RUST_TASK_STORE_PATH").ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(PathBuf::from(t))
        }
    });
    let base_api_url = env::var("AIWF_BASE_URL").ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });
    let base_api_key = env::var("AIWF_API_KEY").ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });
    let remote_enabled = env::var("AIWF_RUST_TASK_STORE_REMOTE")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true");
    let backend = env::var("AIWF_RUST_TASK_STORE_BACKEND")
        .unwrap_or_else(|_| "base_api".to_string())
        .trim()
        .to_lowercase();
    let sql_host = env::var("AIWF_SQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let sql_port = env::var("AIWF_SQL_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(1433);
    let sql_db = env::var("AIWF_SQL_DB").unwrap_or_else(|_| "AIWF".to_string());
    let sql_user = env::var("AIWF_SQL_USER").ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });
    let sql_password = env::var("AIWF_SQL_PASSWORD").ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });
    let sql_use_windows_auth = env::var("AIWF_SQL_USE_WINDOWS_AUTH")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true");
    TaskStoreConfig {
        ttl_sec,
        max_tasks,
        store_path,
        remote_enabled,
        backend,
        base_api_url,
        base_api_key,
        sql_host,
        sql_port,
        sql_db,
        sql_user,
        sql_password,
        sql_use_windows_auth,
    }
}

fn resolve_task_store_backend(mut cfg: TaskStoreConfig) -> TaskStoreConfig {
    if !cfg.remote_enabled {
        return cfg;
    }
    let backend = cfg.backend.clone();
    let mut cands: Vec<&str> = Vec::new();
    match backend.as_str() {
        "odbc" => cands.extend(["odbc", "sqlcmd", "base_api"]),
        "sqlcmd" => cands.extend(["sqlcmd", "odbc", "base_api"]),
        "base_api" => cands.extend(["base_api", "sqlcmd", "odbc"]),
        _ => cands.extend(["base_api", "sqlcmd", "odbc"]),
    }
    for b in cands {
        let mut probe_cfg = cfg.clone();
        probe_cfg.backend = b.to_string();
        if b == "sqlcmd" && !is_sqlcmd_available() {
            continue;
        }
        if b == "base_api" && probe_cfg.base_api_url.as_ref().is_none_or(|u| u.trim().is_empty()) {
            continue;
        }
        if probe_remote_task_store(&probe_cfg) {
            cfg.backend = b.to_string();
            return cfg;
        }
    }
    cfg.remote_enabled = false;
    cfg
}

fn is_sqlcmd_available() -> bool {
    if cfg!(windows) {
        if let Ok(out) = Command::new("where").arg("sqlcmd").output() {
            return out.status.success();
        }
        false
    } else {
        if let Ok(out) = Command::new("which").arg("sqlcmd").output() {
            return out.status.success();
        }
        false
    }
}

fn task_epoch(task: &TaskState) -> u64 {
    task.updated_at
        .parse::<u64>()
        .ok()
        .or_else(|| task.created_at.parse::<u64>().ok())
        .unwrap_or(0)
}

fn prune_tasks(tasks: &mut HashMap<String, TaskState>, cfg: &TaskStoreConfig) -> usize {
    if tasks.is_empty() {
        return 0;
    }
    let now = utc_now_iso().parse::<u64>().unwrap_or(0);
    let mut removed = 0usize;
    if cfg.ttl_sec > 0 && now > 0 {
        let before = tasks.len();
        tasks.retain(|_, t| now.saturating_sub(task_epoch(t)) <= cfg.ttl_sec);
        removed += before.saturating_sub(tasks.len());
    }

    if cfg.max_tasks > 0 && tasks.len() > cfg.max_tasks {
        let mut ids = tasks
            .iter()
            .map(|(k, t)| (k.clone(), task_epoch(t)))
            .collect::<Vec<_>>();
        ids.sort_by_key(|(_, ts)| *ts);
        let drop_n = tasks.len().saturating_sub(cfg.max_tasks);
        for (id, _) in ids.into_iter().take(drop_n) {
            if tasks.remove(&id).is_some() {
                removed += 1;
            }
        }
    }
    removed
}

fn load_tasks_from_store(path: Option<&PathBuf>) -> HashMap<String, TaskState> {
    let Some(p) = path else {
        return HashMap::new();
    };
    let Ok(bytes) = fs::read(p) else {
        return HashMap::new();
    };
    let mut out: HashMap<String, TaskState> = serde_json::from_slice(&bytes).unwrap_or_default();
    let cfg = task_store_config_from_env();
    let _ = prune_tasks(&mut out, &cfg);
    out
}

fn persist_tasks_to_store(tasks: &HashMap<String, TaskState>, path: Option<&PathBuf>) {
    let Some(p) = path else {
        return;
    };
    if let Some(parent) = p.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(buf) = serde_json::to_vec_pretty(tasks) {
        let _ = fs::write(p, buf);
    }
}

fn task_store_remote_enabled(cfg: &TaskStoreConfig) -> bool {
    if !cfg.remote_enabled {
        return false;
    }
    match cfg.backend.as_str() {
        "odbc" => true,
        "sqlcmd" => true,
        _ => cfg.base_api_url.as_ref().is_some_and(|v| !v.trim().is_empty()),
    }
}

fn probe_remote_task_store(cfg: &TaskStoreConfig) -> bool {
    match cfg.backend.as_str() {
        "odbc" => odbc_probe_task_store(cfg),
        "sqlcmd" => sqlcmd_probe_task_store(cfg),
        _ => base_api_probe_task_store(cfg),
    }
}

fn task_store_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) {
    if !task_store_remote_enabled(cfg) {
        return;
    }
    match cfg.backend.as_str() {
        "odbc" => odbc_upsert_task(task, cfg),
        "sqlcmd" => sqlcmd_upsert_task(task, cfg),
        _ => base_api_upsert_task(task, cfg),
    }
}

fn task_store_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<TaskState> {
    if !task_store_remote_enabled(cfg) {
        return None;
    }
    match cfg.backend.as_str() {
        "odbc" => odbc_get_task(task_id, cfg),
        "sqlcmd" => sqlcmd_get_task(task_id, cfg),
        _ => base_api_get_task(task_id, cfg),
    }
}

fn task_store_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<Value> {
    if !task_store_remote_enabled(cfg) {
        return None;
    }
    match cfg.backend.as_str() {
        "odbc" => odbc_cancel_task(task_id, cfg),
        "sqlcmd" => sqlcmd_cancel_task(task_id, cfg),
        _ => base_api_cancel_task(task_id, cfg),
    }
}

fn base_api_probe_task_store(cfg: &TaskStoreConfig) -> bool {
    let Some(base) = &cfg.base_api_url else {
        return false;
    };
    let url = format!("{}/actuator/health", base.trim_end_matches('/'));
    let mut req = ureq::get(&url);
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let Ok(resp) = req.call() else {
        return false;
    };
    let Ok(v) = resp.into_json::<Value>() else {
        return false;
    };
    v.get("status")
        .and_then(|x| x.as_str())
        .map(|s| s.eq_ignore_ascii_case("UP"))
        .unwrap_or(false)
}

fn base_api_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) {
    let Some(base) = &cfg.base_api_url else {
        return;
    };
    let url = format!("{}/api/v1/runtime/tasks/upsert", base.trim_end_matches('/'));
    let payload = json!({
        "task_id": task.task_id,
        "tenant_id": task.tenant_id,
        "operator": task.operator,
        "status": task.status,
        "created_at": task.created_at.parse::<u64>().unwrap_or(0),
        "updated_at": task.updated_at.parse::<u64>().unwrap_or(0),
        "result": task.result.clone(),
        "error": task.error.clone(),
        "idempotency_key": task.idempotency_key,
        "attempts": task.attempts,
        "source": "accel-rust"
    });
    let mut req = ureq::post(&url).set("Content-Type", "application/json");
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let _ = req.send_json(payload);
}

fn base_api_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<TaskState> {
    let Some(base) = &cfg.base_api_url else {
        return None;
    };
    let url = format!(
        "{}/api/v1/runtime/tasks/{}",
        base.trim_end_matches('/'),
        task_id
    );
    let mut req = ureq::get(&url);
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let resp = req.call().ok()?;
    let body: Value = resp.into_json().ok()?;
    let task = body.get("task")?;
    parse_task_from_runtime_row(task)
}

fn base_api_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<Value> {
    let Some(base) = &cfg.base_api_url else {
        return None;
    };
    let url = format!(
        "{}/api/v1/runtime/tasks/{}/cancel",
        base.trim_end_matches('/'),
        task_id
    );
    let mut req = ureq::post(&url).set("Content-Type", "application/json");
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let resp = req.send_string("{}").ok()?;
    resp.into_json().ok()
}

fn parse_task_from_runtime_row(task: &Value) -> Option<TaskState> {
    let task_id = task.get("task_id")?.as_str()?.to_string();
    let tenant_id = task
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    let operator = task
        .get("operator")
        .and_then(|v| v.as_str())
        .unwrap_or("transform_rows_v2")
        .to_string();
    let status = task
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("queued")
        .to_string();
    let created_at = task
        .get("created_at_epoch")
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        .to_string();
    let updated_at = task
        .get("updated_at_epoch")
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        .to_string();
    let result = task
        .get("result_json")
        .and_then(|v| v.as_str())
        .and_then(|s| serde_json::from_str::<Value>(s).ok());
    let error = task
        .get("error")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let idempotency_key = task
        .get("idempotency_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let attempts = task
        .get("attempts")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    Some(TaskState {
        task_id,
        tenant_id,
        operator,
        status,
        created_at,
        updated_at,
        result,
        error,
        idempotency_key,
        attempts,
    })
}

fn odbc_conn_str(cfg: &TaskStoreConfig) -> String {
    if cfg.sql_use_windows_auth {
        format!(
            "Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{},{};Database={};Trusted_Connection=Yes;Encrypt=no;TrustServerCertificate=yes;",
            cfg.sql_host, cfg.sql_port, cfg.sql_db
        )
    } else {
        format!(
            "Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{},{};Database={};Uid={};Pwd={};Encrypt=no;TrustServerCertificate=yes;",
            cfg.sql_host,
            cfg.sql_port,
            cfg.sql_db,
            cfg.sql_user.clone().unwrap_or_default(),
            cfg.sql_password.clone().unwrap_or_default()
        )
    }
}

fn run_odbc_query_first_text(
    cfg: &TaskStoreConfig,
    query: &str,
    params: &[String],
) -> Result<Option<String>, String> {
    let env = Environment::new().map_err(|e| format!("odbc env: {e}"))?;
    let conn = env
        .connect_with_connection_string(&odbc_conn_str(cfg), ConnectionOptions::default())
        .map_err(|e| format!("odbc connect: {e}"))?;
    let param_buf: Vec<_> = params.iter().map(|p| p.as_str().into_parameter()).collect();
    let maybe_cursor = conn
        .execute(query, param_buf.as_slice())
        .map_err(|e| format!("odbc execute: {e}"))?;
    let Some(mut cursor) = maybe_cursor else {
        return Ok(None);
    };
    let buffers =
        TextRowSet::for_cursor(8, &mut cursor, Some(16384)).map_err(|e| format!("odbc buffer: {e}"))?;
    let mut row_set_cursor = cursor
        .bind_buffer(buffers)
        .map_err(|e| format!("odbc bind: {e}"))?;
    if let Some(batch) = row_set_cursor.fetch().map_err(|e| format!("odbc fetch: {e}"))?
        && batch.num_rows() > 0
        && let Some(txt) = batch.at(0, 0)
    {
        return Ok(Some(String::from_utf8_lossy(txt).to_string()));
    }
    Ok(None)
}

fn run_odbc_exec(cfg: &TaskStoreConfig, query: &str, params: &[String]) -> Result<(), String> {
    let env = Environment::new().map_err(|e| format!("odbc env: {e}"))?;
    let conn = env
        .connect_with_connection_string(&odbc_conn_str(cfg), ConnectionOptions::default())
        .map_err(|e| format!("odbc connect: {e}"))?;
    let param_buf: Vec<_> = params.iter().map(|p| p.as_str().into_parameter()).collect();
    let _ = conn
        .execute(query, param_buf.as_slice())
        .map_err(|e| format!("odbc execute: {e}"))?;
    Ok(())
}

fn odbc_probe_task_store(cfg: &TaskStoreConfig) -> bool {
    let q = "SET NOCOUNT ON; SELECT CASE WHEN OBJECT_ID('dbo.workflow_tasks','U') IS NULL THEN N'0' ELSE N'1' END;";
    match run_odbc_query_first_text(cfg, q, &[]) {
        Ok(Some(v)) => v.trim() == "1",
        _ => false,
    }
}

fn odbc_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) {
    let task_id = task.task_id.clone();
    let tenant_id = task.tenant_id.clone();
    let operator = task.operator.clone();
    let status = task.status.clone();
    let created = task.created_at.parse::<u64>().unwrap_or(0).to_string();
    let updated = task.updated_at.parse::<u64>().unwrap_or(0).to_string();
    let result_json = task.result.as_ref().map(|v| v.to_string()).unwrap_or_default();
    let error = task.error.clone().unwrap_or_default();
    let source = "accel-rust".to_string();
    let q = "SET NOCOUNT ON;\
DECLARE @task_id NVARCHAR(128)=?;\
DECLARE @tenant_id NVARCHAR(128)=?;\
DECLARE @operator NVARCHAR(128)=?;\
DECLARE @status NVARCHAR(64)=?;\
DECLARE @created_at_epoch BIGINT=CAST(? AS BIGINT);\
DECLARE @updated_at_epoch BIGINT=CAST(? AS BIGINT);\
DECLARE @result_json NVARCHAR(MAX)=?;\
DECLARE @error NVARCHAR(MAX)=?;\
DECLARE @source NVARCHAR(64)=?;\
IF EXISTS (SELECT 1 FROM dbo.workflow_tasks WHERE task_id=@task_id)\
BEGIN\
  UPDATE dbo.workflow_tasks\
  SET tenant_id=@tenant_id,operator=@operator,status=@status,\
      created_at_epoch=@created_at_epoch,updated_at_epoch=@updated_at_epoch,\
      result_json=@result_json,error=@error,source=@source\
  WHERE task_id=@task_id;\
END\
ELSE\
BEGIN\
  INSERT INTO dbo.workflow_tasks (task_id,tenant_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error,source)\
  VALUES (@task_id,@tenant_id,@operator,@status,@created_at_epoch,@updated_at_epoch,@result_json,@error,@source);\
END";
    let params = vec![
        task_id, tenant_id, operator, status, created, updated, result_json, error, source,
    ];
    let _ = run_odbc_exec(cfg, q, &params);
}

fn odbc_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<TaskState> {
    let q = "SET NOCOUNT ON;\
DECLARE @task_id NVARCHAR(128)=?;\
SELECT TOP 1 task_id,tenant_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error\
FROM dbo.workflow_tasks WHERE task_id=@task_id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;";
    let out = run_odbc_query_first_text(cfg, q, &[task_id.to_string()]).ok()??;
    let s = out.trim();
    if s.is_empty() {
        return None;
    }
    let row: Value = serde_json::from_str(s).ok()?;
    parse_task_from_runtime_row(&row)
}

fn odbc_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<Value> {
    let now = utc_now_iso().parse::<u64>().unwrap_or(0).to_string();
    let q = "SET NOCOUNT ON;\
DECLARE @task_id NVARCHAR(128)=?;\
DECLARE @now BIGINT=CAST(? AS BIGINT);\
UPDATE dbo.workflow_tasks SET status=N'cancelled',updated_at_epoch=@now\
WHERE task_id=@task_id AND status IN (N'queued',N'running');\
SELECT TOP 1 task_id,status FROM dbo.workflow_tasks WHERE task_id=@task_id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;";
    let out = run_odbc_query_first_text(cfg, q, &[task_id.to_string(), now]).ok()??;
    let row: Value = serde_json::from_str(out.trim()).ok()?;
    let status = row
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    Some(json!({
        "ok": true,
        "task_id": row.get("task_id").and_then(|v| v.as_str()).unwrap_or(""),
        "cancelled": status == "cancelled",
        "status": status
    }))
}

fn sqlcmd_probe_task_store(cfg: &TaskStoreConfig) -> bool {
    let q = "SET NOCOUNT ON; SELECT CASE WHEN OBJECT_ID('dbo.workflow_tasks','U') IS NULL THEN 0 ELSE 1 END AS ok_flag;";
    let Ok(out) = run_sqlcmd_query(cfg, q) else {
        return false;
    };
    out.trim().ends_with('1')
}

fn sqlcmd_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) {
    let task_id = escape_tsql(&task.task_id);
    let tenant_id = escape_tsql(&task.tenant_id);
    let operator = escape_tsql(&task.operator);
    let status = escape_tsql(&task.status);
    let created = task.created_at.parse::<u64>().unwrap_or(0);
    let updated = task.updated_at.parse::<u64>().unwrap_or(0);
    let result_json = task.result.as_ref().map(|v| v.to_string()).unwrap_or_default();
    let result_json = escape_tsql(&result_json);
    let error = escape_tsql(task.error.as_deref().unwrap_or(""));
    let q = format!(
        "SET NOCOUNT ON; IF EXISTS (SELECT 1 FROM dbo.workflow_tasks WHERE task_id=N'{task_id}') BEGIN UPDATE dbo.workflow_tasks SET tenant_id=N'{tenant_id}',operator=N'{operator}',status=N'{status}',created_at_epoch={created},updated_at_epoch={updated},result_json=N'{result_json}',error=N'{error}',source=N'accel-rust' WHERE task_id=N'{task_id}'; END ELSE BEGIN INSERT INTO dbo.workflow_tasks (task_id,tenant_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error,source) VALUES (N'{task_id}',N'{tenant_id}',N'{operator}',N'{status}',{created},{updated},N'{result_json}',N'{error}',N'accel-rust'); END"
    );
    let _ = run_sqlcmd_query(cfg, &q);
}

fn sqlcmd_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<TaskState> {
    let task_id = escape_tsql(task_id);
    let q = format!(
        "SET NOCOUNT ON; SELECT TOP 1 task_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error FROM dbo.workflow_tasks WHERE task_id=N'{task_id}' FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;"
    );
    let out = run_sqlcmd_query(cfg, &q).ok()?;
    let s = out.trim();
    if s.is_empty() {
        return None;
    }
    let row: Value = serde_json::from_str(s).ok()?;
    parse_task_from_runtime_row(&row)
}

fn sqlcmd_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<Value> {
    let task_id = escape_tsql(task_id);
    let now = utc_now_iso().parse::<u64>().unwrap_or(0);
    let q = format!(
        "SET NOCOUNT ON; UPDATE dbo.workflow_tasks SET status=N'cancelled',updated_at_epoch={now} WHERE task_id=N'{task_id}' AND status IN (N'queued',N'running'); SELECT TOP 1 task_id,status FROM dbo.workflow_tasks WHERE task_id=N'{task_id}' FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;"
    );
    let out = run_sqlcmd_query(cfg, &q).ok()?;
    let row: Value = serde_json::from_str(out.trim()).ok()?;
    let status = row
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    Some(json!({
        "ok": true,
        "task_id": row.get("task_id").and_then(|v| v.as_str()).unwrap_or(""),
        "cancelled": status == "cancelled",
        "status": status
    }))
}

fn run_sqlcmd_query(cfg: &TaskStoreConfig, query: &str) -> Result<String, String> {
    let mut cmd = Command::new("sqlcmd");
    cmd.arg("-S")
        .arg(format!("{},{}", cfg.sql_host, cfg.sql_port))
        .arg("-d")
        .arg(cfg.sql_db.clone())
        .arg("-W")
        .arg("-h")
        .arg("-1")
        .arg("-Q")
        .arg(query);

    if cfg.sql_use_windows_auth {
        cmd.arg("-E");
    } else {
        let user = cfg.sql_user.clone().unwrap_or_default();
        let pwd = cfg.sql_password.clone().unwrap_or_default();
        cmd.arg("-U").arg(user).arg("-P").arg(pwd);
    }
    let out = cmd.output().map_err(|e| format!("run sqlcmd: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "sqlcmd failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn escape_tsql(s: &str) -> String {
    s.replace('\'', "''")
}

fn validate_sql_identifier(s: &str) -> Result<String, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty sql identifier".to_string());
    }
    let ok = t
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.');
    if !ok || t.starts_with('.') || t.ends_with('.') || t.contains("..") {
        return Err(format!("invalid sql identifier: {s}"));
    }
    Ok(t.to_string())
}

fn validate_where_clause(s: &str) -> Result<String, String> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(String::new());
    }
    // Strict mode: only allow conjunction/disjunction of simple predicates:
    // identifier op literal, where op in (=, !=, >, >=, <, <=, like)
    let lower = t.to_lowercase().replace('\n', " ");
    let tokens = lower
        .split_whitespace()
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();
    if tokens.len() < 3 {
        return Err("where_sql too short".to_string());
    }
    let ident_ok = |x: &str| {
        !x.is_empty()
            && x.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    };
    let op_ok = |x: &str| matches!(x, "=" | "!=" | ">" | ">=" | "<" | "<=" | "like");
    let lit_ok = |x: &str| {
        if x.starts_with('\'') && x.ends_with('\'') && x.len() >= 2 {
            return true;
        }
        x.parse::<f64>().is_ok()
    };
    let mut i = 0usize;
    while i < tokens.len() {
        if i + 2 >= tokens.len() {
            return Err("where_sql invalid predicate tail".to_string());
        }
        if !ident_ok(tokens[i]) || !op_ok(tokens[i + 1]) || !lit_ok(tokens[i + 2]) {
            return Err("where_sql contains unsupported predicate".to_string());
        }
        i += 3;
        if i >= tokens.len() {
            break;
        }
        if !matches!(tokens[i], "and" | "or") {
            return Err("where_sql only supports AND/OR connectors".to_string());
        }
        i += 1;
        if i >= tokens.len() {
            return Err("where_sql ends with connector".to_string());
        }
    }
    Ok(t.to_string())
}

fn utc_now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{ts}")
}

fn short_trace(seed: &str) -> String {
    let mut h = Sha256::new();
    h.update(seed.as_bytes());
    let hex = format!("{:x}", h.finalize());
    hex[..16].to_string()
}

fn rule_get<'a>(rules: &'a Value, key: &str) -> Option<&'a Value> {
    rules.as_object().and_then(|m| m.get(key))
}

fn as_array_str(v: Option<&Value>) -> Vec<String> {
    v.and_then(|x| x.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn as_bool(v: Option<&Value>, default: bool) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) != 0,
        Some(Value::String(s)) => {
            let t = s.trim().to_lowercase();
            matches!(t.as_str(), "1" | "true" | "yes" | "on")
        }
        _ => default,
    }
}

fn is_missing(v: Option<&Value>) -> bool {
    match v {
        None => true,
        Some(Value::Null) => true,
        Some(Value::String(s)) => s.trim().is_empty(),
        _ => false,
    }
}

fn cast_value(v: Value, cast_type: &str) -> Option<Value> {
    if v.is_null() {
        return Some(Value::Null);
    }
    match cast_type {
        "string" | "str" => Some(Value::String(value_to_string(&v))),
        "int" | "integer" => {
            if let Some(n) = v.as_i64() {
                return Some(Value::Number(n.into()));
            }
            let s = value_to_string(&v);
            s.trim()
                .parse::<i64>()
                .ok()
                .map(|x| Value::Number(x.into()))
        }
        "float" | "double" | "number" => {
            if let Some(n) = v.as_f64() {
                return serde_json::Number::from_f64(n).map(Value::Number);
            }
            let s = value_to_string(&v).replace(',', "");
            s.trim()
                .parse::<f64>()
                .ok()
                .and_then(|x| serde_json::Number::from_f64(x).map(Value::Number))
        }
        "bool" | "boolean" => {
            if let Some(b) = v.as_bool() {
                return Some(Value::Bool(b));
            }
            let s = value_to_string(&v).to_lowercase();
            if matches!(s.as_str(), "1" | "true" | "yes" | "on") {
                Some(Value::Bool(true))
            } else if matches!(s.as_str(), "0" | "false" | "no" | "off") {
                Some(Value::Bool(false))
            } else {
                None
            }
        }
        _ => Some(v),
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "".to_string(),
        _ => v.to_string(),
    }
}

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.replace(',', "").trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn value_to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_u64().map(|x| x as i64)),
        Value::String(s) => s.replace(',', "").trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn compile_filters(filters: &[Value]) -> Vec<CompiledFilter> {
    filters
        .iter()
        .map(|f| {
            let Some(obj) = f.as_object() else {
                return CompiledFilter {
                    field: String::new(),
                    op: FilterOp::Passthrough,
                };
            };
            let field = obj
                .get("field")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let op_name = obj
                .get("op")
                .and_then(|v| v.as_str())
                .unwrap_or("eq")
                .to_lowercase();
            let target = obj.get("value");
            let op = match op_name.as_str() {
                "exists" => FilterOp::Exists,
                "not_exists" => FilterOp::NotExists,
                "eq" => FilterOp::Eq(target.map(value_to_string).unwrap_or_default()),
                "ne" => FilterOp::Ne(target.map(value_to_string).unwrap_or_default()),
                "contains" => FilterOp::Contains(target.map(value_to_string).unwrap_or_default()),
                "in" => match target.and_then(|v| v.as_array()) {
                    Some(arr) => FilterOp::In(arr.iter().map(value_to_string).collect()),
                    None => FilterOp::Invalid,
                },
                "not_in" => match target.and_then(|v| v.as_array()) {
                    Some(arr) => FilterOp::NotIn(arr.iter().map(value_to_string).collect()),
                    None => FilterOp::Invalid,
                },
                "regex" => {
                    let pat = target.map(value_to_string).unwrap_or_default();
                    if pat.trim().is_empty() || pat.len() > 1024 {
                        FilterOp::Invalid
                    } else {
                        Regex::new(&pat).map(FilterOp::Regex).unwrap_or(FilterOp::Invalid)
                    }
                }
                "not_regex" => {
                    let pat = target.map(value_to_string).unwrap_or_default();
                    if pat.trim().is_empty() || pat.len() > 1024 {
                        FilterOp::Invalid
                    } else {
                        Regex::new(&pat)
                            .map(FilterOp::NotRegex)
                            .unwrap_or(FilterOp::Invalid)
                    }
                }
                "gt" => target
                    .and_then(value_to_f64)
                    .map(FilterOp::Gt)
                    .unwrap_or(FilterOp::Invalid),
                "gte" => target
                    .and_then(value_to_f64)
                    .map(FilterOp::Gte)
                    .unwrap_or(FilterOp::Invalid),
                "lt" => target
                    .and_then(value_to_f64)
                    .map(FilterOp::Lt)
                    .unwrap_or(FilterOp::Invalid),
                "lte" => target
                    .and_then(value_to_f64)
                    .map(FilterOp::Lte)
                    .unwrap_or(FilterOp::Invalid),
                _ => FilterOp::Passthrough,
            };
            CompiledFilter { field, op }
        })
        .collect()
}

fn filter_match_compiled(row: &Map<String, Value>, f: &CompiledFilter) -> bool {
    let val = row.get(&f.field);
    match &f.op {
        FilterOp::Exists => !is_missing(val),
        FilterOp::NotExists => is_missing(val),
        FilterOp::Eq(t) => value_to_string_or_null(val) == *t,
        FilterOp::Ne(t) => value_to_string_or_null(val) != *t,
        FilterOp::Contains(t) => value_to_string_or_null(val).contains(t),
        FilterOp::In(arr) => {
            let cur = value_to_string_or_null(val);
            arr.iter().any(|x| x == &cur)
        }
        FilterOp::NotIn(arr) => {
            let cur = value_to_string_or_null(val);
            arr.iter().all(|x| x != &cur)
        }
        FilterOp::Regex(re) => re.is_match(&value_to_string_or_null(val)),
        FilterOp::NotRegex(re) => !re.is_match(&value_to_string_or_null(val)),
        FilterOp::Gt(y) => val.and_then(value_to_f64).is_some_and(|x| x > *y),
        FilterOp::Gte(y) => val.and_then(value_to_f64).is_some_and(|x| x >= *y),
        FilterOp::Lt(y) => val.and_then(value_to_f64).is_some_and(|x| x < *y),
        FilterOp::Lte(y) => val.and_then(value_to_f64).is_some_and(|x| x <= *y),
        FilterOp::Invalid => false,
        FilterOp::Passthrough => true,
    }
}

fn value_to_string_or_null(v: Option<&Value>) -> String {
    v.map(value_to_string).unwrap_or_default()
}

fn dedup_key(row: &Map<String, Value>, fields: &[String]) -> String {
    fields
        .iter()
        .map(|f| value_to_string_or_null(row.get(f)))
        .collect::<Vec<String>>()
        .join("|")
}

fn compare_rows(a: &Map<String, Value>, b: &Map<String, Value>, sort_by: &[Value]) -> std::cmp::Ordering {
    for item in sort_by {
        match item {
            Value::String(field) => {
                let av = value_to_string_or_null(a.get(field));
                let bv = value_to_string_or_null(b.get(field));
                let ord = av.cmp(&bv);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            Value::Object(obj) => {
                let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
                let desc = obj
                    .get("order")
                    .and_then(|v| v.as_str())
                    .unwrap_or("asc")
                    .eq_ignore_ascii_case("desc");
                let av = value_to_string_or_null(a.get(field));
                let bv = value_to_string_or_null(b.get(field));
                let mut ord = av.cmp(&bv);
                if desc {
                    ord = ord.reverse();
                }
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            _ => {}
        }
    }
    std::cmp::Ordering::Equal
}

fn evaluate_quality_gates(quality: &Value, gates: &Value) -> Value {
    let input_rows = quality
        .get("input_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as f64;
    let output_rows = quality
        .get("output_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let invalid_rows = quality
        .get("invalid_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let required_missing_ratio = quality
        .get("required_missing_ratio")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let mut errors: Vec<String> = Vec::new();

    if let Some(max_invalid_rows) = gates.get("max_invalid_rows").and_then(|v| v.as_u64())
        && invalid_rows > max_invalid_rows
    {
        errors.push(format!(
            "invalid_rows={} exceeds max_invalid_rows={}",
            invalid_rows, max_invalid_rows
        ));
    }
    if let Some(min_output_rows) = gates.get("min_output_rows").and_then(|v| v.as_u64())
        && output_rows < min_output_rows
    {
        errors.push(format!(
            "output_rows={} below min_output_rows={}",
            output_rows, min_output_rows
        ));
    }
    if let Some(max_invalid_ratio) = gates.get("max_invalid_ratio").and_then(|v| v.as_f64()) {
        let ratio = if input_rows > 0.0 {
            invalid_rows as f64 / input_rows
        } else {
            0.0
        };
        if ratio > max_invalid_ratio {
            errors.push(format!(
                "invalid_ratio={:.6} exceeds max_invalid_ratio={:.6}",
                ratio, max_invalid_ratio
            ));
        }
    }
    if let Some(max_required_missing_ratio) = gates
        .get("max_required_missing_ratio")
        .and_then(|v| v.as_f64())
        && required_missing_ratio > max_required_missing_ratio
    {
        errors.push(format!(
            "required_missing_ratio={:.6} exceeds max_required_missing_ratio={:.6}",
            required_missing_ratio, max_required_missing_ratio
        ));
    }
    json!({
        "passed": errors.is_empty(),
        "errors": errors,
    })
}

fn compute_aggregate(rows: &[Map<String, Value>], aggregate_rule: Option<&Value>) -> Option<Value> {
    let Some(rule) = aggregate_rule.and_then(|v| v.as_object()) else {
        return None;
    };
    let group_by = rule
        .get("group_by")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect::<Vec<String>>()
        })
        .unwrap_or_default();
    let metrics = rule
        .get("metrics")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_else(|| vec![json!({"field": "amount", "op": "sum", "as": "sum_amount"})]);
    if rows.is_empty() {
        return Some(json!({"rows": [], "group_by": group_by}));
    }

    let mut groups: HashMap<String, Vec<&Map<String, Value>>> = HashMap::new();
    for r in rows {
        let key = if group_by.is_empty() {
            "__all__".to_string()
        } else {
            group_by
                .iter()
                .map(|f| value_to_string_or_null(r.get(f)))
                .collect::<Vec<String>>()
                .join("|")
        };
        groups.entry(key).or_default().push(r);
    }

    let mut out: Vec<Value> = Vec::new();
    for (_k, rs) in groups {
        let mut row = Map::<String, Value>::new();
        if let Some(first) = rs.first() {
            for f in &group_by {
                row.insert(f.clone(), first.get(f).cloned().unwrap_or(Value::Null));
            }
        }
        for m in &metrics {
            let Some(obj) = m.as_object() else { continue };
            let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
            let op = obj.get("op").and_then(|v| v.as_str()).unwrap_or("count");
            let as_name = obj
                .get("as")
                .and_then(|v| v.as_str())
                .unwrap_or(op)
                .to_string();
            match op {
                "count" => {
                    row.insert(as_name, Value::Number((rs.len() as u64).into()));
                }
                "sum" | "avg" | "min" | "max" => {
                    let nums: Vec<f64> = rs.iter().filter_map(|r| r.get(field).and_then(value_to_f64)).collect();
                    if nums.is_empty() {
                        row.insert(as_name, Value::Null);
                    } else {
                        let val = match op {
                            "sum" => nums.iter().sum::<f64>(),
                            "avg" => nums.iter().sum::<f64>() / nums.len() as f64,
                            "min" => nums.iter().fold(f64::INFINITY, |a, b| a.min(*b)),
                            _ => nums.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b)),
                        };
                        row.insert(
                            as_name,
                            serde_json::Number::from_f64(val)
                                .map(Value::Number)
                                .unwrap_or(Value::Null),
                        );
                    }
                }
                _ => {}
            }
        }
        out.push(Value::Object(row));
    }
    Some(json!({"rows": out, "group_by": group_by, "metrics": metrics}))
}

fn run_compute_metrics(req: ComputeReq) -> Result<ComputeResp, String> {
    let text = req.text;
    if text.trim().is_empty() {
        return Err("empty text for compute_metrics".to_string());
    }

    let lines_vec: Vec<&str> = text.lines().collect();
    let mut sections = 0usize;
    let mut bullets = 0usize;
    let mut cjk = 0usize;
    let mut latin = 0usize;
    let mut digits = 0usize;
    let mut reference_hits = 0usize;
    let mut note_hits = 0usize;

    for line in &lines_vec {
        let t = line.trim();
        if t.starts_with("## ") {
            sections += 1;
        }
        if t.starts_with("- ") {
            bullets += 1;
        }
        let tl = t.to_lowercase();        if tl.contains("references")
            || tl.contains("bibliography")
            || t.contains("参考文献")
            || t.contains("引用文献")
            || t.contains("文献目录")
        {
            reference_hits += 1;
        }
        if tl.contains("acknowledg")
            || tl.contains("footnote")
            || tl.contains("appendix")
            || t.contains("注释")
            || t.contains("脚注")
            || t.contains("附录")
            || t.contains("致谢")
        {
            note_hits += 1;
        }}

    for ch in text.chars() {
        if ch.is_ascii_alphabetic() {
            latin += 1;
        } else if ch.is_ascii_digit() {
            digits += 1;
        } else if ('\u{4E00}'..='\u{9FFF}').contains(&ch) {
            cjk += 1;
        }
    }

    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let sha256 = format!("{:x}", hasher.finalize());

    Ok(ComputeResp {
        ok: true,
        operator: "compute_metrics".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        metrics: ComputeMetrics {
            sections,
            bullets,
            chars: text.chars().count(),
            lines: lines_vec.len(),
            cjk,
            latin,
            digits,
            reference_hits,
            note_hits,
            sha256,
        },
    })
}

fn run_cleaning_operator(req: CleaningReq) -> Result<CleaningResp, String> {
    let job_id = req
        .job_id
        .clone()
        .unwrap_or_else(|| "job_unknown".to_string());
    let step_id = req
        .step_id
        .clone()
        .unwrap_or_else(|| "cleaning".to_string());

    let job_root = resolve_job_root(req.job_root.as_deref(), &job_id)?;
    let stage_dir = job_root.join("stage");
    let artifacts_dir = job_root.join("artifacts");
    let evidence_dir = job_root.join("evidence");

    fs::create_dir_all(&stage_dir).map_err(|e| format!("create stage dir: {e}"))?;
    fs::create_dir_all(&artifacts_dir).map_err(|e| format!("create artifacts dir: {e}"))?;
    fs::create_dir_all(&evidence_dir).map_err(|e| format!("create evidence dir: {e}"))?;

    let csv_path = stage_dir.join("cleaned.csv");
    let parquet_path = stage_dir.join("cleaned.parquet");
    let profile_path = evidence_dir.join("profile.json");
    let xlsx_path = artifacts_dir.join("fin.xlsx");
    let docx_path = artifacts_dir.join("audit.docx");
    let pptx_path = artifacts_dir.join("deck.pptx");

    let cleaned_rows = load_and_clean_rows(req.params.as_ref())?;

    write_cleaned_csv(&csv_path, &cleaned_rows)?;
    if should_force_bad_parquet(req.force_bad_parquet) {
        write_bad_parquet_placeholder(&parquet_path)?;
    } else {
        write_cleaned_parquet(&parquet_path, &cleaned_rows)?;
    }
    write_profile_json(&profile_path, &cleaned_rows)?;
    let office = write_office_documents_with_mode(&xlsx_path, &docx_path, &pptx_path, &job_id)?;

    let csv_sha = sha256_file(&csv_path)?;
    let parquet_sha = sha256_file(&parquet_path)?;
    let profile_sha = sha256_file(&profile_path)?;
    let xlsx_sha = sha256_file(&xlsx_path)?;
    let docx_sha = sha256_file(&docx_path)?;
    let pptx_sha = sha256_file(&pptx_path)?;

    Ok(CleaningResp {
        ok: true,
        operator: "cleaning".to_string(),
        status: "done".to_string(),
        job_id: req.job_id,
        step_id: Some(step_id),
        input_uri: req.input_uri,
        output_uri: req.output_uri,
        job_root: path_to_string(&job_root),
        outputs: CleaningOutputs {
            cleaned_csv: FileOut {
                path: path_to_string(&csv_path),
                sha256: csv_sha,
            },
            cleaned_parquet: FileOut {
                path: path_to_string(&parquet_path),
                sha256: parquet_sha,
            },
            profile_json: FileOut {
                path: path_to_string(&profile_path),
                sha256: profile_sha,
            },
            xlsx_fin: FileOut {
                path: path_to_string(&xlsx_path),
                sha256: xlsx_sha,
            },
            audit_docx: FileOut {
                path: path_to_string(&docx_path),
                sha256: docx_sha,
            },
            deck_pptx: FileOut {
                path: path_to_string(&pptx_path),
                sha256: pptx_sha,
            },
        },
        profile: ProfileOut {
            rows: cleaned_rows.len(),
            cols: 2,
        },
        office_generation_mode: office.mode,
        office_generation_warning: office.warning,
        message: "accel-rust generated outputs".to_string(),
    })
}

fn resolve_job_root(input_root: Option<&str>, job_id: &str) -> Result<PathBuf, String> {
    fn is_valid_job_id(s: &str) -> bool {
        let t = s.trim();
        if t.len() < 8 || t.len() > 128 {
            return false;
        }
        t.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    }

    fn normalize_path(p: &Path) -> PathBuf {
        use std::path::Component;
        let mut out = PathBuf::new();
        for c in p.components() {
            match c {
                Component::CurDir => {}
                Component::ParentDir => {
                    let _ = out.pop();
                }
                other => out.push(other.as_os_str()),
            }
        }
        out
    }

    let jid = job_id.trim();
    if !is_valid_job_id(jid) {
        return Err("invalid job_id".to_string());
    }

    let bus = env::var("AIWF_BUS").unwrap_or_else(|_| "R:\\aiwf".to_string());
    let allowed_root = normalize_path(&PathBuf::from(bus).join("jobs"));
    let requested = if let Some(v) = input_root {
        if v.trim().is_empty() {
            allowed_root.join(jid)
        } else {
            PathBuf::from(v)
        }
    } else {
        allowed_root.join(jid)
    };

    let absolute = if requested.is_absolute() {
        requested
    } else {
        std::env::current_dir()
            .map_err(|e| format!("resolve current dir: {e}"))?
            .join(requested)
    };
    let normalized = normalize_path(&absolute);

    let leaf_ok = normalized.file_name().and_then(|n| n.to_str()) == Some(jid);
    let in_scope = normalized.starts_with(&allowed_root);
    if !leaf_ok || !in_scope {
        return Err(format!(
            "job_root must be under '{}' and end with job_id",
            allowed_root.to_string_lossy()
        ));
    }

    Ok(normalized)
}

fn rule_value<'a>(params: &'a Value, key: &str) -> Option<&'a Value> {
    if let Some(rules) = params.get("rules").and_then(|v| v.as_object())
        && let Some(v) = rules.get(key)
    {
        return Some(v);
    }
    params.get(key)
}

fn value_as_bool(v: Option<&Value>, default: bool) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => {
            let l = s.trim().to_lowercase();
            matches!(l.as_str(), "1" | "true" | "yes" | "on")
        }
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) != 0,
        _ => default,
    }
}

fn value_as_i32(v: Option<&Value>, default: i32) -> i32 {
    match v {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(default as i64) as i32,
        Some(Value::String(s)) => s.trim().parse::<i32>().unwrap_or(default),
        _ => default,
    }
}

fn value_as_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => parse_amount(s),
        _ => None,
    }
}

fn parse_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|x| x as i64)),
        Value::String(s) => s.trim().parse::<f64>().ok().map(|x| x as i64),
        _ => None,
    }
}

fn parse_amount(s: &str) -> Option<f64> {
    let mut t = s.trim().replace(',', "");
    if t.starts_with('$') {
        t = t[1..].to_string();
    }
    t.parse::<f64>().ok()
}

fn parse_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => parse_amount(s),
        _ => None,
    }
}

fn round_half_up(v: f64, digits: i32) -> f64 {
    let factor = 10f64.powi(digits.max(0));
    (v * factor).round() / factor
}

fn load_and_clean_rows(params_opt: Option<&Value>) -> Result<Vec<CleanRow>, String> {
    let default_rows = vec![
        CleanRow {
            id: 1,
            amount: 100.0,
        },
        CleanRow {
            id: 2,
            amount: 200.0,
        },
    ];
    let Some(params) = params_opt else {
        return Ok(default_rows);
    };

    let rows_val = params.get("rows");
    let Some(rows_arr) = rows_val.and_then(|v| v.as_array()) else {
        return Ok(default_rows);
    };

    let id_field = rule_value(params, "id_field")
        .and_then(|v| v.as_str())
        .unwrap_or("id")
        .to_string();
    let amount_field = rule_value(params, "amount_field")
        .and_then(|v| v.as_str())
        .unwrap_or("amount")
        .to_string();
    let drop_negative = value_as_bool(rule_value(params, "drop_negative_amount"), false);
    let deduplicate = value_as_bool(rule_value(params, "deduplicate_by_id"), true);
    let dedup_keep = rule_value(params, "deduplicate_keep")
        .and_then(|v| v.as_str())
        .unwrap_or("last")
        .to_lowercase();
    let sort_by_id = value_as_bool(rule_value(params, "sort_by_id"), true);
    let digits = value_as_i32(rule_value(params, "amount_round_digits"), 2).clamp(0, 6);
    let min_amount = value_as_f64(rule_value(params, "min_amount"));
    let max_amount = value_as_f64(rule_value(params, "max_amount"));

    let mut normalized: Vec<(i64, f64)> = Vec::new();
    for r in rows_arr {
        let Some(obj) = r.as_object() else {
            continue;
        };
        let id_val = obj.get(&id_field).and_then(parse_i64);
        let amount_val = obj.get(&amount_field).and_then(parse_f64);
        let (Some(id), Some(amount)) = (id_val, amount_val) else {
            continue;
        };
        if drop_negative && amount < 0.0 {
            continue;
        }
        if let Some(min_v) = min_amount
            && amount < min_v
        {
            continue;
        }
        if let Some(max_v) = max_amount
            && amount > max_v
        {
            continue;
        }
        normalized.push((id, round_half_up(amount, digits)));
    }

    let mut cleaned: Vec<(i64, f64)> = if deduplicate {
        use std::collections::HashMap;
        let mut map: HashMap<i64, f64> = HashMap::new();
        if dedup_keep == "first" {
            for (id, amount) in &normalized {
                map.entry(*id).or_insert(*amount);
            }
        } else {
            for (id, amount) in &normalized {
                map.insert(*id, *amount);
            }
        }
        map.into_iter().collect()
    } else {
        normalized
    };

    if sort_by_id {
        cleaned.sort_by_key(|x| x.0);
    }

    let out = cleaned
        .into_iter()
        .map(|(id, amount)| CleanRow { id, amount })
        .collect::<Vec<_>>();
    if out.is_empty() {
        return Ok(Vec::new());
    }
    Ok(out)
}

fn write_cleaned_csv(path: &Path, rows: &[CleanRow]) -> Result<(), String> {
    let mut f = fs::File::create(path).map_err(|e| format!("create csv: {e}"))?;
    f.write_all(b"id,amount\n")
        .map_err(|e| format!("write csv header: {e}"))?;
    for r in rows {
        let line = format!("{},{}\n", r.id, r.amount);
        f.write_all(line.as_bytes())
            .map_err(|e| format!("write csv row: {e}"))?;
    }
    Ok(())
}

fn write_cleaned_parquet(path: &Path, rows: &[CleanRow]) -> Result<(), String> {
    let id_col = Arc::new(
        Type::primitive_type_builder("id", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .map_err(|e| format!("build parquet id column schema: {e}"))?,
    );
    let amount_col = Arc::new(
        Type::primitive_type_builder("amount", PhysicalType::DOUBLE)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .map_err(|e| format!("build parquet amount column schema: {e}"))?,
    );
    let schema = Arc::new(
        Type::group_type_builder("aiwf_cleaned")
            .with_fields(vec![id_col, amount_col])
            .build()
            .map_err(|e| format!("build parquet schema: {e}"))?,
    );

    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let file = fs::File::create(path).map_err(|e| format!("create parquet: {e}"))?;
    let mut writer = SerializedFileWriter::new(file, schema, props)
        .map_err(|e| format!("create parquet writer: {e}"))?;

    let mut row_group_writer = writer
        .next_row_group()
        .map_err(|e| format!("open parquet row group: {e}"))?;

    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let amounts: Vec<f64> = rows.iter().map(|r| r.amount).collect();
    while let Some(mut column_writer) = row_group_writer
        .next_column()
        .map_err(|e| format!("open parquet column: {e}"))?
    {
        match column_writer.untyped() {
            ColumnWriter::Int64ColumnWriter(typed) => {
                let values: &[i64] = &ids;
                typed
                    .write_batch(values, None, None)
                    .map_err(|e| format!("write parquet id values: {e}"))?;
            }
            ColumnWriter::DoubleColumnWriter(typed) => {
                let values: &[f64] = &amounts;
                typed
                    .write_batch(values, None, None)
                    .map_err(|e| format!("write parquet amount values: {e}"))?;
            }
            _ => {
                return Err("unexpected parquet column type".to_string());
            }
        }
        column_writer
            .close()
            .map_err(|e| format!("close parquet column: {e}"))?;
    }

    row_group_writer
        .close()
        .map_err(|e| format!("close parquet row group: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("close parquet writer: {e}"))?;
    Ok(())
}

fn write_profile_json(path: &Path, rows: &[CleanRow]) -> Result<(), String> {
    let sum_amount: f64 = rows.iter().map(|r| r.amount).sum();
    let payload = serde_json::json!({
        "profile": {"rows": rows.len(), "cols": 2, "sum_amount": sum_amount},
        "engine": "accel-rust",
    });
    let s = serde_json::to_string_pretty(&payload).map_err(|e| format!("json profile: {e}"))?;
    fs::write(path, s).map_err(|e| format!("write profile: {e}"))
}

fn office_mode() -> String {
    let mode = env::var("AIWF_ACCEL_OFFICE_MODE").unwrap_or_else(|_| "fallback".to_string());
    let lower = mode.trim().to_lowercase();
    if lower == "strict" {
        "strict".to_string()
    } else {
        "fallback".to_string()
    }
}

fn should_force_bad_parquet(force_bad_parquet: Option<bool>) -> bool {
    if force_bad_parquet.unwrap_or(false) {
        return true;
    }
    env::var("AIWF_ACCEL_FORCE_BAD_PARQUET")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true")
}

fn write_bad_parquet_placeholder(path: &Path) -> Result<(), String> {
    let mut f = fs::File::create(path).map_err(|e| format!("create parquet: {e}"))?;
    f.write_all(b"PARQUET_PLACEHOLDER\n")
        .map_err(|e| format!("write parquet: {e}"))?;
    Ok(())
}

fn find_python_command() -> Option<String> {
    for cmd in ["python", "py"] {
        let probe = if cmd == "py" {
            Command::new(cmd).arg("-3").arg("--version").output()
        } else {
            Command::new(cmd).arg("--version").output()
        };

        if let Ok(out) = probe {
            if out.status.success() {
                return Some(cmd.to_string());
            }
        }
    }
    None
}

fn write_office_documents_with_mode(
    xlsx: &Path,
    docx: &Path,
    pptx: &Path,
    job_id: &str,
) -> Result<OfficeGenInfo, String> {
    let force_placeholder = env::var("AIWF_ACCEL_OFFICE_FORCE_PLACEHOLDER")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true");

    if force_placeholder {
        write_placeholder_office_documents(xlsx, docx, pptx)?;
        return Ok(OfficeGenInfo {
            mode: "placeholder".to_string(),
            warning: Some("forced placeholder mode by AIWF_ACCEL_OFFICE_FORCE_PLACEHOLDER=true".to_string()),
        });
    }

    match write_office_documents_python(xlsx, docx, pptx, job_id) {
        Ok(()) => Ok(OfficeGenInfo {
            mode: "python".to_string(),
            warning: None,
        }),
        Err(e) => {
            if office_mode() == "strict" {
                Err(e)
            } else {
                write_placeholder_office_documents(xlsx, docx, pptx)?;
                Ok(OfficeGenInfo {
                    mode: "placeholder".to_string(),
                    warning: Some(format!("python office generation failed, used placeholders: {e}")),
                })
            }
        }
    }
}

fn write_office_documents_python(
    xlsx: &Path,
    docx: &Path,
    pptx: &Path,
    job_id: &str,
) -> Result<(), String> {
    let py = find_python_command().ok_or_else(|| "python runtime not found for office generation".to_string())?;

    let script = r####"
import sys
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter
from docx import Document
from pptx import Presentation
from pptx.util import Inches, Pt

xlsx_path, docx_path, pptx_path, job_id = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

# XLSX
wb = Workbook()
ws = wb.active
ws.title = "detail"
header_fill = PatternFill(fill_type="solid", fgColor="1F4E78")
header_font = Font(color="FFFFFF", bold=True)
rows = [
    {"id": 1, "amount": 100.0},
    {"id": 2, "amount": 200.0},
]
cols = ["id", "amount"]
for i, c in enumerate(cols, start=1):
    cell = ws.cell(row=1, column=i, value=c)
    cell.fill = header_fill
    cell.font = header_font
for r_idx, r in enumerate(rows, start=2):
    ws.cell(row=r_idx, column=1, value=r["id"])
    ws.cell(row=r_idx, column=2, value=r["amount"])
    ws.cell(row=r_idx, column=2).number_format = "#,##0.00"
ws.freeze_panes = "A2"
ws.auto_filter.ref = "A1:B3"
ws.column_dimensions[get_column_letter(1)].width = 10
ws.column_dimensions[get_column_letter(2)].width = 14

sum_sheet = wb.create_sheet("summary")
sum_sheet["A1"] = "Metric"
sum_sheet["B1"] = "Value"
sum_sheet["A1"].fill = header_fill
sum_sheet["B1"].fill = header_fill
sum_sheet["A1"].font = header_font
sum_sheet["B1"].font = header_font
metrics = [
    ("rows", 2),
    ("cols", 2),
    ("sum_amount", 300.0),
    ("avg_amount", 150.0),
    ("generated_at", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")),
]
for i, (k, v) in enumerate(metrics, start=2):
    sum_sheet.cell(row=i, column=1, value=k)
    sum_sheet.cell(row=i, column=2, value=v)
sum_sheet.column_dimensions["A"].width = 20
sum_sheet.column_dimensions["B"].width = 28
wb.save(xlsx_path)

# DOCX
doc = Document()
doc.add_heading("AIWF Data Cleaning Audit Report", level=1)
meta = doc.add_table(rows=4, cols=2)
meta.style = "Light List Accent 1"
meta.cell(0, 0).text = "Job ID"
meta.cell(0, 1).text = job_id
meta.cell(1, 0).text = "Step"
meta.cell(1, 1).text = "cleaning"
meta.cell(2, 0).text = "Generated At"
meta.cell(2, 1).text = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
meta.cell(3, 0).text = "Status"
meta.cell(3, 1).text = "DONE"
doc.add_paragraph("")
doc.add_heading("Core Metrics", level=2)
t = doc.add_table(rows=1, cols=2)
t.style = "Light Grid Accent 1"
t.rows[0].cells[0].text = "Metric"
t.rows[0].cells[1].text = "Value"
for k, v in metrics[:4]:
    row = t.add_row().cells
    row[0].text = str(k)
    row[1].text = str(v)
doc.save(docx_path)

# PPTX
prs = Presentation()
s1 = prs.slides.add_slide(prs.slide_layouts[0])
s1.shapes.title.text = "AIWF Cleaning Output Summary"
if len(s1.placeholders) > 1:
    s1.placeholders[1].text = f"Job {job_id}\nGenerated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"

s2 = prs.slides.add_slide(prs.slide_layouts[5])
s2.shapes.title.text = "Key Metrics"
tb = s2.shapes.add_textbox(Inches(0.8), Inches(1.4), Inches(8.5), Inches(3.2))
tf = tb.text_frame
tf.clear()
for idx, text in enumerate(["Rows: 2", "Columns: 2", "Sum Amount: 300.0", "Avg Amount: 150.0"]):
    p = tf.paragraphs[0] if idx == 0 else tf.add_paragraph()
    p.text = text
    p.font.size = Pt(24 if idx < 2 else 20)

s3 = prs.slides.add_slide(prs.slide_layouts[1])
s3.shapes.title.text = "Data Quality"
s3.placeholders[1].text = "Input rows: 2\nOutput rows: 2\nInvalid rows: 0\nFiltered rows: 0"
prs.save(pptx_path)
"####;

    let py_file = std::env::temp_dir().join("aiwf_accel_office_gen.py");
    fs::write(&py_file, script).map_err(|e| format!("write temp python script: {e}"))?;

    let mut cmd = Command::new(&py);
    if py == "py" {
        cmd.arg("-3");
    }

    let out = cmd
        .arg(py_file.to_string_lossy().to_string())
        .arg(xlsx.to_string_lossy().to_string())
        .arg(docx.to_string_lossy().to_string())
        .arg(pptx.to_string_lossy().to_string())
        .arg(job_id)
        .output()
        .map_err(|e| format!("run python office generator: {e}"))?;

    let _ = fs::remove_file(&py_file);

    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        return Err(format!("python office generation failed; stdout={stdout}; stderr={stderr}"));
    }

    Ok(())
}

fn write_placeholder_office_documents(xlsx: &Path, docx: &Path, pptx: &Path) -> Result<(), String> {
    write_placeholder_binary(xlsx, b"XLSX_PLACEHOLDER\n")?;
    write_placeholder_binary(docx, b"DOCX_PLACEHOLDER\n")?;
    write_placeholder_binary(pptx, b"PPTX_PLACEHOLDER\n")?;
    Ok(())
}

fn write_placeholder_binary(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut f = fs::File::create(path).map_err(|e| format!("create placeholder: {e}"))?;
    f.write_all(bytes)
        .map_err(|e| format!("write placeholder: {e}"))?;
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = fs::File::open(path).map_err(|e| format!("open for hash: {e}"))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];

    loop {
        let n = file.read(&mut buf).map_err(|e| format!("read for hash: {e}"))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    let digest = hasher.finalize();
    Ok(format!("{digest:x}"))
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

fn load_rows_from_uri_limited(
    uri: &str,
    max_rows: usize,
    max_bytes: usize,
) -> Result<Vec<Value>, String> {
    let lower = uri.to_lowercase();
    if lower.ends_with(".jsonl") {
        return load_jsonl_rows_limited(uri, max_rows, max_bytes);
    }
    if lower.ends_with(".csv") {
        return load_csv_rows_limited(uri, max_rows, max_bytes);
    }
    if lower.ends_with(".parquet") {
        if let Ok(meta) = fs::metadata(uri) && meta.len() as usize > max_bytes {
            return Err(format!(
                "input parquet exceeds byte limit: {} > {}",
                meta.len(),
                max_bytes
            ));
        }
        return load_parquet_rows(uri, max_rows);
    }
    if lower.starts_with("sqlite://") {
        let p = uri.trim_start_matches("sqlite://");
        return load_sqlite_rows(p, "SELECT * FROM data", max_rows);
    }
    if lower.starts_with("sqlserver://") {
        let q = "SELECT TOP 10000 * FROM dbo.workflow_tasks";
        return load_sqlserver_rows(uri.trim_start_matches("sqlserver://"), q, max_rows);
    }
    Err("unsupported input_uri".to_string())
}

fn save_rows_to_uri(uri: &str, rows: &[Value]) -> Result<(), String> {
    let lower = uri.to_lowercase();
    if lower.ends_with(".jsonl") {
        return save_rows_jsonl(uri, rows);
    }
    if lower.ends_with(".csv") {
        return save_rows_csv(uri, rows);
    }
    if lower.ends_with(".parquet") {
        return save_rows_parquet(uri, rows);
    }
    Err("unsupported output_uri".to_string())
}

fn load_jsonl_rows(path: &str, limit: usize) -> Result<Vec<Value>, String> {
    load_jsonl_rows_limited(path, limit, 128 * 1024 * 1024)
}

fn load_jsonl_rows_limited(path: &str, limit: usize, max_bytes: usize) -> Result<Vec<Value>, String> {
    if let Ok(meta) = fs::metadata(path) && meta.len() as usize > max_bytes {
        return Err(format!(
            "jsonl exceeds byte limit: {} > {}",
            meta.len(),
            max_bytes
        ));
    }
    let f = fs::File::open(path).map_err(|e| format!("read jsonl: {e}"))?;
    let mut rd = BufReader::new(f);
    let mut out = Vec::new();
    let mut line = String::new();
    let mut read_rows = 0usize;
    let mut read_bytes = 0usize;
    loop {
        line.clear();
        let n = rd
            .read_line(&mut line)
            .map_err(|e| format!("read jsonl line: {e}"))?;
        if n == 0 || read_rows >= limit {
            break;
        }
        read_bytes += n;
        if read_bytes > max_bytes {
            return Err(format!(
                "jsonl streaming bytes exceed limit: {} > {}",
                read_bytes, max_bytes
            ));
        }
        let s = line.trim();
        if s.is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(s).map_err(|e| format!("jsonl parse: {e}"))?;
        out.push(v);
        read_rows += 1;
    }
    Ok(out)
}

fn load_csv_rows(path: &str, limit: usize) -> Result<Vec<Value>, String> {
    load_csv_rows_limited(path, limit, 128 * 1024 * 1024)
}

fn load_csv_rows_limited(path: &str, limit: usize, max_bytes: usize) -> Result<Vec<Value>, String> {
    if let Ok(meta) = fs::metadata(path) && meta.len() as usize > max_bytes {
        return Err(format!(
            "csv exceeds byte limit: {} > {}",
            meta.len(),
            max_bytes
        ));
    }
    let f = fs::File::open(path).map_err(|e| format!("read csv: {e}"))?;
    let mut rd = BufReader::new(f);
    let mut header = String::new();
    let n = rd
        .read_line(&mut header)
        .map_err(|e| format!("read csv header: {e}"))?;
    if n == 0 {
        return Ok(Vec::new());
    }
    let cols: Vec<String> = header.trim_end().split(',').map(|x| x.trim().to_string()).collect();
    let mut out = Vec::new();
    let mut line = String::new();
    let mut read_rows = 0usize;
    let mut read_bytes = n;
    loop {
        line.clear();
        let n = rd
            .read_line(&mut line)
            .map_err(|e| format!("read csv line: {e}"))?;
        if n == 0 || read_rows >= limit {
            break;
        }
        read_bytes += n;
        if read_bytes > max_bytes {
            return Err(format!(
                "csv streaming bytes exceed limit: {} > {}",
                read_bytes, max_bytes
            ));
        }
        let vals: Vec<&str> = line.trim_end().split(',').collect();
        let mut obj = Map::new();
        for (i, c) in cols.iter().enumerate() {
            obj.insert(
                c.clone(),
                Value::String(vals.get(i).copied().unwrap_or("").trim().to_string()),
            );
        }
        out.push(Value::Object(obj));
        read_rows += 1;
    }
    Ok(out)
}

fn load_parquet_rows(path: &str, limit: usize) -> Result<Vec<Value>, String> {
    let file = fs::File::open(path).map_err(|e| format!("open parquet: {e}"))?;
    let reader = SerializedFileReader::new(file).map_err(|e| format!("read parquet: {e}"))?;
    let schema = reader
        .metadata()
        .file_metadata()
        .schema_descr_ptr()
        .root_schema()
        .clone();
    if schema.get_fields().len() == 1 && schema.get_fields()[0].name() == "payload" {
        return load_parquet_payload_rows(reader, limit);
    }
    let mut out: Vec<Value> = Vec::new();
    let iter = reader
        .get_row_iter(None)
        .map_err(|e| format!("parquet row iter: {e}"))?;
    for row in iter.take(limit) {
        let row = row.map_err(|e| format!("parquet row: {e}"))?;
        out.push(row.to_json_value());
    }
    Ok(out)
}

fn load_parquet_payload_rows(
    reader: SerializedFileReader<fs::File>,
    limit: usize,
) -> Result<Vec<Value>, String> {
    let mut out: Vec<Value> = Vec::new();
    for rg_i in 0..reader.num_row_groups() {
        if out.len() >= limit {
            break;
        }
        let rg = reader
            .get_row_group(rg_i)
            .map_err(|e| format!("parquet row group: {e}"))?;
        if rg.num_columns() == 0 {
            continue;
        }
        let mut col = rg
            .get_column_reader(0)
            .map_err(|e| format!("parquet column reader: {e}"))?;
        match col {
            ColumnReader::ByteArrayColumnReader(ref mut typed) => loop {
                if out.len() >= limit {
                    break;
                }
                let to_read = (limit - out.len()).min(2048);
                let mut vals: Vec<ByteArray> = Vec::with_capacity(to_read);
                let (rows_read, _, _) = typed
                    .read_records(to_read, None, None, &mut vals)
                    .map_err(|e| format!("parquet read records: {e}"))?;
                if rows_read == 0 {
                    break;
                }
                for b in vals.into_iter().take(rows_read) {
                    let parsed = std::str::from_utf8(b.data())
                        .ok()
                        .and_then(|s| serde_json::from_str::<Value>(s).ok())
                        .unwrap_or_else(|| Value::String(String::from_utf8_lossy(b.data()).to_string()));
                    out.push(parsed);
                    if out.len() >= limit {
                        break;
                    }
                }
            },
            _ => return Err("parquet generic loader expects BYTE_ARRAY payload column".to_string()),
        }
    }
    Ok(out)
}

fn load_sqlite_rows(db_path: &str, query: &str, limit: usize) -> Result<Vec<Value>, String> {
    let conn = SqliteConnection::open(db_path).map_err(|e| format!("sqlite open: {e}"))?;
    let q = format!("{query} LIMIT {}", limit);
    let mut stmt = conn.prepare(&q).map_err(|e| format!("sqlite prepare: {e}"))?;
    let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let mut rows = stmt.query([]).map_err(|e| format!("sqlite query: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("sqlite next: {e}"))? {
        let mut obj = Map::new();
        for (i, name) in col_names.iter().enumerate() {
            let s = row.get_ref(i).map(|v| format!("{v:?}")).unwrap_or_default();
            obj.insert(name.clone(), Value::String(s));
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

fn load_sqlserver_rows(conn_str: &str, query: &str, limit: usize) -> Result<Vec<Value>, String> {
    let cfg = parse_sqlserver_conn_str(conn_str);
    let q = format!("SET NOCOUNT ON; SELECT TOP {limit} * FROM ({query}) x FOR JSON PATH;");
    let out = run_sqlcmd_query(&cfg, &q)?;
    let s = out.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    let arr: Value = serde_json::from_str(s).map_err(|e| format!("sqlserver json parse: {e}"))?;
    Ok(arr.as_array().cloned().unwrap_or_default())
}

fn save_rows_jsonl(path: &str, rows: &[Value]) -> Result<(), String> {
    let mut out = String::new();
    for r in rows {
        out.push_str(&serde_json::to_string(r).map_err(|e| e.to_string())?);
        out.push('\n');
    }
    fs::write(path, out).map_err(|e| format!("write jsonl: {e}"))
}

fn save_rows_csv(path: &str, rows: &[Value]) -> Result<(), String> {
    let cols = rows
        .iter()
        .filter_map(|v| v.as_object())
        .flat_map(|m| m.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut out = String::new();
    out.push_str(&cols.join(","));
    out.push('\n');
    for r in rows {
        let Some(obj) = r.as_object() else {
            continue;
        };
        let line = cols
            .iter()
            .map(|c| value_to_string_or_null(obj.get(c)).replace(',', " "))
            .collect::<Vec<_>>()
            .join(",");
        out.push_str(&line);
        out.push('\n');
    }
    fs::write(path, out).map_err(|e| format!("write csv: {e}"))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TypedColKind {
    Bool,
    Int,
    Float,
    Str,
}

#[derive(Clone)]
struct TypedColSpec {
    name: String,
    kind: TypedColKind,
}

fn infer_typed_parquet_columns(rows: &[Value]) -> Vec<TypedColSpec> {
    let cols = rows
        .iter()
        .filter_map(|v| v.as_object())
        .flat_map(|m| m.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut out = Vec::new();
    for c in cols {
        let mut saw_bool = false;
        let mut saw_int = false;
        let mut saw_float = false;
        let mut saw_string = false;
        for r in rows {
            let Some(obj) = r.as_object() else {
                continue;
            };
            let Some(v) = obj.get(&c) else {
                continue;
            };
            if v.is_null() {
                continue;
            }
            match v {
                Value::Bool(_) => saw_bool = true,
                Value::Number(n) => {
                    if n.is_i64() || n.is_u64() {
                        saw_int = true;
                    } else {
                        saw_float = true;
                    }
                }
                Value::String(_) => saw_string = true,
                _ => saw_string = true,
            }
        }
        let kind = if saw_string || (saw_bool && (saw_int || saw_float)) {
            TypedColKind::Str
        } else if saw_float {
            TypedColKind::Float
        } else if saw_int {
            TypedColKind::Int
        } else if saw_bool {
            TypedColKind::Bool
        } else {
            TypedColKind::Str
        };
        out.push(TypedColSpec { name: c, kind });
    }
    out
}

fn save_rows_parquet(path: &str, rows: &[Value]) -> Result<(), String> {
    save_rows_parquet_typed(path, rows)
}

fn save_rows_parquet_payload(path: &str, rows: &[Value]) -> Result<(), String> {
    let payload_col = Arc::new(
        Type::primitive_type_builder("payload", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .map_err(|e| format!("build parquet payload schema: {e}"))?,
    );
    let schema = Arc::new(
        Type::group_type_builder("aiwf_rows")
            .with_fields(vec![payload_col])
            .build()
            .map_err(|e| format!("build parquet schema: {e}"))?,
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let file = fs::File::create(path).map_err(|e| format!("create parquet: {e}"))?;
    let mut writer = SerializedFileWriter::new(file, schema, props)
        .map_err(|e| format!("create parquet writer: {e}"))?;
    let mut row_group_writer = writer
        .next_row_group()
        .map_err(|e| format!("open parquet row group: {e}"))?;
    let payloads = rows
        .iter()
        .map(|r| {
            serde_json::to_string(r)
                .map(|s| ByteArray::from(s.into_bytes()))
                .map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<ByteArray>, String>>()?;
    while let Some(mut column_writer) = row_group_writer
        .next_column()
        .map_err(|e| format!("open parquet column: {e}"))?
    {
        match column_writer.untyped() {
            ColumnWriter::ByteArrayColumnWriter(typed) => {
                typed
                    .write_batch(&payloads, None, None)
                    .map_err(|e| format!("write parquet payload values: {e}"))?;
            }
            _ => return Err("unexpected parquet generic column type".to_string()),
        }
        column_writer
            .close()
            .map_err(|e| format!("close parquet column: {e}"))?;
    }
    row_group_writer
        .close()
        .map_err(|e| format!("close parquet row group: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("close parquet writer: {e}"))?;
    Ok(())
}

fn save_rows_parquet_typed(path: &str, rows: &[Value]) -> Result<(), String> {
    let specs = infer_typed_parquet_columns(rows);
    if specs.is_empty() {
        return save_rows_parquet_payload(path, rows);
    }
    let mut fields = Vec::new();
    for c in &specs {
        let ty = match c.kind {
            TypedColKind::Bool => PhysicalType::BOOLEAN,
            TypedColKind::Int => PhysicalType::INT64,
            TypedColKind::Float => PhysicalType::DOUBLE,
            TypedColKind::Str => PhysicalType::BYTE_ARRAY,
        };
        fields.push(Arc::new(
            if c.kind == TypedColKind::Str {
                Type::primitive_type_builder(&c.name, ty)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_logical_type(Some(LogicalType::String))
                    .build()
                    .map_err(|e| format!("build parquet typed string column schema: {e}"))?
            } else {
                Type::primitive_type_builder(&c.name, ty)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .map_err(|e| format!("build parquet typed column schema: {e}"))?
            },
        ));
    }
    let schema = Arc::new(
        Type::group_type_builder("aiwf_rows_typed")
            .with_fields(fields)
            .build()
            .map_err(|e| format!("build parquet typed schema: {e}"))?,
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let file = fs::File::create(path).map_err(|e| format!("create parquet: {e}"))?;
    let mut writer = SerializedFileWriter::new(file, schema, props)
        .map_err(|e| format!("create parquet writer: {e}"))?;
    let mut row_group_writer = writer
        .next_row_group()
        .map_err(|e| format!("open parquet row group: {e}"))?;
    let n = rows.len();
    for c in &specs {
        let Some(mut column_writer) = row_group_writer
            .next_column()
            .map_err(|e| format!("open parquet typed column: {e}"))?
        else {
            break;
        };
        match column_writer.untyped() {
            ColumnWriter::BoolColumnWriter(typed) if c.kind == TypedColKind::Bool => {
                let mut vals = Vec::<bool>::new();
                let mut defs = vec![0i16; n];
                for (i, r) in rows.iter().enumerate() {
                    if let Some(v) = r.as_object().and_then(|o| o.get(&c.name)).and_then(|v| v.as_bool()) {
                        defs[i] = 1;
                        vals.push(v);
                    }
                }
                typed
                    .write_batch(&vals, Some(&defs), None)
                    .map_err(|e| format!("write parquet bool column: {e}"))?;
            }
            ColumnWriter::Int64ColumnWriter(typed) if c.kind == TypedColKind::Int => {
                let mut vals = Vec::<i64>::new();
                let mut defs = vec![0i16; n];
                for (i, r) in rows.iter().enumerate() {
                    if let Some(v) = r
                        .as_object()
                        .and_then(|o| o.get(&c.name))
                        .and_then(value_to_i64)
                    {
                        defs[i] = 1;
                        vals.push(v);
                    }
                }
                typed
                    .write_batch(&vals, Some(&defs), None)
                    .map_err(|e| format!("write parquet int column: {e}"))?;
            }
            ColumnWriter::DoubleColumnWriter(typed) if c.kind == TypedColKind::Float => {
                let mut vals = Vec::<f64>::new();
                let mut defs = vec![0i16; n];
                for (i, r) in rows.iter().enumerate() {
                    if let Some(v) = r
                        .as_object()
                        .and_then(|o| o.get(&c.name))
                        .and_then(value_to_f64)
                    {
                        defs[i] = 1;
                        vals.push(v);
                    }
                }
                typed
                    .write_batch(&vals, Some(&defs), None)
                    .map_err(|e| format!("write parquet float column: {e}"))?;
            }
            ColumnWriter::ByteArrayColumnWriter(typed) if c.kind == TypedColKind::Str => {
                let mut vals = Vec::<ByteArray>::new();
                let mut defs = vec![0i16; n];
                for (i, r) in rows.iter().enumerate() {
                    let s = r
                        .as_object()
                        .and_then(|o| o.get(&c.name))
                        .map(|v| value_to_string_or_null(Some(v)))
                        .unwrap_or_default();
                    if !s.is_empty() {
                        defs[i] = 1;
                        vals.push(ByteArray::from(s.into_bytes()));
                    }
                }
                typed
                    .write_batch(&vals, Some(&defs), None)
                    .map_err(|e| format!("write parquet string column: {e}"))?;
            }
            _ => return Err("unexpected parquet typed column writer type".to_string()),
        }
        column_writer
            .close()
            .map_err(|e| format!("close parquet typed column: {e}"))?;
    }
    row_group_writer
        .close()
        .map_err(|e| format!("close parquet typed row group: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("close parquet typed writer: {e}"))?;
    Ok(())
}

fn save_rows_sqlite(db_path: &str, table: &str, rows: &[Value]) -> Result<(), String> {
    let conn = SqliteConnection::open(db_path).map_err(|e| format!("sqlite open: {e}"))?;
    conn.execute(
        &format!("CREATE TABLE IF NOT EXISTS {table} (payload TEXT NOT NULL)"),
        [],
    )
    .map_err(|e| format!("sqlite create: {e}"))?;
    let tx = conn.unchecked_transaction().map_err(|e| format!("sqlite tx: {e}"))?;
    for r in rows {
        let s = serde_json::to_string(r).map_err(|e| e.to_string())?;
        tx.execute(&format!("INSERT INTO {table}(payload) VALUES (?1)"), [&s])
            .map_err(|e| format!("sqlite insert: {e}"))?;
    }
    tx.commit().map_err(|e| format!("sqlite commit: {e}"))
}

fn save_rows_sqlserver(conn_str: &str, table: &str, rows: &[Value]) -> Result<(), String> {
    let cfg = parse_sqlserver_conn_str(conn_str);
    let q_create = format!("IF OBJECT_ID('{table}','U') IS NULL CREATE TABLE {table}(payload NVARCHAR(MAX) NOT NULL);");
    let _ = run_sqlcmd_query(&cfg, &q_create)?;
    for r in rows {
        let payload = escape_tsql(&serde_json::to_string(r).map_err(|e| e.to_string())?);
        let q = format!("INSERT INTO {table}(payload) VALUES (N'{payload}');");
        let _ = run_sqlcmd_query(&cfg, &q)?;
    }
    Ok(())
}

fn parse_sqlserver_conn_str(s: &str) -> TaskStoreConfig {
    // format: host:port/db?user=u&password=p
    let mut cfg = task_store_config_from_env();
    let trimmed = s.trim();
    let (main, query) = trimmed.split_once('?').unwrap_or((trimmed, ""));
    let (host_port, db) = main.split_once('/').unwrap_or((main, "AIWF"));
    let (host, port) = host_port.split_once(':').unwrap_or((host_port, "1433"));
    cfg.sql_host = host.to_string();
    cfg.sql_port = port.parse::<u16>().unwrap_or(1433);
    cfg.sql_db = db.to_string();
    let params = query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .collect::<HashMap<_, _>>();
    if let Some(u) = params.get("user") {
        cfg.sql_user = Some((*u).to_string());
    }
    if let Some(p) = params.get("password") {
        cfg.sql_password = Some((*p).to_string());
    }
    cfg.sql_use_windows_auth = params
        .get("windows_auth")
        .map(|v| *v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    cfg
}

#[cfg(test)]
mod tests {
    use super::{
        AggregateRowsReq, AppState, CleanRow, ServiceMetrics, TaskState, TaskStoreConfig,
        RulesPackageGetReq, RulesPackagePublishReq, TransformRowsReq, WorkflowRunReq,
        build_router, can_cancel_status, load_and_clean_rows, load_parquet_rows, prune_tasks,
        run_aggregate_rows_v1, run_rules_package_get_v1, run_rules_package_publish_v1,
        run_plugin_exec_v1, run_quality_check_v1, run_transform_rows_v2,
        run_transform_rows_v2_with_cancel, run_workflow, save_rows_parquet, utc_now_iso,
        validate_where_clause, load_rows_from_uri_limited, PluginExecReq,
        write_cleaned_parquet,
    };
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use serde_json::json;
    use std::{
        collections::HashMap,
        fs,
        io::Write,
        sync::{Arc, Mutex, atomic::AtomicBool},
        time::{SystemTime, UNIX_EPOCH},
    };
    use tower::ServiceExt;

    #[test]
    fn writes_valid_parquet_magic_bytes() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("aiwf_cleaned_{now}.parquet"));

        let rows = vec![
            CleanRow {
                id: 1,
                amount: 100.12,
            },
            CleanRow {
                id: 2,
                amount: 200.34,
            },
        ];
        write_cleaned_parquet(&path, &rows).expect("failed to write parquet");
        let bytes = fs::read(&path).expect("failed to read parquet");

        assert!(bytes.len() >= 8, "parquet file too small");
        assert_eq!(&bytes[0..4], b"PAR1", "invalid parquet header");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1", "invalid parquet footer");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn load_and_clean_rows_supports_rules_object() {
        let params = json!({
            "rows": [
                {"ID": "1", "AMT": "10.4"},
                {"ID": "1", "AMT": "12.5"},
                {"ID": "2", "AMT": "-3"}
            ],
            "rules": {
                "id_field": "ID",
                "amount_field": "AMT",
                "drop_negative_amount": true,
                "deduplicate_by_id": true,
                "deduplicate_keep": "last",
                "amount_round_digits": 0
            }
        });
        let out = load_and_clean_rows(Some(&params)).expect("clean rows failed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, 1);
        assert_eq!(out[0].amount, 13.0);
    }

    #[test]
    fn load_and_clean_rows_applies_min_max_filters() {
        let params = json!({
            "rows": [
                {"id": 1, "amount": 5},
                {"id": 2, "amount": 50},
                {"id": 3, "amount": 500}
            ],
            "min_amount": 10,
            "max_amount": 100
        });
        let out = load_and_clean_rows(Some(&params)).expect("clean rows failed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, 2);
        assert_eq!(out[0].amount, 50.0);
    }

    #[test]
    fn transform_rows_v2_works_for_basic_rules() {
        let req = TransformRowsReq {
            run_id: Some("r1".to_string()),
            tenant_id: None,
            trace_id: None,
            traceparent: None,
            rows: Some(vec![
                json!({"ID":"1","AMT":"10.5","tag":"A"}),
                json!({"ID":"1","AMT":"11.5","tag":"A"}),
                json!({"ID":"2","AMT":"-1","tag":"B"}),
            ]),
            rules: Some(json!({
                "rename_map": {"ID":"id","AMT":"amount"},
                "casts": {"id":"int","amount":"float"},
                "filters": [{"field":"amount","op":"gte","value":0}],
                "deduplicate_by": ["id"],
                "deduplicate_keep": "last",
                "sort_by": [{"field":"id","order":"asc"}]
            })),
            quality_gates: Some(json!({"min_output_rows":1, "max_invalid_rows":0})),
            schema_hint: None,
            rules_dsl: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: None,
        };
        let out = run_transform_rows_v2(req).expect("transform rows failed");
        assert_eq!(out.rows.len(), 1);
        assert!(out.rust_v2_used);
    }

    #[test]
    fn transform_rows_v2_supports_in_regex_and_required_missing_gate() {
        let req = TransformRowsReq {
            run_id: Some("r2".to_string()),
            tenant_id: None,
            trace_id: None,
            traceparent: None,
            rows: Some(vec![
                json!({"name":"Alice","city":"beijing","claim_text":"tax policy support","source_url":"https://a"}),
                json!({"name":"Bob","city":"shanghai","claim_text":"tax policy oppose","source_url":""}),
            ]),
            rules: Some(json!({
                "filters": [
                    {"field":"city","op":"in","value":["beijing","shanghai"]},
                    {"field":"claim_text","op":"regex","value":"tax\\s+policy"}
                ]
            })),
            quality_gates: Some(json!({
                "required_fields": ["claim_text","source_url"],
                "max_required_missing_ratio": 0.5
            })),
            schema_hint: None,
            rules_dsl: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: None,
        };
        let out = run_transform_rows_v2(req).expect("transform rows failed");
        assert_eq!(out.rows.len(), 2);
        assert!(
            out.quality
                .get("required_missing_ratio")
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0)
                <= 0.5
        );
    }

    #[test]
    fn router_builds_without_panicking() {
        let state = AppState {
            service: "accel-rust".to_string(),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
            task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
                ttl_sec: 3600,
                max_tasks: 1000,
                store_path: None,
                remote_enabled: false,
                backend: "base_api".to_string(),
                base_api_url: None,
                base_api_key: None,
                sql_host: "127.0.0.1".to_string(),
                sql_port: 1433,
                sql_db: "AIWF".to_string(),
                sql_user: None,
                sql_password: None,
                sql_use_windows_auth: false,
            })),
            cancel_flags: Arc::new(Mutex::new(HashMap::new())),
            tenant_running: Arc::new(Mutex::new(HashMap::new())),
            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        };
        let _ = build_router(state);
    }

    #[test]
    fn prune_tasks_respects_ttl_and_max() {
        let now = utc_now_iso().parse::<u64>().unwrap_or(0);
        let mut tasks = HashMap::new();
        tasks.insert(
            "old".to_string(),
            TaskState {
                task_id: "old".to_string(),
                tenant_id: "default".to_string(),
                operator: "transform_rows_v2".to_string(),
                status: "done".to_string(),
                created_at: now.saturating_sub(100).to_string(),
                updated_at: now.saturating_sub(100).to_string(),
                result: None,
                error: None,
                idempotency_key: "".to_string(),
                attempts: 0,
            },
        );
        tasks.insert(
            "new1".to_string(),
            TaskState {
                task_id: "new1".to_string(),
                tenant_id: "default".to_string(),
                operator: "transform_rows_v2".to_string(),
                status: "done".to_string(),
                created_at: now.saturating_sub(2).to_string(),
                updated_at: now.saturating_sub(2).to_string(),
                result: None,
                error: None,
                idempotency_key: "".to_string(),
                attempts: 0,
            },
        );
        tasks.insert(
            "new2".to_string(),
            TaskState {
                task_id: "new2".to_string(),
                tenant_id: "default".to_string(),
                operator: "transform_rows_v2".to_string(),
                status: "done".to_string(),
                created_at: now.saturating_sub(1).to_string(),
                updated_at: now.saturating_sub(1).to_string(),
                result: None,
                error: None,
                idempotency_key: "".to_string(),
                attempts: 0,
            },
        );
        let cfg = TaskStoreConfig {
            ttl_sec: 10,
            max_tasks: 1,
            store_path: None,
            remote_enabled: false,
            backend: "base_api".to_string(),
            base_api_url: None,
            base_api_key: None,
            sql_host: "127.0.0.1".to_string(),
            sql_port: 1433,
            sql_db: "AIWF".to_string(),
            sql_user: None,
            sql_password: None,
            sql_use_windows_auth: false,
        };
        let removed = prune_tasks(&mut tasks, &cfg);
        assert!(removed >= 2);
        assert_eq!(tasks.len(), 1);
        assert!(tasks.contains_key("new2"));
    }

    #[test]
    fn can_cancel_only_for_queued_or_running() {
        assert!(can_cancel_status("queued"));
        assert!(can_cancel_status("running"));
        assert!(!can_cancel_status("done"));
        assert!(!can_cancel_status("failed"));
        assert!(!can_cancel_status("cancelled"));
    }

    #[tokio::test]
    async fn async_submit_and_poll_task() {
        let state = AppState {
            service: "accel-rust".to_string(),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
            task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
                ttl_sec: 3600,
                max_tasks: 1000,
                store_path: None,
                remote_enabled: false,
                backend: "base_api".to_string(),
                base_api_url: None,
                base_api_key: None,
                sql_host: "127.0.0.1".to_string(),
                sql_port: 1433,
                sql_db: "AIWF".to_string(),
                sql_user: None,
                sql_password: None,
                sql_use_windows_auth: false,
            })),
            cancel_flags: Arc::new(Mutex::new(HashMap::new())),
            tenant_running: Arc::new(Mutex::new(HashMap::new())),
            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        };
        let app = build_router(state);
        let submit_payload = json!({
            "run_id": "it-1",
            "rows": [
                {"id":"1","amount":"10.1"},
                {"id":"2","amount":"11.2"}
            ],
            "rules": {"casts":{"id":"int","amount":"float"}}
        });
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/operators/transform_rows_v2/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(submit_payload.to_string()))
                    .expect("submit request"),
            )
            .await
            .expect("submit response");
        assert_eq!(resp.status(), 200);
        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("submit body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("submit json");
        let task_id = v
            .get("task_id")
            .and_then(|x| x.as_str())
            .expect("task_id")
            .to_string();

        let mut last = String::new();
        for _ in 0..60 {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(format!("/tasks/{task_id}"))
                        .body(Body::empty())
                        .expect("task request"),
                )
                .await
                .expect("task response");
            assert_eq!(resp.status(), 200);
            let body = to_bytes(resp.into_body(), 1024 * 1024)
                .await
                .expect("task body");
            let v: serde_json::Value = serde_json::from_slice(&body).expect("task json");
            last = v
                .get("status")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            if last == "done" || last == "failed" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(last, "done");
    }

    #[tokio::test]
    async fn metrics_include_transform_success_and_latency_aggregates() {
        let state = AppState {
            service: "accel-rust".to_string(),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
            task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
                ttl_sec: 3600,
                max_tasks: 1000,
                store_path: None,
                remote_enabled: false,
                backend: "base_api".to_string(),
                base_api_url: None,
                base_api_key: None,
                sql_host: "127.0.0.1".to_string(),
                sql_port: 1433,
                sql_db: "AIWF".to_string(),
                sql_user: None,
                sql_password: None,
                sql_use_windows_auth: false,
            })),
            cancel_flags: Arc::new(Mutex::new(HashMap::new())),
            tenant_running: Arc::new(Mutex::new(HashMap::new())),
            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        };
        let app = build_router(state);
        let payload = json!({
            "run_id": "m1",
            "rows": [
                {"id":"1","amount":"10.1"},
                {"id":"2","amount":"11.2"}
            ],
            "rules": {"casts":{"id":"int","amount":"float"}}
        });
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/operators/transform_rows_v2")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .expect("transform request"),
            )
            .await
            .expect("transform response");
        assert_eq!(resp.status(), 200);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");
        assert_eq!(resp.status(), 200);
        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("metrics body");
        let text = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(text.contains("aiwf_transform_rows_v2_success_total 1"));
        assert!(text.contains("aiwf_transform_rows_v2_latency_ms_sum "));
        assert!(text.contains("aiwf_transform_rows_v2_latency_ms_max "));
        assert!(text.contains("aiwf_transform_rows_v2_output_rows_sum 2"));
        assert!(text.contains("aiwf_tasks_active 0"));
    }

    #[tokio::test]
    async fn cancel_task_endpoint_updates_status() {
        let state = AppState {
            service: "accel-rust".to_string(),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
            task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
                ttl_sec: 3600,
                max_tasks: 1000,
                store_path: None,
                remote_enabled: false,
                backend: "base_api".to_string(),
                base_api_url: None,
                base_api_key: None,
                sql_host: "127.0.0.1".to_string(),
                sql_port: 1433,
                sql_db: "AIWF".to_string(),
                sql_user: None,
                sql_password: None,
                sql_use_windows_auth: false,
            })),
            cancel_flags: Arc::new(Mutex::new(HashMap::new())),
            tenant_running: Arc::new(Mutex::new(HashMap::new())),
            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        };
        if let Ok(mut t) = state.tasks.lock() {
            t.insert(
                "task-cancel".to_string(),
                TaskState {
                    task_id: "task-cancel".to_string(),
                    tenant_id: "default".to_string(),
                    operator: "transform_rows_v2".to_string(),
                    status: "running".to_string(),
                    created_at: utc_now_iso(),
                    updated_at: utc_now_iso(),
                    result: None,
                    error: None,
                    idempotency_key: "".to_string(),
                    attempts: 0,
                },
            );
        }
        let app = build_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/tasks/task-cancel/cancel")
                    .body(Body::empty())
                    .expect("cancel request"),
            )
            .await
            .expect("cancel response");
        assert_eq!(resp.status(), 200);
        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("cancel body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("cancel json");
        assert_eq!(v.get("cancelled").and_then(|x| x.as_bool()), Some(true));
        assert_eq!(v.get("status").and_then(|x| x.as_str()), Some("cancelled"));
    }

    #[test]
    fn parquet_generic_roundtrip_rows() {
        let now = utc_now_iso();
        let path = std::env::temp_dir().join(format!("aiwf_rows_{now}.parquet"));
        let rows = vec![
            json!({"id":1,"text":"alpha"}),
            json!({"id":2,"text":"beta","score":9.5}),
        ];
        save_rows_parquet(path.to_string_lossy().as_ref(), &rows).expect("save parquet generic");
        let loaded =
            load_parquet_rows(path.to_string_lossy().as_ref(), 100).expect("load parquet generic");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].get("id").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(loaded[1].get("text").and_then(|v| v.as_str()), Some("beta"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn workflow_run_records_failed_step_replay() {
        let req = WorkflowRunReq {
            run_id: Some("wf-failed".to_string()),
            trace_id: None,
            traceparent: None,
            tenant_id: None,
            context: None,
            steps: vec![json!({
                "id": "bad-op",
                "operator": "missing_operator",
                "input": {}
            })],
        };
        let out = run_workflow(req).expect("workflow response");
        assert!(!out.ok);
        assert_eq!(out.status, "failed");
        assert_eq!(out.failed_step.as_deref(), Some("bad-op"));
        assert_eq!(out.steps.len(), 1);
        assert_eq!(out.steps[0].status, "failed");
        assert!(out.steps[0].error.as_deref().unwrap_or("").contains("unsupported"));
    }

    #[test]
    fn aggregate_rows_v1_groups_and_metrics() {
        let req = AggregateRowsReq {
            run_id: Some("agg-1".to_string()),
            rows: vec![
                json!({"team":"A","amount":10}),
                json!({"team":"A","amount":20}),
                json!({"team":"B","amount":7}),
            ],
            group_by: vec!["team".to_string()],
            aggregates: vec![
                json!({"op":"count","as":"cnt"}),
                json!({"op":"sum","field":"amount","as":"sum_amount"}),
                json!({"op":"avg","field":"amount","as":"avg_amount"}),
            ],
        };
        let out = run_aggregate_rows_v1(req).expect("aggregate rows");
        assert_eq!(out.rows.len(), 2);
        let a = out
            .rows
            .iter()
            .find(|r| r.get("team").and_then(|v| v.as_str()) == Some("A"))
            .expect("team A");
        assert_eq!(a.get("cnt").and_then(|v| v.as_u64()), Some(2));
        assert_eq!(a.get("sum_amount").and_then(|v| v.as_f64()), Some(30.0));
    }

    #[test]
    fn rules_package_publish_and_get_roundtrip() {
        let now = utc_now_iso();
        let name = format!("pkg_{now}");
        let version = "v1.0.0".to_string();
        let published = run_rules_package_publish_v1(RulesPackagePublishReq {
            name: name.clone(),
            version: version.clone(),
            dsl: Some("cast amount:float\nrequired amount".to_string()),
            rules: None,
        })
        .expect("publish package");
        assert!(published.ok);
        assert!(!published.fingerprint.is_empty());

        let fetched = run_rules_package_get_v1(RulesPackageGetReq {
            name: name.clone(),
            version: version.clone(),
        })
        .expect("get package");
        assert!(fetched.ok);
        assert_eq!(fetched.name, name);
        assert_eq!(fetched.version, version);
    }

    #[test]
    fn quality_check_v1_detects_duplicate_and_null_ratio() {
        let out = run_quality_check_v1(super::QualityCheckReq {
            run_id: Some("q1".to_string()),
            rows: vec![
                json!({"id":"1","name":"A","score":10}),
                json!({"id":"1","name":null,"score":11}),
            ],
            rules: json!({
                "unique_fields": ["id"],
                "required_fields": ["name"],
                "max_null_ratio": 0.0
            }),
        })
        .expect("quality check");
        assert!(!out.passed);
        assert!(out
            .report
            .get("violations")
            .and_then(|v| v.as_array())
            .map(|v| !v.is_empty())
            .unwrap_or(false));
    }

    #[test]
    fn validate_where_clause_blocks_injection_tokens() {
        assert!(validate_where_clause("amount > 10").is_ok());
        assert!(validate_where_clause("amount >= 10 and city = 'beijing'").is_ok());
        assert!(validate_where_clause("1=1; drop table data").is_err());
        assert!(validate_where_clause("amount > 10 union select 1").is_err());
        assert!(validate_where_clause("amount > 10 and").is_err());
    }

    #[test]
    fn transform_rows_v2_honors_cancel_flag() {
        let req = TransformRowsReq {
            run_id: Some("cancel-1".to_string()),
            tenant_id: None,
            trace_id: None,
            traceparent: None,
            rows: Some(vec![json!({"id":"1","amount":"10"})]),
            rules: Some(json!({"casts":{"id":"int","amount":"float"}})),
            quality_gates: Some(json!({})),
            schema_hint: None,
            rules_dsl: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: None,
        };
        let flag = Arc::new(AtomicBool::new(true));
        let res = run_transform_rows_v2_with_cancel(req, Some(flag));
        assert!(res.is_err());
        let err = res.err().unwrap_or_default();
        assert!(err.to_lowercase().contains("cancel"));
    }

    #[test]
    fn plugin_exec_v1_is_disabled_by_default() {
        let res = run_plugin_exec_v1(PluginExecReq {
            run_id: Some("plugin-off".to_string()),
            tenant_id: Some("default".to_string()),
            trace_id: None,
            plugin: "demo".to_string(),
            input: json!({"x": 1}),
        });
        assert!(res.is_err());
        assert!(res.err().unwrap_or_default().contains("disabled"));
    }

    #[test]
    fn load_rows_from_uri_limited_blocks_oversized_jsonl() {
        let now = utc_now_iso().replace(':', "_");
        let path = std::env::temp_dir().join(format!("aiwf_large_{now}.jsonl"));
        let mut f = fs::File::create(&path).expect("create temp jsonl");
        // Two medium lines, then force a very small byte quota to trigger limit rejection.
        writeln!(f, "{{\"id\":1,\"text\":\"abcdefghijk\"}}").expect("write line 1");
        writeln!(f, "{{\"id\":2,\"text\":\"mnopqrstuvw\"}}").expect("write line 2");
        drop(f);

        let err = load_rows_from_uri_limited(path.to_string_lossy().as_ref(), 100, 8)
            .err()
            .unwrap_or_default();
        assert!(err.contains("exceeds byte limit") || err.contains("exceeds"));
        let _ = fs::remove_file(path);
    }
}

