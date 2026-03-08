use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
};
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;
use axum::{
    Json,
    extract::{Path as AxPath, State},
    http::StatusCode,
    response::IntoResponse,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STD;
use chrono::{NaiveDate, NaiveDateTime};
use odbc_api::Environment;
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
use statrs::distribution::{ContinuousCDF, StudentsT};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env, fs,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use wasmtime::{
    Engine as WasmEngine, Linker as WasmLinker, Module as WasmModule, Store as WasmStore,
};

use accel_rust::{
    app_state::{
        AppState, ServiceMetrics, TaskState, TaskStoreConfig, TransformCacheEntry,
        TransformRowsResp, TransformRowsStats,
    },
    config::{ServerBind, allow_egress_enabled, is_local_endpoint},
    metrics::{
        acquire_file_lock, load_metrics_v2_samples, observe_operator_latency_v2,
        percentile_from_sorted, release_file_lock,
    },
    task_store::{
        escape_tsql, load_tasks_from_store, parse_sqlserver_conn_str, persist_tasks_to_store,
        probe_remote_task_store, prune_tasks, resolve_task_store_backend, run_sqlcmd_query,
        task_store_cancel_task, task_store_config_from_env, task_store_get_task,
        task_store_remote_enabled, task_store_upsert_task,
    },
};

mod http;
mod operators;
use http::routes::build_router;
pub(crate) use operators::transform::{
    TransformRowsReq, TransformRowsV3Req, collect_expr_lineage, observe_transform_success,
    run_transform_rows_v2, run_transform_rows_v2_with_cache, run_transform_rows_v2_with_cancel,
    run_transform_rows_v3,
};
pub(crate) use operators::analytics::{
    AggregateRowsReq, AggregateRowsV2Req, AggregateRowsV3Req, AggregateRowsV4Req,
    QualityCheckReq, QualityCheckV2Req, QualityCheckV3Req, QualityCheckV4Req,
    approx_percentile, compute_aggregate, parse_agg_specs, run_aggregate_rows_v1,
    run_aggregate_rows_v2, run_aggregate_rows_v3, run_aggregate_rows_v4, run_quality_check_v1,
    run_quality_check_v2, run_quality_check_v3, run_quality_check_v4,
};
pub(crate) use operators::join::{
    JoinRowsReq, JoinRowsV2Req, JoinRowsV3Req, JoinRowsV4Req, run_join_rows_v1,
    run_join_rows_v2, run_join_rows_v3, run_join_rows_v4,
};
pub(crate) use operators::workflow::{
    LineageV2Req, LineageV3Req, WorkflowRunReq, run_lineage_v2, run_lineage_v3, run_workflow,
};

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
    watermark_field: Option<String>,
    watermark_value: Option<Value>,
    max_chunks_per_run: Option<usize>,
}

#[derive(Serialize)]
struct TransformRowsStreamResp {
    ok: bool,
    operator: String,
    status: String,
    run_id: Option<String>,
    rows: Vec<Value>,
    chunks: usize,
    has_more: bool,
    next_checkpoint: Option<usize>,
    stats: Value,
}

#[derive(Deserialize)]
struct SchemaRegisterReq {
    name: String,
    version: String,
    schema: Value,
}

#[derive(Deserialize)]
struct SchemaGetReq {
    name: String,
    version: String,
}

#[derive(Deserialize)]
struct SchemaInferReq {
    name: Option<String>,
    version: Option<String>,
    rows: Vec<Value>,
}

#[derive(Deserialize)]
struct SchemaCompatReq {
    name: String,
    from_version: String,
    to_version: String,
    mode: Option<String>,
}

#[derive(Serialize)]
struct SchemaCompatResp {
    ok: bool,
    operator: String,
    status: String,
    compatible: bool,
    mode: String,
    breaking_fields: Vec<String>,
    widening_fields: Vec<String>,
}

#[derive(Deserialize)]
struct SchemaMigrationSuggestReq {
    name: String,
    from_version: String,
    to_version: String,
}

#[derive(Serialize)]
struct SchemaMigrationSuggestResp {
    ok: bool,
    operator: String,
    status: String,
    steps: Vec<Value>,
}

#[derive(Serialize)]
struct SchemaRegistryResp {
    ok: bool,
    operator: String,
    status: String,
    name: String,
    version: String,
    schema: Value,
}

#[derive(Serialize)]
struct SchemaInferResp {
    ok: bool,
    operator: String,
    status: String,
    name: Option<String>,
    version: Option<String>,
    schema: Value,
    stats: Value,
}

#[derive(Deserialize)]
struct LoadRowsV2Req {
    source_type: String,
    source: String,
    query: Option<String>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct LoadRowsV3Req {
    source_type: String,
    source: String,
    query: Option<String>,
    limit: Option<usize>,
    max_retries: Option<usize>,
    retry_backoff_ms: Option<u64>,
    resume_token: Option<String>,
    connector_options: Option<Value>,
}

#[derive(Deserialize)]
struct UdfWasmReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    field: String,
    output_field: String,
    op: Option<String>,
    wasm_base64: Option<String>,
}

#[derive(Deserialize)]
struct TimeSeriesReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    time_field: String,
    value_field: String,
    group_by: Option<Vec<String>>,
    window: Option<usize>,
}

#[derive(Deserialize)]
struct StatsReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    x_field: String,
    y_field: String,
}

#[derive(Deserialize)]
struct EntityLinkReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    field: String,
    id_field: Option<String>,
}

#[derive(Deserialize)]
struct TableReconstructReq {
    run_id: Option<String>,
    lines: Option<Vec<String>>,
    text: Option<String>,
    delimiter: Option<String>,
}

#[derive(Deserialize)]
struct FeatureStoreUpsertReq {
    run_id: Option<String>,
    key_field: String,
    rows: Vec<Value>,
}

#[derive(Deserialize)]
struct FeatureStoreGetReq {
    run_id: Option<String>,
    key: String,
}

#[derive(Deserialize)]
struct RuleSimulatorReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    rules: Value,
    candidate_rules: Value,
}

#[derive(Deserialize)]
struct ConstraintSolverReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    constraints: Vec<Value>,
}

#[derive(Deserialize)]
struct ChartDataPrepReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    category_field: String,
    value_field: String,
    series_field: Option<String>,
    top_n: Option<usize>,
}

#[derive(Deserialize)]
struct DiffAuditReq {
    run_id: Option<String>,
    left_rows: Vec<Value>,
    right_rows: Vec<Value>,
    keys: Vec<String>,
}

#[derive(Deserialize)]
struct VectorIndexBuildReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    id_field: String,
    text_field: String,
}

#[derive(Deserialize)]
struct VectorIndexSearchReq {
    run_id: Option<String>,
    query: String,
    top_k: Option<usize>,
}

#[derive(Deserialize)]
struct EvidenceRankReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    time_field: Option<String>,
    source_field: Option<String>,
    relevance_field: Option<String>,
    consistency_field: Option<String>,
}

#[derive(Deserialize)]
struct FactCrosscheckReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    claim_field: String,
    source_field: Option<String>,
}

#[derive(Deserialize)]
struct TimeSeriesForecastReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    time_field: String,
    value_field: String,
    horizon: Option<usize>,
    method: Option<String>,
}

#[derive(Deserialize)]
struct FinanceRatioReq {
    run_id: Option<String>,
    rows: Vec<Value>,
}

#[derive(Deserialize)]
struct AnomalyExplainReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    score_field: String,
    threshold: Option<f64>,
}

#[derive(Deserialize)]
struct TemplateBindReq {
    run_id: Option<String>,
    template_text: String,
    data: Value,
}

#[derive(Deserialize)]
struct ProvenanceSignReq {
    run_id: Option<String>,
    payload: Value,
    prev_hash: Option<String>,
}

#[derive(Deserialize)]
struct StreamStateSaveReq {
    run_id: Option<String>,
    stream_key: String,
    state: Value,
    offset: Option<u64>,
}

#[derive(Deserialize)]
struct StreamStateLoadReq {
    run_id: Option<String>,
    stream_key: String,
}

#[derive(Deserialize)]
struct QueryLangReq {
    run_id: Option<String>,
    rows: Vec<Value>,
    query: String,
}

#[derive(Deserialize)]
struct WindowRowsV1Req {
    run_id: Option<String>,
    rows: Vec<Value>,
    partition_by: Option<Vec<String>>,
    order_by: String,
    functions: Vec<Value>,
}

#[derive(Deserialize)]
struct OptimizerV1Req {
    run_id: Option<String>,
    rows: Option<Vec<Value>>,
    row_count_hint: Option<usize>,
    prefer_arrow: Option<bool>,
    join_hint: Option<Value>,
    aggregate_hint: Option<Value>,
}

#[derive(Deserialize)]
struct ParquetIoV2Req {
    run_id: Option<String>,
    op: String,
    path: String,
    rows: Option<Vec<Value>>,
    parquet_mode: Option<String>,
    limit: Option<usize>,
    columns: Option<Vec<String>>,
    predicate_field: Option<String>,
    predicate_eq: Option<Value>,
    partition_by: Option<Vec<String>>,
    compression: Option<String>,
    recursive: Option<bool>,
    schema_mode: Option<String>,
}

#[derive(Deserialize)]
struct StreamStateV2Req {
    run_id: Option<String>,
    op: String,
    stream_key: String,
    state: Option<Value>,
    offset: Option<u64>,
    checkpoint_version: Option<u64>,
    expected_version: Option<u64>,
    backend: Option<String>,
    db_path: Option<String>,
    event_ts_ms: Option<i64>,
    max_late_ms: Option<u64>,
}

#[derive(Deserialize)]
struct UdfWasmV2Req {
    run_id: Option<String>,
    rows: Vec<Value>,
    field: String,
    output_field: String,
    op: Option<String>,
    wasm_base64: Option<String>,
    max_output_bytes: Option<usize>,
    signed_token: Option<String>,
    allowed_ops: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct ExplainPlanV1Req {
    run_id: Option<String>,
    steps: Vec<Value>,
    rows: Option<Vec<Value>>,
    actual_stats: Option<Vec<Value>>,
    persist_feedback: Option<bool>,
}

#[derive(Deserialize)]
struct ExplainPlanV2Req {
    run_id: Option<String>,
    steps: Vec<Value>,
    rows: Option<Vec<Value>>,
    actual_stats: Option<Vec<Value>>,
    persist_feedback: Option<bool>,
    include_runtime_stats: Option<bool>,
}

#[derive(Deserialize)]
struct CapabilitiesV1Req {
    run_id: Option<String>,
    include_ops: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct IoContractV1Req {
    run_id: Option<String>,
    operator: String,
    input: Value,
    strict: Option<bool>,
}

#[derive(Deserialize)]
struct FailurePolicyV1Req {
    run_id: Option<String>,
    operator: Option<String>,
    error: String,
    status_code: Option<u16>,
    attempts: Option<u32>,
    max_retries: Option<u32>,
}

#[derive(Deserialize)]
struct IncrementalPlanV1Req {
    run_id: Option<String>,
    operator: String,
    input: Value,
    checkpoint_key: Option<String>,
}

#[derive(Deserialize)]
struct TenantIsolationV1Req {
    run_id: Option<String>,
    op: String,
    tenant_id: Option<String>,
    max_concurrency: Option<usize>,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    max_workflow_steps: Option<usize>,
}

#[derive(Deserialize)]
struct OperatorPolicyV1Req {
    run_id: Option<String>,
    op: String,
    tenant_id: Option<String>,
    allow: Option<Vec<String>>,
    deny: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct OptimizerAdaptiveV2Req {
    run_id: Option<String>,
    operator: Option<String>,
    row_count_hint: Option<usize>,
    prefer_arrow: Option<bool>,
}

#[derive(Deserialize)]
struct VectorIndexBuildV2Req {
    run_id: Option<String>,
    shard: Option<String>,
    rows: Vec<Value>,
    id_field: String,
    text_field: String,
    metadata_fields: Option<Vec<String>>,
    replace: Option<bool>,
}

#[derive(Deserialize)]
struct VectorIndexSearchV2Req {
    run_id: Option<String>,
    query: String,
    top_k: Option<usize>,
    shard: Option<String>,
    filter_eq: Option<Value>,
    rerank_meta_field: Option<String>,
    rerank_meta_weight: Option<f64>,
}

#[derive(Deserialize)]
struct VectorIndexEvalV2Req {
    run_id: Option<String>,
    shard: Option<String>,
    top_k: Option<usize>,
    cases: Vec<Value>,
}

#[derive(Deserialize)]
struct StreamReliabilityV1Req {
    run_id: Option<String>,
    op: String,
    stream_key: String,
    msg_id: Option<String>,
    row: Option<Value>,
    error: Option<String>,
    checkpoint: Option<u64>,
}

#[derive(Deserialize)]
struct LineageProvenanceV1Req {
    run_id: Option<String>,
    rules: Option<Value>,
    computed_fields_v3: Option<Vec<Value>>,
    workflow_steps: Option<Vec<Value>>,
    rows: Option<Vec<Value>>,
    payload: Option<Value>,
    prev_hash: Option<String>,
}

#[derive(Deserialize)]
struct ContractRegressionV1Req {
    run_id: Option<String>,
    operators: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct PerfBaselineV1Req {
    run_id: Option<String>,
    op: String,
    operator: Option<String>,
    p95_ms: Option<u128>,
    max_p95_ms: Option<u128>,
}

#[derive(Deserialize)]
struct PluginOperatorV1Req {
    run_id: Option<String>,
    tenant_id: Option<String>,
    plugin: String,
    op: Option<String>,
    payload: Option<Value>,
}

#[derive(Deserialize)]
struct StreamWindowV1Req {
    run_id: Option<String>,
    stream_key: String,
    rows: Vec<Value>,
    event_time_field: String,
    window_ms: u64,
    watermark_ms: Option<u64>,
    group_by: Option<Vec<String>>,
    value_field: Option<String>,
    trigger: Option<String>,
}

#[derive(Deserialize)]
struct StreamWindowV2Req {
    run_id: Option<String>,
    stream_key: String,
    rows: Vec<Value>,
    event_time_field: String,
    window_type: Option<String>,
    window_ms: u64,
    slide_ms: Option<u64>,
    session_gap_ms: Option<u64>,
    watermark_ms: Option<u64>,
    allowed_lateness_ms: Option<u64>,
    group_by: Option<Vec<String>>,
    value_field: Option<String>,
    trigger: Option<String>,
    emit_late_side: Option<bool>,
}

#[derive(Deserialize)]
struct ColumnarEvalV1Req {
    run_id: Option<String>,
    rows: Vec<Value>,
    select_fields: Option<Vec<String>>,
    filter_eq: Option<Value>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct SketchV1Req {
    run_id: Option<String>,
    op: String,
    kind: Option<String>,
    state: Option<Value>,
    rows: Option<Vec<Value>>,
    field: Option<String>,
    topk_n: Option<usize>,
    merge_state: Option<Value>,
}

#[derive(Deserialize)]
struct RuntimeStatsV1Req {
    run_id: Option<String>,
    op: String,
    operator: Option<String>,
    ok: Option<bool>,
    error_code: Option<String>,
    duration_ms: Option<u128>,
    rows_in: Option<usize>,
    rows_out: Option<usize>,
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

#[derive(Deserialize)]
struct PluginRegistryV1Req {
    run_id: Option<String>,
    op: String,
    plugin: Option<String>,
    manifest: Option<Value>,
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
    let bind = ServerBind::from_env();

    let task_cfg = resolve_task_store_backend(task_store_config_from_env());
    if task_cfg.remote_enabled && task_cfg.backend == "odbc" {
        let _ = unsafe {
            Environment::set_connection_pooling(odbc_api::sys::AttrConnectionPooling::DriverAware)
        };
    }
    let tasks_loaded = load_tasks_from_store(task_cfg.store_path.as_ref());
    let mut metrics0 = ServiceMetrics::default();
    metrics0.operator_latency_samples = load_metrics_v2_samples();
    let state = AppState {
        service: "accel-rust".to_string(),
        tasks: Arc::new(Mutex::new(tasks_loaded)),
        metrics: Arc::new(Mutex::new(metrics0)),
        task_cfg: Arc::new(Mutex::new(task_cfg)),
        cancel_flags: Arc::new(Mutex::new(HashMap::new())),
        tenant_running: Arc::new(Mutex::new(HashMap::new())),
        idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        transform_cache: Arc::new(Mutex::new(HashMap::new())),
        schema_registry: Arc::new(Mutex::new(HashMap::new())),
    };
    init_remote_task_store_probe(&state);

    let app = build_router(state);

    let addr = bind
        .socket_addr()
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
            warn!(
                "otel exporter endpoint blocked by AIWF_ALLOW_EGRESS=false: {}",
                ep
            );
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

async fn metrics_v2(State(state): State<AppState>) -> impl IntoResponse {
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

async fn metrics_v2_prom(State(state): State<AppState>) -> impl IntoResponse {
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
    let columnar_preferred = request_prefers_columnar(&req);
    if let Some(rows) = req.rows.as_ref() {
        let bytes = serde_json::to_vec(rows).map(|v| v.len()).unwrap_or(0);
        if let Err(e) =
            enforce_tenant_payload_quota(Some(&state), req.tenant_id.as_deref(), rows.len(), bytes)
        {
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
        if columnar_preferred {
            m.transform_rows_v2_columnar_calls += 1;
        }
    }
    match run_transform_rows_v2_with_cache(
        req,
        None,
        Some(&state.transform_cache),
        Some(&state.metrics),
    ) {
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

async fn transform_rows_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<TransformRowsV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.transform_rows_v3_calls += 1;
    }
    match run_transform_rows_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "transform_rows_v3",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "transform_rows_v3".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn transform_rows_v2_cache_stats_operator(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let now = unix_now_sec();
    let mut entries = 0usize;
    let mut expired = 0usize;
    if let Ok(guard) = state.transform_cache.lock() {
        entries = guard.len();
        expired = guard.values().filter(|v| v.expires_at_epoch <= now).count();
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "operator": "transform_rows_v2_cache_stats",
            "cache_enabled": transform_cache_enabled(),
            "entries": entries,
            "expired_entries": expired,
            "ttl_sec": transform_cache_ttl_sec(),
            "max_entries": transform_cache_max_entries()
        })),
    )
        .into_response()
}

async fn transform_rows_v2_cache_clear_operator(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut cleared = 0usize;
    if let Ok(mut guard) = state.transform_cache.lock() {
        cleared = guard.len();
        guard.clear();
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "operator": "transform_rows_v2_cache_clear",
            "cleared": cleared
        })),
    )
        .into_response()
}

async fn transform_rows_v2_submit_operator(
    State(state): State<AppState>,
    Json(req): Json<TransformRowsReq>,
) -> impl IntoResponse {
    let columnar_preferred = request_prefers_columnar(&req);
    if let Some(rows) = req.rows.as_ref() {
        let bytes = serde_json::to_vec(rows).map(|v| v.len()).unwrap_or(0);
        if let Err(e) =
            enforce_tenant_payload_quota(Some(&state), req.tenant_id.as_deref(), rows.len(), bytes)
        {
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
        if let Ok(t) = state.tasks.lock()
            && let Some(ts) = t.get(existing_task_id)
        {
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
        if columnar_preferred {
            m.transform_rows_v2_columnar_calls += 1;
        }
    }

    let task_id_for_worker = task_id.clone();
    let tasks = Arc::clone(&state.tasks);
    let metrics = Arc::clone(&state.metrics);
    let transform_cache = Arc::clone(&state.transform_cache);
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
                let metrics_for_call = Arc::clone(&metrics);
                let cache_for_call = Arc::clone(&transform_cache);
                move || {
                    run_transform_rows_v2_with_cache(
                        req_call,
                        Some(cancel_flag),
                        Some(&cache_for_call),
                        Some(&metrics_for_call),
                    )
                }
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

    (
        StatusCode::OK,
        Json(json!({"ok": true, "task_id": task_id, "status": "queued"})),
    )
        .into_response()
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

async fn join_rows_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<JoinRowsV2Req>,
) -> impl IntoResponse {
    if let Ok(mut m) = state.metrics.lock() {
        m.join_rows_v2_calls += 1;
    }
    match run_join_rows_v2(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "join_rows_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn join_rows_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<JoinRowsV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.join_rows_v3_calls += 1;
    }
    match run_join_rows_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "join_rows_v3",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "join_rows_v3".to_string(),
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

async fn aggregate_rows_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<AggregateRowsV2Req>,
) -> impl IntoResponse {
    if let Ok(mut m) = state.metrics.lock() {
        m.aggregate_rows_v2_calls += 1;
    }
    match run_aggregate_rows_v2(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "aggregate_rows_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn aggregate_rows_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<AggregateRowsV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.aggregate_rows_v3_calls += 1;
    }
    match run_aggregate_rows_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "aggregate_rows_v3",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "aggregate_rows_v3".to_string(),
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

async fn quality_check_v2_operator(
    State(state): State<AppState>,
    Json(req): Json<QualityCheckV2Req>,
) -> impl IntoResponse {
    if let Ok(mut m) = state.metrics.lock() {
        m.quality_check_v2_calls += 1;
    }
    match run_quality_check_v2(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "quality_check_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn quality_check_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<QualityCheckV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.quality_check_v3_calls += 1;
    }
    match run_quality_check_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "quality_check_v3",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "quality_check_v3".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn aggregate_pushdown_v1_operator(
    Json(req): Json<AggregatePushdownReq>,
) -> impl IntoResponse {
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
    if let Err(e) = enforce_tenant_payload_quota(Some(&state), req.tenant_id.as_deref(), 1, bytes) {
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

async fn plugin_registry_v1_operator(Json(req): Json<PluginRegistryV1Req>) -> impl IntoResponse {
    match run_plugin_registry_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "plugin_registry_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn plugin_operator_v1_operator(Json(req): Json<PluginOperatorV1Req>) -> impl IntoResponse {
    match run_plugin_operator_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "plugin_operator_v1".to_string(),
                status: "failed".to_string(),
                error: e,
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

async fn load_rows_v2_operator(Json(req): Json<LoadRowsV2Req>) -> impl IntoResponse {
    match run_load_rows_v2(req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "load_rows_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn load_rows_v3_operator(
    State(state): State<AppState>,
    Json(req): Json<LoadRowsV3Req>,
) -> impl IntoResponse {
    let begin = Instant::now();
    if let Ok(mut m) = state.metrics.lock() {
        m.load_rows_v3_calls += 1;
    }
    match run_load_rows_v3(req) {
        Ok(resp) => {
            observe_operator_latency_v2(
                &state.metrics,
                "load_rows_v3",
                begin.elapsed().as_millis(),
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "load_rows_v3".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn schema_registry_register_v1_operator(
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

async fn schema_registry_get_v1_operator(
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

async fn schema_registry_infer_v1_operator(
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

async fn schema_registry_register_v2_operator(
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

async fn schema_registry_get_v2_operator(
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

async fn schema_registry_infer_v2_operator(
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

async fn schema_registry_check_compat_v2_operator(
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

async fn schema_registry_suggest_migration_v2_operator(
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

async fn udf_wasm_v1_operator(
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

async fn time_series_v1_operator(Json(req): Json<TimeSeriesReq>) -> impl IntoResponse {
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

async fn stats_v1_operator(Json(req): Json<StatsReq>) -> impl IntoResponse {
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

async fn entity_linking_v1_operator(Json(req): Json<EntityLinkReq>) -> impl IntoResponse {
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

async fn table_reconstruct_v1_operator(Json(req): Json<TableReconstructReq>) -> impl IntoResponse {
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

async fn feature_store_upsert_v1_operator(
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

async fn feature_store_get_v1_operator(Json(req): Json<FeatureStoreGetReq>) -> impl IntoResponse {
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

async fn lineage_v2_operator(Json(req): Json<LineageV2Req>) -> impl IntoResponse {
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

async fn rule_simulator_v1_operator(Json(req): Json<RuleSimulatorReq>) -> impl IntoResponse {
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

async fn constraint_solver_v1_operator(Json(req): Json<ConstraintSolverReq>) -> impl IntoResponse {
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

async fn chart_data_prep_v1_operator(Json(req): Json<ChartDataPrepReq>) -> impl IntoResponse {
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

async fn diff_audit_v1_operator(Json(req): Json<DiffAuditReq>) -> impl IntoResponse {
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

async fn vector_index_build_v1_operator(Json(req): Json<VectorIndexBuildReq>) -> impl IntoResponse {
    match run_vector_index_build_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v1_build".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn vector_index_search_v1_operator(
    Json(req): Json<VectorIndexSearchReq>,
) -> impl IntoResponse {
    match run_vector_index_search_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v1_search".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn evidence_rank_v1_operator(Json(req): Json<EvidenceRankReq>) -> impl IntoResponse {
    match run_evidence_rank_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "evidence_rank_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn fact_crosscheck_v1_operator(Json(req): Json<FactCrosscheckReq>) -> impl IntoResponse {
    match run_fact_crosscheck_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "fact_crosscheck_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn timeseries_forecast_v1_operator(
    Json(req): Json<TimeSeriesForecastReq>,
) -> impl IntoResponse {
    match run_timeseries_forecast_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "timeseries_forecast_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn finance_ratio_v1_operator(Json(req): Json<FinanceRatioReq>) -> impl IntoResponse {
    match run_finance_ratio_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "finance_ratio_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn anomaly_explain_v1_operator(Json(req): Json<AnomalyExplainReq>) -> impl IntoResponse {
    match run_anomaly_explain_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "anomaly_explain_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn template_bind_v1_operator(Json(req): Json<TemplateBindReq>) -> impl IntoResponse {
    match run_template_bind_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "template_bind_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn provenance_sign_v1_operator(Json(req): Json<ProvenanceSignReq>) -> impl IntoResponse {
    match run_provenance_sign_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "provenance_sign_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn stream_state_save_v1_operator(Json(req): Json<StreamStateSaveReq>) -> impl IntoResponse {
    match run_stream_state_save_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_state_v1_save".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn stream_state_load_v1_operator(Json(req): Json<StreamStateLoadReq>) -> impl IntoResponse {
    match run_stream_state_load_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_state_v1_load".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn query_lang_v1_operator(Json(req): Json<QueryLangReq>) -> impl IntoResponse {
    match run_query_lang_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "query_lang_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn columnar_eval_v1_operator(Json(req): Json<ColumnarEvalV1Req>) -> impl IntoResponse {
    match run_columnar_eval_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "columnar_eval_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn stream_window_v1_operator(Json(req): Json<StreamWindowV1Req>) -> impl IntoResponse {
    match run_stream_window_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_window_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn stream_window_v2_operator(Json(req): Json<StreamWindowV2Req>) -> impl IntoResponse {
    match run_stream_window_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_window_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn sketch_v1_operator(Json(req): Json<SketchV1Req>) -> impl IntoResponse {
    match run_sketch_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "sketch_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn runtime_stats_v1_operator(Json(req): Json<RuntimeStatsV1Req>) -> impl IntoResponse {
    match run_runtime_stats_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "runtime_stats_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn capabilities_v1_operator(Json(req): Json<CapabilitiesV1Req>) -> impl IntoResponse {
    match run_capabilities_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "capabilities_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn io_contract_v1_operator(Json(req): Json<IoContractV1Req>) -> impl IntoResponse {
    match run_io_contract_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "io_contract_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn failure_policy_v1_operator(Json(req): Json<FailurePolicyV1Req>) -> impl IntoResponse {
    match run_failure_policy_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "failure_policy_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn incremental_plan_v1_operator(
    State(state): State<AppState>,
    Json(req): Json<IncrementalPlanV1Req>,
) -> impl IntoResponse {
    match run_incremental_plan_v1(&state, req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "incremental_plan_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn tenant_isolation_v1_operator(Json(req): Json<TenantIsolationV1Req>) -> impl IntoResponse {
    match run_tenant_isolation_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "tenant_isolation_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn operator_policy_v1_operator(Json(req): Json<OperatorPolicyV1Req>) -> impl IntoResponse {
    match run_operator_policy_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "operator_policy_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn optimizer_adaptive_v2_operator(
    Json(req): Json<OptimizerAdaptiveV2Req>,
) -> impl IntoResponse {
    match run_optimizer_adaptive_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "optimizer_adaptive_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn vector_index_build_v2_operator(
    Json(req): Json<VectorIndexBuildV2Req>,
) -> impl IntoResponse {
    match run_vector_index_build_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v2_build".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn vector_index_search_v2_operator(
    Json(req): Json<VectorIndexSearchV2Req>,
) -> impl IntoResponse {
    match run_vector_index_search_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v2_search".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn vector_index_eval_v2_operator(
    Json(req): Json<VectorIndexEvalV2Req>,
) -> impl IntoResponse {
    match run_vector_index_eval_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "vector_index_v2_eval".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn stream_reliability_v1_operator(
    Json(req): Json<StreamReliabilityV1Req>,
) -> impl IntoResponse {
    match run_stream_reliability_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_reliability_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn lineage_provenance_v1_operator(
    Json(req): Json<LineageProvenanceV1Req>,
) -> impl IntoResponse {
    match run_lineage_provenance_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "lineage_provenance_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn contract_regression_v1_operator(
    Json(req): Json<ContractRegressionV1Req>,
) -> impl IntoResponse {
    match run_contract_regression_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "contract_regression_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn perf_baseline_v1_operator(Json(req): Json<PerfBaselineV1Req>) -> impl IntoResponse {
    match run_perf_baseline_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "perf_baseline_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn window_rows_v1_operator(Json(req): Json<WindowRowsV1Req>) -> impl IntoResponse {
    match run_window_rows_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "window_rows_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn optimizer_v1_operator(Json(req): Json<OptimizerV1Req>) -> impl IntoResponse {
    match run_optimizer_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "optimizer_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn join_rows_v4_operator(Json(req): Json<JoinRowsV4Req>) -> impl IntoResponse {
    match run_join_rows_v4(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "join_rows_v4".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn aggregate_rows_v4_operator(Json(req): Json<AggregateRowsV4Req>) -> impl IntoResponse {
    match run_aggregate_rows_v4(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "aggregate_rows_v4".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn quality_check_v4_operator(Json(req): Json<QualityCheckV4Req>) -> impl IntoResponse {
    match run_quality_check_v4(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "quality_check_v4".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn lineage_v3_operator(Json(req): Json<LineageV3Req>) -> impl IntoResponse {
    match run_lineage_v3(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "lineage_v3".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn parquet_io_v2_operator(Json(req): Json<ParquetIoV2Req>) -> impl IntoResponse {
    match run_parquet_io_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "parquet_io_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn stream_state_v2_operator(Json(req): Json<StreamStateV2Req>) -> impl IntoResponse {
    match run_stream_state_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "stream_state_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn udf_wasm_v2_operator(Json(req): Json<UdfWasmV2Req>) -> impl IntoResponse {
    match run_udf_wasm_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "udf_wasm_v2".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn explain_plan_v1_operator(Json(req): Json<ExplainPlanV1Req>) -> impl IntoResponse {
    match run_explain_plan_v1(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "explain_plan_v1".to_string(),
                status: "failed".to_string(),
                error: e,
            }),
        )
            .into_response(),
    }
}

async fn explain_plan_v2_operator(Json(req): Json<ExplainPlanV2Req>) -> impl IntoResponse {
    match run_explain_plan_v2(req) {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrResp {
                ok: false,
                operator: "explain_plan_v2".to_string(),
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
        if let Err(e) =
            enforce_tenant_payload_quota(Some(&state), req.tenant_id.as_deref(), rows.len(), bytes)
        {
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
    let step_limit = tenant_max_workflow_steps_for(req.tenant_id.as_deref());
    if req.steps.len() > step_limit {
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
                    step_limit
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
            if t == "references" || t == "bibliography" || t == "参考文献" || t == "引用文献"
            {
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
            if t.to_lowercase().starts_with("footnote")
                || t.starts_with("注释")
                || t.starts_with("脚注")
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
    let payload =
        json!({"checkpoint_key": key, "last_chunk": chunk_idx, "updated_at": utc_now_iso()});
    fs::write(
        &path,
        serde_json::to_string_pretty(&payload).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("write checkpoint: {e}"))
}

fn read_stream_checkpoint(key: &str) -> Result<Option<usize>, String> {
    let path = checkpoint_path(key)?;
    if !path.exists() {
        return Ok(None);
    }
    let txt = fs::read_to_string(&path).map_err(|e| format!("read checkpoint: {e}"))?;
    let v: Value = serde_json::from_str(&txt).map_err(|e| format!("parse checkpoint: {e}"))?;
    Ok(v.get("last_chunk")
        .and_then(|x| x.as_u64())
        .map(|x| x as usize))
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
    let email_re =
        Regex::new(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}").map_err(|e| e.to_string())?;
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

fn run_aggregate_pushdown_v1(req: AggregatePushdownReq) -> Result<AggregatePushdownResp, String> {
    if req.group_by.is_empty() {
        return Err("group_by is empty".to_string());
    }
    let from = req.from.as_deref().unwrap_or("data").trim().to_string();
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
            "count" => Ok(format!(
                "COUNT(1) AS {}",
                validate_sql_identifier(&s.as_name)?
            )),
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
        .as_deref()
        .map(|w| validate_where_clause(w))
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

fn plugin_registry_store_path() -> PathBuf {
    env::var("AIWF_PLUGIN_REGISTRY_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("plugin_registry.json"))
}

fn plugin_runtime_store_path() -> PathBuf {
    env::var("AIWF_PLUGIN_RUNTIME_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("plugin_runtime.json"))
}

fn plugin_tenant_running_map() -> &'static Mutex<HashMap<String, usize>> {
    static RUN: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    RUN.get_or_init(|| Mutex::new(HashMap::new()))
}

fn plugin_audit_log_path() -> PathBuf {
    env::var("AIWF_PLUGIN_AUDIT_LOG")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("plugin_audit.log"))
}

fn append_plugin_audit(record: &Value) -> Result<(), String> {
    let p = plugin_audit_log_path();
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create plugin audit dir: {e}"))?;
    }
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&p)
        .map_err(|e| format!("open plugin audit log: {e}"))?;
    let line = serde_json::to_string(record).map_err(|e| format!("encode plugin audit: {e}"))?;
    writeln!(f, "{line}").map_err(|e| format!("write plugin audit: {e}"))?;
    Ok(())
}

fn load_plugin_registry_store() -> HashMap<String, Value> {
    load_kv_store(&plugin_registry_store_path())
}

fn save_plugin_registry_store(store: &HashMap<String, Value>) -> Result<(), String> {
    save_kv_store(&plugin_registry_store_path(), store)
}

fn run_plugin_registry_v1(req: PluginRegistryV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let mut store = load_plugin_registry_store();
    match op.as_str() {
        "list" => {
            let mut items = store
                .iter()
                .map(|(k, v)| json!({"plugin": k, "manifest": v}))
                .collect::<Vec<_>>();
            items.sort_by(|a, b| {
                let ak = a
                    .get("plugin")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let bk = b
                    .get("plugin")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                ak.cmp(&bk)
            });
            Ok(
                json!({"ok": true, "operator":"plugin_registry_v1", "status":"done", "run_id": req.run_id, "op": op, "items": items}),
            )
        }
        "get" => {
            let plugin = safe_pkg_token(req.plugin.as_deref().unwrap_or(""))?;
            let manifest = store.get(&plugin).cloned().unwrap_or(Value::Null);
            Ok(
                json!({"ok": true, "operator":"plugin_registry_v1", "status":"done", "run_id": req.run_id, "op": op, "plugin": plugin, "manifest": manifest}),
            )
        }
        "register" | "upsert" => {
            let plugin = safe_pkg_token(req.plugin.as_deref().unwrap_or(""))?;
            let manifest = req.manifest.unwrap_or(Value::Null);
            let pm: PluginManifest = serde_json::from_value(manifest.clone())
                .map_err(|e| format!("plugin manifest invalid: {e}"))?;
            if pm.command.trim().is_empty() {
                return Err("plugin manifest missing command".to_string());
            }
            store.insert(plugin.clone(), manifest);
            save_plugin_registry_store(&store)?;
            Ok(
                json!({"ok": true, "operator":"plugin_registry_v1", "status":"done", "run_id": req.run_id, "op": op, "plugin": plugin, "size": store.len()}),
            )
        }
        "delete" | "unregister" => {
            let plugin = safe_pkg_token(req.plugin.as_deref().unwrap_or(""))?;
            let deleted = store.remove(&plugin).is_some();
            save_plugin_registry_store(&store)?;
            Ok(
                json!({"ok": true, "operator":"plugin_registry_v1", "status":"done", "run_id": req.run_id, "op": op, "plugin": plugin, "deleted": deleted, "size": store.len()}),
            )
        }
        _ => Err(format!("plugin_registry_v1 unsupported op: {}", req.op)),
    }
}

fn run_plugin_operator_v1(req: PluginOperatorV1Req) -> Result<Value, String> {
    let plugin = safe_pkg_token(&req.plugin)?;
    let op = req.op.unwrap_or_else(|| "run".to_string());
    let tenant = req
        .tenant_id
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let payload = req.payload.unwrap_or(Value::Null);
    let max_bytes = env::var("AIWF_PLUGIN_OPERATOR_MAX_INPUT_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000);
    let bytes = serde_json::to_vec(&payload).map(|v| v.len()).unwrap_or(0);
    if bytes > max_bytes {
        return Err(format!(
            "plugin_operator_v1 input exceeds limit: {} > {}",
            bytes, max_bytes
        ));
    }
    let max_out = env::var("AIWF_PLUGIN_OPERATOR_MAX_OUTPUT_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(2_000_000);
    let max_concurrent = env::var("AIWF_PLUGIN_OPERATOR_MAX_CONCURRENT_PER_TENANT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(2)
        .max(1);
    let cb_threshold = env::var("AIWF_PLUGIN_OPERATOR_CB_FAIL_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(3)
        .max(1);
    let cb_open_ms = env::var("AIWF_PLUGIN_OPERATOR_CB_OPEN_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(30_000)
        .max(1000);

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut rt = load_kv_store(&plugin_runtime_store_path());
    let key = format!("{}::{}", tenant, plugin);
    let open_until = rt
        .get(&key)
        .and_then(|v| v.get("open_until"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    if open_until > now_ms {
        return Err(format!(
            "plugin_operator_v1 circuit open for {} ms",
            open_until.saturating_sub(now_ms)
        ));
    }

    {
        let mut m = plugin_tenant_running_map()
            .lock()
            .map_err(|_| "plugin_operator_v1 tenant runtime lock poisoned".to_string())?;
        let cur = m.get(&tenant).copied().unwrap_or(0);
        if cur >= max_concurrent {
            return Err(format!(
                "plugin_operator_v1 tenant concurrent limit exceeded: {} >= {}",
                cur, max_concurrent
            ));
        }
        m.insert(tenant.clone(), cur + 1);
    }

    let started = Instant::now();
    let result = run_plugin_exec_v1(PluginExecReq {
        run_id: req.run_id.clone(),
        tenant_id: Some(tenant.clone()),
        trace_id: None,
        plugin: plugin.clone(),
        input: json!({
            "op": op,
            "payload": payload
        }),
    });
    {
        if let Ok(mut m) = plugin_tenant_running_map().lock() {
            let cur = m.get(&tenant).copied().unwrap_or(1);
            if cur <= 1 {
                m.remove(&tenant);
            } else {
                m.insert(tenant.clone(), cur - 1);
            }
        }
    }

    match result {
        Ok(exec) => {
            let out_bytes = serde_json::to_vec(&exec.output)
                .map(|v| v.len())
                .unwrap_or(0);
            if out_bytes > max_out {
                return Err(format!(
                    "plugin_operator_v1 output exceeds limit: {} > {}",
                    out_bytes, max_out
                ));
            }
            rt.insert(
                key.clone(),
                json!({
                    "fail_count": 0u64,
                    "open_until": 0u64,
                    "updated_at": utc_now_iso()
                }),
            );
            let _ = save_kv_store(&plugin_runtime_store_path(), &rt);
            let _ = append_plugin_audit(&json!({
                "ts": utc_now_iso(),
                "run_id": req.run_id,
                "tenant_id": tenant,
                "plugin": plugin,
                "op": op,
                "status": "done",
                "duration_ms": started.elapsed().as_millis(),
                "stderr_len": exec.stderr.len(),
                "output_bytes": out_bytes
            }));
            Ok(json!({
                "ok": true,
                "operator": "plugin_operator_v1",
                "status": "done",
                "run_id": req.run_id,
                "plugin": plugin,
                "output": exec.output,
                "stderr": exec.stderr
            }))
        }
        Err(e) => {
            let fail = rt
                .get(&key)
                .and_then(|v| v.get("fail_count"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                + 1;
            let next_open = if fail >= cb_threshold {
                now_ms + cb_open_ms
            } else {
                0
            };
            rt.insert(
                key.clone(),
                json!({
                    "fail_count": fail,
                    "open_until": next_open,
                    "updated_at": utc_now_iso()
                }),
            );
            let _ = save_kv_store(&plugin_runtime_store_path(), &rt);
            let _ = append_plugin_audit(&json!({
                "ts": utc_now_iso(),
                "run_id": req.run_id,
                "tenant_id": tenant,
                "plugin": plugin,
                "op": op,
                "status": "failed",
                "duration_ms": started.elapsed().as_millis(),
                "error": e
            }));
            Err("plugin_operator_v1 execution failed".to_string())
        }
    }
}

fn load_plugin_manifest(plugin: &str) -> Result<PluginManifest, String> {
    let m: PluginManifest = {
        let reg = load_plugin_registry_store();
        if let Some(v) = reg.get(plugin) {
            serde_json::from_value(v.clone())
                .map_err(|e| format!("parse plugin registry config: {e}"))?
        } else {
            let cfg_path = plugin_dir().join(format!("{plugin}.json"));
            let cfg_txt =
                fs::read_to_string(&cfg_path).map_err(|e| format!("read plugin config: {e}"))?;
            serde_json::from_str(&cfg_txt).map_err(|e| format!("parse plugin config: {e}"))?
        }
    };
    if m.command.trim().is_empty() {
        return Err("plugin config missing command".to_string());
    }
    if let Some(n) = &m.name
        && !n.trim().is_empty()
    {
        let nn = safe_pkg_token(n)?;
        if !nn.eq_ignore_ascii_case(plugin) {
            return Err(format!(
                "plugin name mismatch: manifest={nn}, request={plugin}"
            ));
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
    if let Some(ver) = &m.version
        && ver.trim().is_empty()
    {
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
    let allow = env::var("AIWF_PLUGIN_ALLOWLIST").ok().unwrap_or_default();
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
    h.update(format!("{secret}:{plugin}:{cmd}:{}", args.join("\u{1f}")).as_bytes());
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
                return Err(format!(
                    "plugin {label} exceeds limit: {} > {}",
                    out.len() + n,
                    cap
                ));
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
    let timeout_ms = manifest.timeout_ms.unwrap_or(20_000).min(120_000);
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
        "sqlite" => load_sqlite_rows(
            &req.source,
            req.query.as_deref().unwrap_or("SELECT * FROM data"),
            limit,
        )?,
        "sqlserver" => load_sqlserver_rows(
            &req.source,
            req.query
                .as_deref()
                .unwrap_or("SELECT TOP 100 * FROM dbo.workflow_tasks"),
            limit,
        )?,
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

fn run_load_rows_v2(req: LoadRowsV2Req) -> Result<LoadRowsResp, String> {
    let st = req.source_type.to_lowercase();
    let limit = req.limit.unwrap_or(10000).max(1);
    let rows = match st.as_str() {
        "jsonl" => load_jsonl_rows(&req.source, limit)?,
        "csv" => load_csv_rows(&req.source, limit)?,
        "sqlite" => load_sqlite_rows(
            &req.source,
            req.query.as_deref().unwrap_or("SELECT * FROM data"),
            limit,
        )?,
        "sqlserver" => load_sqlserver_rows(
            &req.source,
            req.query
                .as_deref()
                .unwrap_or("SELECT TOP 100 * FROM dbo.workflow_tasks"),
            limit,
        )?,
        "parquet" => load_parquet_rows(&req.source, limit)?,
        "txt" => {
            let txt = fs::read_to_string(&req.source).map_err(|e| format!("read txt: {e}"))?;
            txt.lines()
                .take(limit)
                .enumerate()
                .map(|(i, line)| json!({"line_no": i + 1, "text": line}))
                .collect::<Vec<_>>()
        }
        "pdf" | "docx" | "xlsx" | "image" => {
            let meta =
                fs::metadata(&req.source).map_err(|e| format!("read source metadata: {e}"))?;
            vec![json!({
                "source": req.source,
                "source_type": st,
                "size_bytes": meta.len(),
                "extract_status": "metadata_only",
                "hint": "use glue-python ingest for rich extraction"
            })]
        }
        _ => return Err(format!("unsupported source_type: {}", req.source_type)),
    };
    Ok(LoadRowsResp {
        ok: true,
        operator: "load_rows_v2".to_string(),
        status: "done".to_string(),
        stats: json!({"source_type": st, "rows": rows.len()}),
        rows,
    })
}

fn run_load_rows_v3(req: LoadRowsV3Req) -> Result<LoadRowsResp, String> {
    let max_retries = req.max_retries.unwrap_or(2).min(8);
    let backoff_ms = req.retry_backoff_ms.unwrap_or(150).clamp(10, 10_000);
    let mut last_err = None::<String>;
    for attempt in 0..=max_retries {
        let out = run_load_rows_v2(LoadRowsV2Req {
            source_type: req.source_type.clone(),
            source: req.source.clone(),
            query: req.query.clone(),
            limit: req.limit,
        });
        match out {
            Ok(mut resp) => {
                resp.operator = "load_rows_v3".to_string();
                let mut stats = resp.stats.as_object().cloned().unwrap_or_default();
                stats.insert("attempt".to_string(), json!(attempt + 1));
                stats.insert("max_retries".to_string(), json!(max_retries));
                stats.insert("resume_token".to_string(), json!(req.resume_token));
                stats.insert(
                    "connector_options".to_string(),
                    req.connector_options.clone().unwrap_or_else(|| json!({})),
                );
                resp.stats = Value::Object(stats);
                return Ok(resp);
            }
            Err(e) => {
                last_err = Some(e);
                if attempt < max_retries {
                    std::thread::sleep(std::time::Duration::from_millis(
                        backoff_ms * (attempt as u64 + 1),
                    ));
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| "load_rows_v3 failed".to_string()))
}

fn schema_registry_key(name: &str, version: &str) -> Result<String, String> {
    let n = safe_pkg_token(name)?;
    let v = safe_pkg_token(version)?;
    Ok(format!("{n}@{v}"))
}

fn infer_schema_from_rows(rows: &[Value]) -> Value {
    let mut fields: HashMap<String, HashSet<String>> = HashMap::new();
    for r in rows {
        let Some(obj) = r.as_object() else { continue };
        for (k, v) in obj {
            let t = match v {
                Value::Null => "null",
                Value::Bool(_) => "bool",
                Value::Number(n) => {
                    if n.is_i64() || n.is_u64() {
                        "int"
                    } else {
                        "float"
                    }
                }
                Value::String(s) => {
                    let ts = s.trim().to_ascii_lowercase();
                    if ts == "true" || ts == "false" {
                        "bool_like"
                    } else if s.parse::<i64>().is_ok() {
                        "int_like"
                    } else if s.parse::<f64>().is_ok() {
                        "float_like"
                    } else {
                        "string"
                    }
                }
                Value::Array(_) => "array",
                Value::Object(_) => "object",
            };
            fields.entry(k.clone()).or_default().insert(t.to_string());
        }
    }
    let mut out = Map::new();
    for (k, tset) in fields {
        let t = if tset.contains("string") {
            "string"
        } else if tset.contains("float") || tset.contains("float_like") {
            "float"
        } else if tset.contains("int") || tset.contains("int_like") {
            "int"
        } else if tset.contains("bool") || tset.contains("bool_like") {
            "bool"
        } else if tset.contains("object") {
            "object"
        } else if tset.contains("array") {
            "array"
        } else {
            "unknown"
        };
        out.insert(k, Value::String(t.to_string()));
    }
    Value::Object(out)
}

fn schema_registry_store_path() -> PathBuf {
    env::var("AIWF_SCHEMA_REGISTRY_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("schema_registry.json"))
}

fn load_schema_registry_store() -> HashMap<String, Value> {
    let p = schema_registry_store_path();
    if let Ok(lock) = acquire_file_lock(&p) {
        let out = (|| {
            let Ok(txt) = fs::read_to_string(&p) else {
                return HashMap::new();
            };
            serde_json::from_str::<HashMap<String, Value>>(&txt).unwrap_or_default()
        })();
        release_file_lock(&lock);
        return out;
    }
    let Ok(txt) = fs::read_to_string(&p) else {
        return HashMap::new();
    };
    serde_json::from_str::<HashMap<String, Value>>(&txt).unwrap_or_default()
}

fn save_schema_registry_store(store: &HashMap<String, Value>) -> Result<(), String> {
    let p = schema_registry_store_path();
    let lock = acquire_file_lock(&p)?;
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create schema store dir: {e}"))?;
    }
    let s =
        serde_json::to_string_pretty(store).map_err(|e| format!("serialize schema store: {e}"))?;
    let out = fs::write(&p, s).map_err(|e| format!("write schema store: {e}"));
    release_file_lock(&lock);
    out
}

fn run_schema_registry_register_local(
    req: &SchemaRegisterReq,
) -> Result<SchemaRegistryResp, String> {
    let key = schema_registry_key(&req.name, &req.version)?;
    let mut store = load_schema_registry_store();
    store.insert(key, req.schema.clone());
    save_schema_registry_store(&store)?;
    Ok(SchemaRegistryResp {
        ok: true,
        operator: "schema_registry_v1_register".to_string(),
        status: "done".to_string(),
        name: req.name.clone(),
        version: req.version.clone(),
        schema: req.schema.clone(),
    })
}

fn run_schema_registry_get_local(req: &SchemaGetReq) -> Result<SchemaRegistryResp, String> {
    let key = schema_registry_key(&req.name, &req.version)?;
    let store = load_schema_registry_store();
    let schema = store
        .get(&key)
        .cloned()
        .ok_or_else(|| "schema not found".to_string())?;
    Ok(SchemaRegistryResp {
        ok: true,
        operator: "schema_registry_v1_get".to_string(),
        status: "done".to_string(),
        name: req.name.clone(),
        version: req.version.clone(),
        schema,
    })
}

fn run_schema_registry_infer_local(req: &SchemaInferReq) -> Result<SchemaInferResp, String> {
    let schema = infer_schema_from_rows(&req.rows);
    if let (Some(name), Some(version)) = (req.name.as_ref(), req.version.as_ref()) {
        let key = schema_registry_key(name, version)?;
        let mut store = load_schema_registry_store();
        store.insert(key, schema.clone());
        save_schema_registry_store(&store)?;
    }
    Ok(SchemaInferResp {
        ok: true,
        operator: "schema_registry_v1_infer".to_string(),
        status: "done".to_string(),
        name: req.name.clone(),
        version: req.version.clone(),
        schema: schema.clone(),
        stats: json!({"rows": req.rows.len(), "fields": schema.as_object().map(|m| m.len()).unwrap_or(0)}),
    })
}

fn run_schema_registry_register_v1(
    state: &AppState,
    req: SchemaRegisterReq,
) -> Result<SchemaRegistryResp, String> {
    let key = schema_registry_key(&req.name, &req.version)?;
    let resp = run_schema_registry_register_local(&req)?;
    if let Ok(mut reg) = state.schema_registry.lock() {
        reg.insert(key, req.schema.clone());
    }
    Ok(resp)
}

fn run_schema_registry_get_v1(
    state: &AppState,
    req: SchemaGetReq,
) -> Result<SchemaRegistryResp, String> {
    let key = schema_registry_key(&req.name, &req.version)?;
    if let Ok(reg) = state.schema_registry.lock()
        && let Some(schema) = reg.get(&key).cloned()
    {
        return Ok(SchemaRegistryResp {
            ok: true,
            operator: "schema_registry_v1_get".to_string(),
            status: "done".to_string(),
            name: req.name,
            version: req.version,
            schema,
        });
    }
    run_schema_registry_get_local(&req)
}

fn run_schema_registry_infer_v1(
    state: &AppState,
    req: SchemaInferReq,
) -> Result<SchemaInferResp, String> {
    let resp = run_schema_registry_infer_local(&req)?;
    if let (Some(name), Some(version)) = (req.name.as_ref(), req.version.as_ref())
        && let Ok(key) = schema_registry_key(name, version)
        && let Ok(mut reg) = state.schema_registry.lock()
    {
        reg.insert(key, resp.schema.clone());
    }
    Ok(resp)
}

fn schema_field_map(schema: &Value) -> HashMap<String, String> {
    schema
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("unknown").to_string()))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default()
}

fn run_schema_registry_check_compat_v2(
    state: &AppState,
    req: SchemaCompatReq,
) -> Result<SchemaCompatResp, String> {
    let from = run_schema_registry_get_v1(
        state,
        SchemaGetReq {
            name: req.name.clone(),
            version: req.from_version.clone(),
        },
    )?;
    let to = run_schema_registry_get_v1(
        state,
        SchemaGetReq {
            name: req.name.clone(),
            version: req.to_version.clone(),
        },
    )?;
    let from_map = schema_field_map(&from.schema);
    let to_map = schema_field_map(&to.schema);
    let mode = req
        .mode
        .unwrap_or_else(|| "backward".to_string())
        .to_lowercase();
    let mut breaking = Vec::new();
    let mut widening = Vec::new();
    for (k, old_t) in &from_map {
        match to_map.get(k) {
            None => breaking.push(format!("{k}: removed")),
            Some(new_t) if new_t != old_t => {
                let widening_pair = (old_t.as_str(), new_t.as_str());
                if matches!(
                    widening_pair,
                    ("int", "float") | ("int_like", "float") | ("bool_like", "string")
                ) {
                    widening.push(format!("{k}: {old_t}->{new_t}"));
                } else {
                    breaking.push(format!("{k}: {old_t}->{new_t}"));
                }
            }
            _ => {}
        }
    }
    if mode == "forward" || mode == "full" {
        for k in to_map.keys() {
            if !from_map.contains_key(k) && mode == "full" {
                breaking.push(format!("{k}: added in full mode"));
            }
        }
    }
    Ok(SchemaCompatResp {
        ok: true,
        operator: "schema_registry_v2_check_compat".to_string(),
        status: "done".to_string(),
        compatible: breaking.is_empty(),
        mode,
        breaking_fields: breaking,
        widening_fields: widening,
    })
}

fn run_schema_registry_suggest_migration_v2(
    state: &AppState,
    req: SchemaMigrationSuggestReq,
) -> Result<SchemaMigrationSuggestResp, String> {
    let compat = run_schema_registry_check_compat_v2(
        state,
        SchemaCompatReq {
            name: req.name.clone(),
            from_version: req.from_version.clone(),
            to_version: req.to_version.clone(),
            mode: Some("backward".to_string()),
        },
    )?;
    let mut steps = Vec::new();
    for b in compat.breaking_fields {
        if b.contains("removed") {
            let f = b.split(':').next().unwrap_or("").to_string();
            steps.push(json!({"action":"add_default","field":f,"default":Value::Null}));
        } else if b.contains("->") {
            let f = b.split(':').next().unwrap_or("").to_string();
            steps.push(json!({"action":"cast","field":f,"strategy":"safe_cast_or_null"}));
        }
    }
    for w in compat.widening_fields {
        let f = w.split(':').next().unwrap_or("").to_string();
        steps.push(json!({"action":"cast","field":f,"strategy":"widen"}));
    }
    Ok(SchemaMigrationSuggestResp {
        ok: true,
        operator: "schema_registry_v2_suggest_migration".to_string(),
        status: "done".to_string(),
        steps,
    })
}

struct WasmUdfRuntime {
    store: WasmStore<()>,
    memory: Option<wasmtime::Memory>,
    transform_f64: Option<wasmtime::TypedFunc<f64, f64>>,
    transform_i64: Option<wasmtime::TypedFunc<i64, i64>>,
    transform_str: Option<wasmtime::TypedFunc<(i32, i32), i64>>,
    alloc: Option<wasmtime::TypedFunc<i32, i32>>,
    dealloc: Option<wasmtime::TypedFunc<(i32, i32), ()>>,
}

fn wasm_unpack_ptr_len(v: i64) -> (usize, usize) {
    let ptr = ((v >> 32) as u32) as usize;
    let len = (v as u32) as usize;
    (ptr, len)
}

fn init_wasm_udf_runtime(b64: &str) -> Result<WasmUdfRuntime, String> {
    let bytes = BASE64_STD
        .decode(b64.as_bytes())
        .map_err(|e| format!("decode wasm_base64: {e}"))?;
    let engine = WasmEngine::default();
    let module =
        WasmModule::new(&engine, bytes).map_err(|e| format!("compile wasm module: {e}"))?;
    let linker = WasmLinker::new(&engine);
    let mut store = WasmStore::new(&engine, ());
    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| format!("instantiate wasm: {e}"))?;
    let memory = instance.get_memory(&mut store, "memory");
    let transform_f64 = instance
        .get_typed_func::<f64, f64>(&mut store, "transform_f64")
        .ok()
        .or_else(|| {
            instance
                .get_typed_func::<f64, f64>(&mut store, "transform")
                .ok()
        });
    let transform_i64 = instance
        .get_typed_func::<i64, i64>(&mut store, "transform_i64")
        .ok();
    let transform_str = instance
        .get_typed_func::<(i32, i32), i64>(&mut store, "transform_str")
        .ok();
    let alloc = instance
        .get_typed_func::<i32, i32>(&mut store, "alloc")
        .ok();
    let dealloc = instance
        .get_typed_func::<(i32, i32), ()>(&mut store, "dealloc")
        .ok();
    if transform_f64.is_none() && transform_i64.is_none() && transform_str.is_none() {
        return Err(
            "wasm exports missing: need one of transform_f64/transform/transform_i64/transform_str"
                .to_string(),
        );
    }
    Ok(WasmUdfRuntime {
        store,
        memory,
        transform_f64,
        transform_i64,
        transform_str,
        alloc,
        dealloc,
    })
}

fn wasm_call_string(runtime: &mut WasmUdfRuntime, input: &str) -> Result<String, String> {
    let Some(transform) = runtime.transform_str.as_ref() else {
        return Err("transform_str export missing".to_string());
    };
    let Some(alloc) = runtime.alloc.as_ref() else {
        return Err("alloc export missing".to_string());
    };
    let Some(memory) = runtime.memory.as_ref() else {
        return Err("memory export missing".to_string());
    };
    let in_bytes = input.as_bytes();
    let in_len = i32::try_from(in_bytes.len()).map_err(|_| "input string too large".to_string())?;
    let in_ptr = alloc
        .call(&mut runtime.store, in_len)
        .map_err(|e| format!("wasm alloc failed: {e}"))?;
    {
        let data = memory.data_mut(&mut runtime.store);
        let start = in_ptr as usize;
        let end = start.saturating_add(in_bytes.len());
        if end > data.len() {
            return Err("wasm memory overflow while writing input".to_string());
        }
        data[start..end].copy_from_slice(in_bytes);
    }
    let packed = transform
        .call(&mut runtime.store, (in_ptr, in_len))
        .map_err(|e| format!("wasm transform_str call failed: {e}"))?;
    let (out_ptr, out_len) = wasm_unpack_ptr_len(packed);
    let out_bytes = {
        let data = memory.data(&runtime.store);
        let end = out_ptr.saturating_add(out_len);
        if end > data.len() {
            return Err("wasm memory overflow while reading output".to_string());
        }
        data[out_ptr..end].to_vec()
    };
    if let Some(dealloc) = runtime.dealloc.as_ref() {
        let _ = dealloc.call(&mut runtime.store, (in_ptr, in_len));
        if out_len <= i32::MAX as usize && out_ptr <= i32::MAX as usize {
            let _ = dealloc.call(&mut runtime.store, (out_ptr as i32, out_len as i32));
        }
    }
    String::from_utf8(out_bytes).map_err(|e| format!("wasm output is not utf8: {e}"))
}

fn run_udf_wasm_v1(req: UdfWasmReq) -> Result<Value, String> {
    let op = req
        .op
        .unwrap_or_else(|| "identity".to_string())
        .to_lowercase();
    let mut wasm_error: Option<String> = None;
    let mut wasm_mode = "sandboxed_builtin".to_string();
    let mut runtime = req
        .wasm_base64
        .as_ref()
        .map(|b64| match init_wasm_udf_runtime(b64) {
            Ok(rt) => {
                wasm_mode = "wasm_abi".to_string();
                Some(rt)
            }
            Err(e) => {
                wasm_error = Some(e);
                None
            }
        })
        .unwrap_or(None);
    let mut used_abi: HashMap<String, usize> = HashMap::new();

    let mut out = Vec::new();
    for r in req.rows {
        let Some(mut obj) = r.as_object().cloned() else {
            continue;
        };
        let src = obj.get(&req.field).cloned().unwrap_or(Value::Null);
        let nv = if let Some(rt) = runtime.as_mut() {
            if let Some(v) = src.as_i64()
                && let Some(f) = rt.transform_i64.as_ref()
            {
                match f.call(&mut rt.store, v) {
                    Ok(x) => {
                        *used_abi.entry("i64".to_string()).or_insert(0) += 1;
                        Value::Number(x.into())
                    }
                    Err(e) => {
                        wasm_error = Some(format!("wasm i64 call failed: {e}"));
                        Value::Null
                    }
                }
            } else if let Some(v) = src.as_u64()
                && let Some(f) = rt.transform_i64.as_ref()
            {
                match i64::try_from(v) {
                    Ok(vv) => match f.call(&mut rt.store, vv) {
                        Ok(x) => {
                            *used_abi.entry("i64".to_string()).or_insert(0) += 1;
                            Value::Number(x.into())
                        }
                        Err(e) => {
                            wasm_error = Some(format!("wasm i64 call failed: {e}"));
                            Value::Null
                        }
                    },
                    Err(_) => Value::Null,
                }
            } else if let Some(v) = value_to_f64(&src)
                && let Some(f) = rt.transform_f64.as_ref()
            {
                match f.call(&mut rt.store, v) {
                    Ok(x) => {
                        *used_abi.entry("f64".to_string()).or_insert(0) += 1;
                        json!(x)
                    }
                    Err(e) => {
                        wasm_error = Some(format!("wasm f64 call failed: {e}"));
                        Value::Null
                    }
                }
            } else if rt.transform_str.is_some() {
                let s = value_to_string(&src);
                match wasm_call_string(rt, &s) {
                    Ok(s2) => {
                        *used_abi.entry("string".to_string()).or_insert(0) += 1;
                        Value::String(s2)
                    }
                    Err(e) => {
                        wasm_error = Some(e);
                        Value::Null
                    }
                }
            } else {
                wasm_error = Some("wasm runtime has no ABI matching input value type".to_string());
                Value::Null
            }
        } else {
            match op.as_str() {
                "identity" => src,
                "double" => value_to_f64(&src)
                    .map(|x| json!(x * 2.0))
                    .unwrap_or(Value::Null),
                "negate" => value_to_f64(&src).map(|x| json!(-x)).unwrap_or(Value::Null),
                "trim" => Value::String(value_to_string(&src).trim().to_string()),
                "upper" => Value::String(value_to_string(&src).to_uppercase()),
                _ => return Err(format!("unsupported udf op: {op}")),
            }
        };
        obj.insert(req.output_field.clone(), nv);
        out.push(Value::Object(obj));
    }
    let wasm_hash = req.wasm_base64.as_ref().map(|s| {
        let mut h = Sha256::new();
        h.update(s.as_bytes());
        format!("{:x}", h.finalize())
    });
    let out_len = out.len();
    Ok(json!({
        "ok": true,
        "operator": "udf_wasm_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out,
        "stats": {
            "input_rows": out_len,
            "mode": wasm_mode,
            "op": op,
            "wasm_hash": wasm_hash,
            "used_abi": used_abi,
            "note": "supported ABI: transform_f64(f64)->f64 or transform_i64(i64)->i64 or transform_str(ptr,len)->i64(high32=ptr,low32=len)+alloc/dealloc/memory",
            "wasm_error": wasm_error
        }
    }))
}

fn feature_store_path() -> PathBuf {
    env::var("AIWF_FEATURE_STORE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("feature_store.json"))
}

fn load_feature_store() -> HashMap<String, Value> {
    let p = feature_store_path();
    if let Ok(lock) = acquire_file_lock(&p) {
        let out = (|| {
            let Ok(txt) = fs::read_to_string(&p) else {
                return HashMap::new();
            };
            serde_json::from_str::<HashMap<String, Value>>(&txt).unwrap_or_default()
        })();
        release_file_lock(&lock);
        return out;
    }
    let Ok(txt) = fs::read_to_string(&p) else {
        return HashMap::new();
    };
    serde_json::from_str::<HashMap<String, Value>>(&txt).unwrap_or_default()
}

fn save_feature_store(store: &HashMap<String, Value>) -> Result<(), String> {
    let p = feature_store_path();
    let lock = acquire_file_lock(&p)?;
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create feature store dir: {e}"))?;
    }
    let s =
        serde_json::to_string_pretty(store).map_err(|e| format!("serialize feature store: {e}"))?;
    let out = fs::write(&p, s).map_err(|e| format!("write feature store: {e}"));
    release_file_lock(&lock);
    out
}

fn run_time_series_v1(req: TimeSeriesReq) -> Result<Value, String> {
    let window = req.window.unwrap_or(3).max(1);
    let groups = req.group_by.unwrap_or_default();
    let mut grouped: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for r in req.rows {
        let Some(obj) = r.as_object() else { continue };
        let k = if groups.is_empty() {
            "__all__".to_string()
        } else {
            groups
                .iter()
                .map(|g| value_to_string_or_null(obj.get(g)))
                .collect::<Vec<_>>()
                .join("|")
        };
        grouped.entry(k).or_default().push(obj.clone());
    }
    let mut out = Vec::new();
    for (_k, mut rows) in grouped {
        rows.sort_by(|a, b| {
            let av = value_to_string_or_null(a.get(&req.time_field));
            let bv = value_to_string_or_null(b.get(&req.time_field));
            parse_time_order_key(&av).cmp(&parse_time_order_key(&bv))
        });
        for i in 0..rows.len() {
            let mut row = rows[i].clone();
            let cur = row
                .get(&req.value_field)
                .and_then(value_to_f64)
                .unwrap_or(0.0);
            let start = i.saturating_sub(window - 1);
            let win = rows[start..=i]
                .iter()
                .filter_map(|r| r.get(&req.value_field).and_then(value_to_f64))
                .collect::<Vec<_>>();
            let ma = if win.is_empty() {
                Value::Null
            } else {
                json!(win.iter().sum::<f64>() / win.len() as f64)
            };
            let mom = if i >= 1 {
                let prev = rows[i - 1]
                    .get(&req.value_field)
                    .and_then(value_to_f64)
                    .unwrap_or(0.0);
                json!(cur - prev)
            } else {
                Value::Null
            };
            let yoy = if i >= 12 {
                let prev = rows[i - 12]
                    .get(&req.value_field)
                    .and_then(value_to_f64)
                    .unwrap_or(0.0);
                if prev.abs() < f64::EPSILON {
                    Value::Null
                } else {
                    json!((cur - prev) / prev)
                }
            } else {
                Value::Null
            };
            row.insert("ts_moving_avg".to_string(), ma);
            row.insert("ts_mom".to_string(), mom);
            row.insert("ts_yoy".to_string(), yoy);
            out.push(Value::Object(row));
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "time_series_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out,
        "stats": {"window": window}
    }))
}

fn parse_time_order_key(s: &str) -> i64 {
    let t = s.trim();
    if let Ok(v) = t.parse::<i64>() {
        return v;
    }
    let fmts_dt = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
    ];
    for f in fmts_dt {
        if let Ok(dt) = NaiveDateTime::parse_from_str(t, f) {
            return dt.and_utc().timestamp();
        }
    }
    let fmts_d = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m", "%Y/%m", "%Y%m"];
    for f in fmts_d {
        let parsed = if f == "%Y-%m" {
            NaiveDate::parse_from_str(&format!("{t}-01"), "%Y-%m-%d")
        } else if f == "%Y/%m" {
            NaiveDate::parse_from_str(&format!("{t}/01"), "%Y/%m/%d")
        } else if f == "%Y%m" {
            NaiveDate::parse_from_str(&format!("{t}01"), "%Y%m%d")
        } else {
            NaiveDate::parse_from_str(t, f)
        };
        if let Ok(d) = parsed {
            return d
                .and_hms_opt(0, 0, 0)
                .map(|dt| dt.and_utc().timestamp())
                .unwrap_or(i64::MAX - 1);
        }
    }
    i64::MAX
}

fn run_stats_v1(req: StatsReq) -> Result<Value, String> {
    let pairs = req
        .rows
        .iter()
        .filter_map(|r| {
            let o = r.as_object()?;
            let x = o.get(&req.x_field).and_then(value_to_f64)?;
            let y = o.get(&req.y_field).and_then(value_to_f64)?;
            Some((x, y))
        })
        .collect::<Vec<_>>();
    if pairs.len() < 2 {
        return Err("stats_v1 requires at least 2 numeric pairs".to_string());
    }
    let n = pairs.len() as f64;
    let sum_x = pairs.iter().map(|(x, _)| *x).sum::<f64>();
    let sum_y = pairs.iter().map(|(_, y)| *y).sum::<f64>();
    let mean_x = sum_x / n;
    let mean_y = sum_y / n;
    let sxy = pairs
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>();
    let sxx = pairs.iter().map(|(x, _)| (x - mean_x).powi(2)).sum::<f64>();
    let syy = pairs.iter().map(|(_, y)| (y - mean_y).powi(2)).sum::<f64>();
    let corr = if sxx <= 0.0 || syy <= 0.0 {
        0.0
    } else {
        sxy / (sxx.sqrt() * syy.sqrt())
    };
    let slope = if sxx <= 0.0 { 0.0 } else { sxy / sxx };
    let intercept = mean_y - slope * mean_x;
    let mut residual_ss = 0.0;
    for (x, y) in &pairs {
        let pred = intercept + slope * *x;
        residual_ss += (*y - pred).powi(2);
    }
    let dof = (pairs.len() as f64 - 2.0).max(1.0);
    let stderr = if sxx <= 0.0 {
        f64::INFINITY
    } else {
        (residual_ss / dof / sxx).sqrt()
    };
    let t = if !stderr.is_finite() || stderr <= 0.0 {
        0.0
    } else {
        slope / stderr
    };
    let p = p_value_from_t(t.abs(), dof);
    let tcrit = if let Ok(dist) = StudentsT::new(0.0, 1.0, dof) {
        dist.inverse_cdf(0.975)
    } else {
        1.96
    };
    let ci_low = slope - tcrit * stderr;
    let ci_high = slope + tcrit * stderr;
    let robust_median_y = {
        let mut ys = pairs.iter().map(|(_, y)| *y).collect::<Vec<_>>();
        ys.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        ys[ys.len() / 2]
    };
    Ok(json!({
        "ok": true,
        "operator": "stats_v1",
        "status": "done",
        "run_id": req.run_id,
        "metrics": {
            "count": pairs.len(),
            "correlation": corr,
            "slope": slope,
            "intercept": intercept,
            "mean_x": mean_x,
            "mean_y": mean_y,
            "slope_stderr": stderr,
            "slope_t": t,
            "slope_p_value": p,
            "slope_ci95": [ci_low, ci_high],
            "median_y": robust_median_y
        }
    }))
}

fn normalize_entity(s: &str) -> String {
    s.trim()
        .to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c.is_alphanumeric() {
                c
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn run_entity_linking_v1(req: EntityLinkReq) -> Result<Value, String> {
    let id_field = req.id_field.unwrap_or_else(|| "entity_id".to_string());
    let mut out = Vec::new();
    let mut dict = Map::new();
    for r in req.rows {
        let Some(mut obj) = r.as_object().cloned() else {
            continue;
        };
        let raw = value_to_string_or_null(obj.get(&req.field));
        let norm = normalize_entity(&raw);
        let mut h = Sha256::new();
        h.update(norm.as_bytes());
        let id = format!("{:x}", h.finalize());
        let short = id.chars().take(12).collect::<String>();
        obj.insert(id_field.clone(), Value::String(short.clone()));
        obj.insert("entity_norm".to_string(), Value::String(norm.clone()));
        dict.insert(short, Value::String(norm));
        out.push(Value::Object(obj));
    }
    Ok(json!({
        "ok": true,
        "operator": "entity_linking_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out,
        "dictionary": dict
    }))
}

fn run_table_reconstruct_v1(req: TableReconstructReq) -> Result<Value, String> {
    let lines = if let Some(v) = req.lines {
        v
    } else {
        req.text
            .unwrap_or_default()
            .replace("\r\n", "\n")
            .split('\n')
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    };
    let delim = req.delimiter.unwrap_or_else(|| "\\s{2,}|\\t".to_string());
    let re = Regex::new(&delim).map_err(|e| format!("invalid delimiter regex: {e}"))?;
    let mut rows = Vec::new();
    let mut max_cols = 0usize;
    for line in lines {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let cols = re
            .split(t)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        if cols.is_empty() {
            continue;
        }
        max_cols = max_cols.max(cols.len());
        rows.push(cols);
    }
    if rows.is_empty() {
        return Err("table_reconstruct_v1 empty input".to_string());
    }
    // Normalize ragged rows to same width for downstream table rendering.
    let mut norm = Vec::new();
    for mut r in rows {
        if r.len() < max_cols {
            r.resize(max_cols, "".to_string());
        } else if r.len() > max_cols {
            r.truncate(max_cols);
        }
        norm.push(r);
    }
    let header = norm.first().cloned().unwrap_or_default();
    let body = norm
        .iter()
        .skip(1)
        .map(|r| json!({"cells": r, "col_count": r.len()}))
        .collect::<Vec<_>>();
    Ok(json!({
        "ok": true,
        "operator": "table_reconstruct_v1",
        "status": "done",
        "run_id": req.run_id,
        "header": header,
        "rows": body,
        "stats": {"col_count": max_cols, "row_count": norm.len()}
    }))
}

fn p_value_from_t(t_abs: f64, dof: f64) -> f64 {
    if !dof.is_finite() || dof <= 0.0 {
        return 1.0;
    }
    if let Ok(dist) = StudentsT::new(0.0, 1.0, dof) {
        2.0 * (1.0 - dist.cdf(t_abs.max(0.0)))
    } else {
        1.0
    }
}

fn run_feature_store_upsert_v1(req: FeatureStoreUpsertReq) -> Result<Value, String> {
    let mut store = load_feature_store();
    let mut upserted = 0usize;
    for r in req.rows {
        let Some(obj) = r.as_object() else { continue };
        let key = value_to_string_or_null(obj.get(&req.key_field));
        if key.trim().is_empty() || key == "null" {
            continue;
        }
        store.insert(key, Value::Object(obj.clone()));
        upserted += 1;
    }
    save_feature_store(&store)?;
    Ok(json!({
        "ok": true,
        "operator": "feature_store_v1_upsert",
        "status": "done",
        "run_id": req.run_id,
        "upserted": upserted,
        "total_keys": store.len()
    }))
}

fn run_feature_store_get_v1(req: FeatureStoreGetReq) -> Result<Value, String> {
    let store = load_feature_store();
    Ok(json!({
        "ok": true,
        "operator": "feature_store_v1_get",
        "status": "done",
        "run_id": req.run_id,
        "key": req.key,
        "value": store.get(&req.key).cloned().unwrap_or(Value::Null)
    }))
}

fn run_rule_simulator_v1(req: RuleSimulatorReq) -> Result<Value, String> {
    let base = run_transform_rows_v2(TransformRowsReq {
        run_id: req.run_id.clone(),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(req.rows.clone()),
        rules: Some(req.rules),
        rules_dsl: None,
        quality_gates: None,
        schema_hint: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    })?;
    let cand = run_transform_rows_v2(TransformRowsReq {
        run_id: req.run_id.clone(),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(req.rows),
        rules: Some(req.candidate_rules),
        rules_dsl: None,
        quality_gates: None,
        schema_hint: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    })?;
    let base_rows = base.rows;
    let cand_rows = cand.rows;
    let min_n = base_rows.len().min(cand_rows.len());
    let mut field_changed = HashMap::<String, usize>::new();
    for i in 0..min_n {
        let Some(bm) = base_rows[i].as_object() else {
            continue;
        };
        let Some(cm) = cand_rows[i].as_object() else {
            continue;
        };
        let keys = bm.keys().chain(cm.keys()).cloned().collect::<HashSet<_>>();
        for k in keys {
            let bv = bm.get(&k).cloned().unwrap_or(Value::Null);
            let cv = cm.get(&k).cloned().unwrap_or(Value::Null);
            if bv != cv {
                *field_changed.entry(k).or_insert(0) += 1;
            }
        }
    }
    let mut top_changed = field_changed
        .into_iter()
        .map(|(k, v)| json!({"field":k,"changed_rows":v}))
        .collect::<Vec<_>>();
    top_changed.sort_by(|a, b| {
        let av = a.get("changed_rows").and_then(|v| v.as_u64()).unwrap_or(0);
        let bv = b.get("changed_rows").and_then(|v| v.as_u64()).unwrap_or(0);
        bv.cmp(&av)
    });
    Ok(json!({
        "ok": true,
        "operator": "rule_simulator_v1",
        "status": "done",
        "run_id": req.run_id,
        "baseline_rows": base.stats.output_rows,
        "candidate_rows": cand.stats.output_rows,
        "delta_rows": cand.stats.output_rows as i64 - base.stats.output_rows as i64,
        "row_overlap_compared": min_n,
        "field_impact_top": top_changed.into_iter().take(20).collect::<Vec<_>>(),
        "baseline_quality": base.quality,
        "candidate_quality": cand.quality
    }))
}

fn run_constraint_solver_v1(req: ConstraintSolverReq) -> Result<Value, String> {
    let mut violations = Vec::new();
    for (idx, row) in req.rows.iter().enumerate() {
        let Some(obj) = row.as_object() else { continue };
        for c in &req.constraints {
            let Some(o) = c.as_object() else { continue };
            let kind = o.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            match kind {
                "sum_equals" => {
                    let left = o
                        .get("left")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>();
                    let right = o.get("right").and_then(|v| v.as_str()).unwrap_or("");
                    let lv = left
                        .iter()
                        .map(|f| obj.get(f).and_then(value_to_f64).unwrap_or(0.0))
                        .sum::<f64>();
                    let rv = obj.get(right).and_then(value_to_f64).unwrap_or(0.0);
                    let tol = o.get("tolerance").and_then(value_to_f64).unwrap_or(1e-6);
                    if (lv - rv).abs() > tol {
                        violations.push(json!({"row_index":idx,"kind":"sum_equals","left":left,"right":right,"left_value":lv,"right_value":rv,"tolerance":tol}));
                    }
                }
                "non_negative" => {
                    let field = o.get("field").and_then(|v| v.as_str()).unwrap_or("");
                    let v = obj.get(field).and_then(value_to_f64).unwrap_or(0.0);
                    if v < 0.0 {
                        violations.push(
                            json!({"row_index":idx,"kind":"non_negative","field":field,"value":v}),
                        );
                    }
                }
                _ => {}
            }
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "constraint_solver_v1",
        "status": "done",
        "run_id": req.run_id,
        "passed": violations.is_empty(),
        "violations": violations
    }))
}

fn run_chart_data_prep_v1(req: ChartDataPrepReq) -> Result<Value, String> {
    let top_n = req.top_n.unwrap_or(100).max(1);
    let mut m: HashMap<String, HashMap<String, f64>> = HashMap::new();
    for r in req.rows {
        let Some(obj) = r.as_object() else { continue };
        let cat = value_to_string_or_null(obj.get(&req.category_field));
        let ser = req
            .series_field
            .as_ref()
            .map(|f| value_to_string_or_null(obj.get(f)))
            .unwrap_or_else(|| "value".to_string());
        let val = obj
            .get(&req.value_field)
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        *m.entry(cat).or_default().entry(ser).or_insert(0.0) += val;
    }
    let mut cats = m.into_iter().collect::<Vec<_>>();
    cats.sort_by(|a, b| a.0.cmp(&b.0));
    cats.truncate(top_n);
    let categories = cats
        .iter()
        .map(|(c, _)| Value::String(c.clone()))
        .collect::<Vec<_>>();
    let mut series_keys = HashSet::new();
    for (_, sm) in &cats {
        for k in sm.keys() {
            series_keys.insert(k.clone());
        }
    }
    let mut series = Vec::new();
    let mut sk = series_keys.into_iter().collect::<Vec<_>>();
    sk.sort();
    for s in sk {
        let data = cats
            .iter()
            .map(|(_, sm)| json!(sm.get(&s).copied().unwrap_or(0.0)))
            .collect::<Vec<_>>();
        series.push(json!({"name": s, "data": data}));
    }
    Ok(json!({
        "ok": true,
        "operator": "chart_data_prep_v1",
        "status": "done",
        "run_id": req.run_id,
        "chart": {"categories": categories, "series": series}
    }))
}

fn run_diff_audit_v1(req: DiffAuditReq) -> Result<Value, String> {
    if req.keys.is_empty() {
        return Err("diff_audit_v1 requires keys".to_string());
    }
    let key_of = |o: &Map<String, Value>| {
        req.keys
            .iter()
            .map(|k| value_to_string_or_null(o.get(k)))
            .collect::<Vec<_>>()
            .join("|")
    };
    let mut left = HashMap::<String, Map<String, Value>>::new();
    let mut right = HashMap::<String, Map<String, Value>>::new();
    for r in req.left_rows {
        if let Some(o) = r.as_object() {
            left.insert(key_of(o), o.clone());
        }
    }
    for r in req.right_rows {
        if let Some(o) = r.as_object() {
            right.insert(key_of(o), o.clone());
        }
    }
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();
    for (k, rv) in &right {
        if !left.contains_key(k) {
            added.push(Value::Object(rv.clone()));
        }
    }
    for (k, lv) in &left {
        if !right.contains_key(k) {
            removed.push(Value::Object(lv.clone()));
        } else if let Some(rv) = right.get(k)
            && lv != rv
        {
            changed.push(json!({"key":k,"left":lv,"right":rv}));
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "diff_audit_v1",
        "status": "done",
        "run_id": req.run_id,
        "summary": {"added": added.len(), "removed": removed.len(), "changed": changed.len()},
        "added": added,
        "removed": removed,
        "changed": changed
    }))
}

fn vector_index_store_path() -> PathBuf {
    env::var("AIWF_VECTOR_INDEX_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("vector_index.json"))
}

fn stream_state_store_path() -> PathBuf {
    env::var("AIWF_STREAM_STATE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("stream_state.json"))
}

fn runtime_stats_store_path() -> PathBuf {
    env::var("AIWF_RUNTIME_STATS_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("runtime_stats.json"))
}

fn explain_feedback_store_path() -> PathBuf {
    env::var("AIWF_EXPLAIN_FEEDBACK_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("explain_feedback.json"))
}

fn vector_index_v2_store_path() -> PathBuf {
    env::var("AIWF_VECTOR_INDEX_V2_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("vector_index_v2.json"))
}

fn stream_reliability_store_path() -> PathBuf {
    env::var("AIWF_STREAM_RELIABILITY_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("stream_reliability.json"))
}

fn tenant_isolation_store_path() -> PathBuf {
    env::var("AIWF_TENANT_ISOLATION_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("tenant_isolation.json"))
}

fn operator_policy_store_path() -> PathBuf {
    env::var("AIWF_OPERATOR_POLICY_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("operator_policy.json"))
}

fn perf_baseline_store_path() -> PathBuf {
    env::var("AIWF_PERF_BASELINE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("perf_baseline.json"))
}

fn normalize_error_code(err: &str) -> String {
    let e = err.trim().to_lowercase();
    if e.contains("quota") || e.contains("tenant") {
        return "QUOTA_REJECT".to_string();
    }
    if e.contains("timeout") {
        return "TIMEOUT".to_string();
    }
    if e.contains("sandbox_egress_blocked") || e.contains("not_allowed") || e.contains("forbidden")
    {
        return "POLICY_BLOCKED".to_string();
    }
    if e.contains("schema") || e.contains("invalid") || e.contains("missing") {
        return "INPUT_INVALID".to_string();
    }
    if e.contains("rust_http_5") || e.contains("upstream") {
        return "UPSTREAM_5XX".to_string();
    }
    if e.contains("rust_http_4") {
        return "UPSTREAM_4XX".to_string();
    }
    "UNKNOWN".to_string()
}

fn maybe_inject_fault(operator: &str) -> Result<(), String> {
    let cfg = env::var("AIWF_FAULT_INJECT").unwrap_or_default();
    if cfg.trim().is_empty() {
        return Ok(());
    }
    // format: operator:fail or operator:timeout
    let mut parts = cfg.split(':');
    let op = parts.next().unwrap_or_default().trim();
    let mode = parts.next().unwrap_or("fail").trim().to_lowercase();
    if !op.eq_ignore_ascii_case(operator) {
        return Ok(());
    }
    if mode == "timeout" {
        std::thread::sleep(std::time::Duration::from_millis(200));
        return Err(format!("fault injected timeout: {operator}"));
    }
    Err(format!("fault injected failure: {operator}"))
}

fn stream_state_sqlite_path(req: &StreamStateV2Req) -> PathBuf {
    if let Some(p) = req.db_path.as_ref().filter(|s| !s.trim().is_empty()) {
        return PathBuf::from(p);
    }
    env::var("AIWF_STREAM_STATE_SQLITE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("stream_state.sqlite"))
}

fn ensure_stream_state_sqlite(db: &Path) -> Result<SqliteConnection, String> {
    if let Some(parent) = db.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create stream sqlite dir: {e}"))?;
    }
    let conn = SqliteConnection::open(db).map_err(|e| format!("open stream sqlite: {e}"))?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS stream_state (
            stream_key TEXT PRIMARY KEY,
            state_json TEXT NOT NULL,
            offset_val INTEGER NOT NULL,
            version INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )",
        [],
    )
    .map_err(|e| format!("init stream sqlite: {e}"))?;
    Ok(conn)
}

fn load_kv_store(path: &Path) -> HashMap<String, Value> {
    if let Ok(lock) = acquire_file_lock(path) {
        let out = (|| {
            let Ok(txt) = fs::read_to_string(path) else {
                return HashMap::new();
            };
            serde_json::from_str::<HashMap<String, Value>>(&txt).unwrap_or_default()
        })();
        release_file_lock(&lock);
        return out;
    }
    HashMap::new()
}

fn save_kv_store(path: &Path, store: &HashMap<String, Value>) -> Result<(), String> {
    let lock = acquire_file_lock(path)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create kv dir: {e}"))?;
    }
    let s = serde_json::to_string_pretty(store).map_err(|e| format!("serialize kv: {e}"))?;
    let out = fs::write(path, s).map_err(|e| format!("write kv: {e}"));
    release_file_lock(&lock);
    out
}

fn tokenize_text(s: &str) -> Vec<String> {
    s.to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c.is_alphanumeric() {
                c
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .map(|x| x.to_string())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>()
}

fn term_freq(tokens: &[String]) -> HashMap<String, f64> {
    let mut m = HashMap::<String, f64>::new();
    for t in tokens {
        *m.entry(t.clone()).or_insert(0.0) += 1.0;
    }
    let n = tokens.len().max(1) as f64;
    for v in m.values_mut() {
        *v /= n;
    }
    m
}

fn cosine_sparse(a: &HashMap<String, f64>, b: &HashMap<String, f64>) -> f64 {
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for v in a.values() {
        na += v * v;
    }
    for v in b.values() {
        nb += v * v;
    }
    for (k, va) in a {
        if let Some(vb) = b.get(k) {
            dot += va * vb;
        }
    }
    if na <= 0.0 || nb <= 0.0 {
        0.0
    } else {
        dot / (na.sqrt() * nb.sqrt())
    }
}

fn run_vector_index_build_v1(req: VectorIndexBuildReq) -> Result<Value, String> {
    let mut docs = Vec::new();
    for r in req.rows {
        let Some(o) = r.as_object() else { continue };
        let id = value_to_string_or_null(o.get(&req.id_field));
        let text = value_to_string_or_null(o.get(&req.text_field));
        if id.trim().is_empty() || text.trim().is_empty() {
            continue;
        }
        let tf = term_freq(&tokenize_text(&text));
        docs.push(json!({"id": id, "text": text, "tf": tf}));
    }
    let mut store = load_kv_store(&vector_index_store_path());
    store.insert(
        "default".to_string(),
        json!({"updated_at": utc_now_iso(), "size": docs.len(), "docs": docs}),
    );
    save_kv_store(&vector_index_store_path(), &store)?;
    Ok(
        json!({"ok": true, "operator": "vector_index_v1_build", "status": "done", "run_id": req.run_id, "size": docs.len()}),
    )
}

fn run_vector_index_search_v1(req: VectorIndexSearchReq) -> Result<Value, String> {
    let top_k = req.top_k.unwrap_or(5).clamp(1, 100);
    let store = load_kv_store(&vector_index_store_path());
    let docs = store
        .get("default")
        .and_then(|v| v.get("docs"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let qtf = term_freq(&tokenize_text(&req.query));
    let mut scored = Vec::new();
    for d in docs {
        let id = d.get("id").cloned().unwrap_or(Value::Null);
        let text = d.get("text").cloned().unwrap_or(Value::Null);
        let tfm = d
            .get("tf")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_f64().map(|x| (k.clone(), x)))
                    .collect::<HashMap<String, f64>>()
            })
            .unwrap_or_default();
        let score = cosine_sparse(&qtf, &tfm);
        scored.push(json!({"id": id, "text": text, "score": score}));
    }
    scored.sort_by(|a, b| {
        let av = a.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let bv = b.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
    });
    scored.truncate(top_k);
    Ok(
        json!({"ok": true, "operator": "vector_index_v1_search", "status": "done", "run_id": req.run_id, "hits": scored}),
    )
}

fn run_evidence_rank_v1(req: EvidenceRankReq) -> Result<Value, String> {
    let t_field = req.time_field.unwrap_or_else(|| "time".to_string());
    let s_field = req
        .source_field
        .unwrap_or_else(|| "source_score".to_string());
    let r_field = req
        .relevance_field
        .unwrap_or_else(|| "relevance".to_string());
    let c_field = req
        .consistency_field
        .unwrap_or_else(|| "consistency".to_string());
    let now = unix_now_sec() as f64;
    let mut out = Vec::new();
    for r in req.rows {
        let Some(mut o) = r.as_object().cloned() else {
            continue;
        };
        let rel = o.get(&r_field).and_then(value_to_f64).unwrap_or(0.0);
        let src = o.get(&s_field).and_then(value_to_f64).unwrap_or(0.0);
        let cons = o.get(&c_field).and_then(value_to_f64).unwrap_or(0.0);
        let time_score = o
            .get(&t_field)
            .and_then(|v| v.as_str())
            .map(parse_time_order_key)
            .map(|ts| {
                if ts <= 0 {
                    0.0
                } else {
                    let age_days = ((now - ts as f64) / 86400.0).max(0.0);
                    (1.0 / (1.0 + age_days / 30.0)).clamp(0.0, 1.0)
                }
            })
            .unwrap_or(0.5);
        let score = 0.45 * rel + 0.25 * src + 0.20 * cons + 0.10 * time_score;
        o.insert("evidence_score".to_string(), json!(score));
        out.push(Value::Object(o));
    }
    out.sort_by(|a, b| {
        let av = a
            .get("evidence_score")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let bv = b
            .get("evidence_score")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(
        json!({"ok": true, "operator": "evidence_rank_v1", "status": "done", "run_id": req.run_id, "rows": out}),
    )
}

fn canonical_claim(s: &str) -> String {
    tokenize_text(s).join(" ")
}

fn run_fact_crosscheck_v1(req: FactCrosscheckReq) -> Result<Value, String> {
    let src_field = req.source_field.unwrap_or_else(|| "source".to_string());
    let mut groups: HashMap<String, HashSet<String>> = HashMap::new();
    for r in req.rows {
        let Some(o) = r.as_object() else { continue };
        let claim = canonical_claim(&value_to_string_or_null(o.get(&req.claim_field)));
        if claim.is_empty() {
            continue;
        }
        let src = value_to_string_or_null(o.get(&src_field));
        groups.entry(claim).or_default().insert(src);
    }
    let mut out = Vec::new();
    for (claim, srcs) in groups {
        let status = if srcs.len() >= 2 {
            "supported"
        } else {
            "unverified"
        };
        out.push(
            json!({"claim": claim, "status": status, "source_count": srcs.len(), "sources": srcs}),
        );
    }
    Ok(
        json!({"ok": true, "operator": "fact_crosscheck_v1", "status": "done", "run_id": req.run_id, "results": out}),
    )
}

fn run_timeseries_forecast_v1(req: TimeSeriesForecastReq) -> Result<Value, String> {
    let horizon = req.horizon.unwrap_or(3).clamp(1, 60);
    let method = req
        .method
        .unwrap_or_else(|| "naive_drift".to_string())
        .to_lowercase();
    let mut rows = req
        .rows
        .into_iter()
        .filter_map(|r| r.as_object().cloned())
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        parse_time_order_key(&value_to_string_or_null(a.get(&req.time_field))).cmp(
            &parse_time_order_key(&value_to_string_or_null(b.get(&req.time_field))),
        )
    });
    let vals = rows
        .iter()
        .filter_map(|o| o.get(&req.value_field).and_then(value_to_f64))
        .collect::<Vec<_>>();
    if vals.is_empty() {
        return Err("timeseries_forecast_v1 requires non-empty numeric series".to_string());
    }
    let first = vals[0];
    let last = *vals.last().unwrap_or(&first);
    let drift = if vals.len() > 1 {
        (last - first) / (vals.len() as f64 - 1.0)
    } else {
        0.0
    };
    let mut forecast = Vec::new();
    for h in 1..=horizon {
        let pred = if method == "naive_last" {
            last
        } else {
            last + drift * h as f64
        };
        forecast.push(json!({"step": h, "prediction": pred}));
    }
    Ok(
        json!({"ok": true, "operator": "timeseries_forecast_v1", "status": "done", "run_id": req.run_id, "method": method, "forecast": forecast}),
    )
}

fn run_finance_ratio_v1(req: FinanceRatioReq) -> Result<Value, String> {
    let mut out = Vec::new();
    for r in req.rows {
        let Some(mut o) = r.as_object().cloned() else {
            continue;
        };
        let ca = o
            .get("current_assets")
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        let cl = o
            .get("current_liabilities")
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        let debt = o.get("total_debt").and_then(value_to_f64).unwrap_or(0.0);
        let equity = o.get("total_equity").and_then(value_to_f64).unwrap_or(0.0);
        let rev = o.get("revenue").and_then(value_to_f64).unwrap_or(0.0);
        let ni = o.get("net_income").and_then(value_to_f64).unwrap_or(0.0);
        let ocf = o
            .get("operating_cash_flow")
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        let qr = if cl.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(ca / cl)
        };
        let d2e = if equity.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(debt / equity)
        };
        let nm = if rev.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(ni / rev)
        };
        let ocf_margin = if rev.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(ocf / rev)
        };
        o.insert("ratio_current".to_string(), qr);
        o.insert("ratio_debt_to_equity".to_string(), d2e);
        o.insert("ratio_net_margin".to_string(), nm);
        o.insert("ratio_ocf_margin".to_string(), ocf_margin);
        out.push(Value::Object(o));
    }
    Ok(
        json!({"ok": true, "operator": "finance_ratio_v1", "status": "done", "run_id": req.run_id, "rows": out}),
    )
}

fn run_anomaly_explain_v1(req: AnomalyExplainReq) -> Result<Value, String> {
    let th = req.threshold.unwrap_or(0.8);
    let mut anomalies = Vec::new();
    for (idx, r) in req.rows.iter().enumerate() {
        let Some(o) = r.as_object() else { continue };
        let score = o
            .get(&req.score_field)
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        if score < th {
            continue;
        }
        let mut contrib = Vec::new();
        for (k, v) in o {
            if k == &req.score_field {
                continue;
            }
            if let Some(n) = value_to_f64(v) {
                contrib.push((k.clone(), n.abs()));
            }
        }
        contrib.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        anomalies.push(json!({
            "row_index": idx,
            "score": score,
            "top_contributors": contrib.into_iter().take(3).map(|(k,v)| json!({"field":k,"importance":v})).collect::<Vec<_>>()
        }));
    }
    Ok(
        json!({"ok": true, "operator": "anomaly_explain_v1", "status": "done", "run_id": req.run_id, "anomalies": anomalies}),
    )
}

fn template_lookup(data: &Value, key: &str) -> Option<Value> {
    let mut cur = data;
    for p in key.split('.') {
        if let Some(o) = cur.as_object() {
            cur = o.get(p)?;
        } else {
            return None;
        }
    }
    Some(cur.clone())
}

fn run_template_bind_v1(req: TemplateBindReq) -> Result<Value, String> {
    let re = Regex::new(r"\{\{\s*([A-Za-z0-9_.-]+)\s*\}\}").map_err(|e| e.to_string())?;
    let mut out = req.template_text.clone();
    let mut binds = 0usize;
    for cap in re.captures_iter(&req.template_text) {
        let all = cap.get(0).map(|m| m.as_str()).unwrap_or("");
        let key = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        if all.is_empty() || key.is_empty() {
            continue;
        }
        if let Some(v) = template_lookup(&req.data, key) {
            out = out.replace(all, &value_to_string(&v));
            binds += 1;
        }
    }
    Ok(
        json!({"ok": true, "operator": "template_bind_v1", "status": "done", "run_id": req.run_id, "bound_text": out, "bind_count": binds}),
    )
}

fn run_provenance_sign_v1(req: ProvenanceSignReq) -> Result<Value, String> {
    let payload_text = serde_json::to_string(&req.payload).map_err(|e| e.to_string())?;
    let prev = req.prev_hash.unwrap_or_default();
    let ts = utc_now_iso();
    let mut h = Sha256::new();
    h.update(format!("{prev}|{ts}|{payload_text}").as_bytes());
    let hash = format!("{:x}", h.finalize());
    Ok(json!({
        "ok": true,
        "operator": "provenance_sign_v1",
        "status": "done",
        "run_id": req.run_id,
        "record": {"timestamp": ts, "prev_hash": prev, "hash": hash}
    }))
}

fn run_stream_state_save_v1(req: StreamStateSaveReq) -> Result<Value, String> {
    let mut store = load_kv_store(&stream_state_store_path());
    store.insert(
        req.stream_key.clone(),
        json!({
            "state": req.state,
            "offset": req.offset.unwrap_or(0),
            "updated_at": utc_now_iso()
        }),
    );
    save_kv_store(&stream_state_store_path(), &store)?;
    Ok(
        json!({"ok": true, "operator": "stream_state_v1_save", "status": "done", "run_id": req.run_id, "stream_key": req.stream_key}),
    )
}

fn run_stream_state_load_v1(req: StreamStateLoadReq) -> Result<Value, String> {
    let store = load_kv_store(&stream_state_store_path());
    let v = store.get(&req.stream_key).cloned().unwrap_or(Value::Null);
    Ok(
        json!({"ok": true, "operator": "stream_state_v1_load", "status": "done", "run_id": req.run_id, "stream_key": req.stream_key, "value": v}),
    )
}

fn run_query_lang_v1(req: QueryLangReq) -> Result<Value, String> {
    let q = req.query.trim();
    if q.is_empty() {
        return Err("query_lang_v1 query is empty".to_string());
    }
    let mut rows = req.rows;
    if let Some(rest) = q.strip_prefix("where ") {
        let cond = rest.trim();
        let parts = ["==", "!=", ">=", "<=", ">", "<"]
            .iter()
            .find_map(|op| cond.find(op).map(|p| (*op, p)));
        if let Some((op, pos)) = parts {
            let field = cond[..pos].trim();
            let rhs = cond[pos + op.len()..].trim().trim_matches('"');
            rows = rows
                .into_iter()
                .filter(|r| {
                    let o = r.as_object();
                    let lv = o
                        .map(|m| value_to_string_or_null(m.get(field)))
                        .unwrap_or_default();
                    let lnum = lv.parse::<f64>().ok();
                    let rnum = rhs.parse::<f64>().ok();
                    match op {
                        "==" => lv == rhs,
                        "!=" => lv != rhs,
                        ">" => lnum.zip(rnum).map(|(a, b)| a > b).unwrap_or(false),
                        "<" => lnum.zip(rnum).map(|(a, b)| a < b).unwrap_or(false),
                        ">=" => lnum.zip(rnum).map(|(a, b)| a >= b).unwrap_or(false),
                        "<=" => lnum.zip(rnum).map(|(a, b)| a <= b).unwrap_or(false),
                        _ => false,
                    }
                })
                .collect();
        }
    } else if let Some(rest) = q.strip_prefix("select ") {
        let fields = rest
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        if !fields.is_empty() {
            rows = rows
                .into_iter()
                .filter_map(|r| {
                    let o = r.as_object()?;
                    let mut m = Map::new();
                    for f in &fields {
                        if let Some(v) = o.get(f) {
                            m.insert(f.clone(), v.clone());
                        }
                    }
                    Some(Value::Object(m))
                })
                .collect();
        }
    } else if let Some(rest) = q.strip_prefix("limit ") {
        let n = rest.trim().parse::<usize>().unwrap_or(100);
        rows.truncate(n);
    }
    Ok(
        json!({"ok": true, "operator": "query_lang_v1", "status": "done", "run_id": req.run_id, "rows": rows}),
    )
}

fn parse_filter_eq_map(v: &Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(obj) = v.as_object() {
        for (k, vv) in obj {
            out.insert(k.clone(), value_to_string(vv));
        }
    }
    out
}

fn run_columnar_eval_v1(req: ColumnarEvalV1Req) -> Result<Value, String> {
    maybe_inject_fault("columnar_eval_v1")?;
    let begin = Instant::now();
    let rows_in = req.rows.len();
    let select_fields = req.select_fields.unwrap_or_default();
    let filter_eq = req
        .filter_eq
        .as_ref()
        .map(parse_filter_eq_map)
        .unwrap_or_default();
    let limit = req.limit.unwrap_or(10000).max(1);

    let mut cols_set = HashSet::<String>::new();
    for r in &req.rows {
        if let Some(o) = r.as_object() {
            for k in o.keys() {
                cols_set.insert(k.clone());
            }
        }
    }
    let mut columns = cols_set.into_iter().collect::<Vec<_>>();
    columns.sort();
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(c, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for c in &columns {
        let vals = req
            .rows
            .iter()
            .map(|r| r.as_object().and_then(|o| o.get(c)).map(value_to_string))
            .collect::<Vec<_>>();
        arrays.push(Arc::new(StringArray::from(vals)) as ArrayRef);
    }
    let batch = RecordBatch::try_new(schema, arrays).map_err(|e| format!("columnar batch: {e}"))?;

    let mut picked_idx = Vec::<u32>::new();
    for i in 0..batch.num_rows() {
        let mut ok = true;
        for (f, exp) in &filter_eq {
            if let Some(ci) = columns.iter().position(|c| c == f)
                && let Some(a) = batch.column(ci).as_any().downcast_ref::<StringArray>()
            {
                let cur = if a.is_null(i) {
                    "".to_string()
                } else {
                    a.value(i).to_string()
                };
                if cur != *exp {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            picked_idx.push(i as u32);
        }
        if picked_idx.len() >= limit {
            break;
        }
    }
    let idx = UInt32Array::from(picked_idx);
    let mut out_rows = Vec::new();
    for row_pos in 0..idx.len() {
        let src = idx.value(row_pos) as usize;
        let mut obj = Map::new();
        for (ci, c) in columns.iter().enumerate() {
            if !select_fields.is_empty() && !select_fields.iter().any(|x| x == c) {
                continue;
            }
            if let Some(a) = batch.column(ci).as_any().downcast_ref::<StringArray>() {
                if a.is_null(src) {
                    obj.insert(c.clone(), Value::Null);
                } else {
                    obj.insert(c.clone(), json!(a.value(src)));
                }
            }
        }
        out_rows.push(Value::Object(obj));
    }
    let duration = begin.elapsed().as_millis();
    let _ = run_runtime_stats_v1(RuntimeStatsV1Req {
        run_id: req.run_id.clone(),
        op: "record".to_string(),
        operator: Some("columnar_eval_v1".to_string()),
        ok: Some(true),
        error_code: None,
        duration_ms: Some(duration),
        rows_in: Some(rows_in),
        rows_out: Some(out_rows.len()),
    });
    Ok(json!({
        "ok": true,
        "operator": "columnar_eval_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out_rows,
        "stats": {"rows_in": rows_in, "rows_out": out_rows.len(), "duration_ms": duration}
    }))
}

fn parse_event_ts_ms(v: Option<&Value>) -> Option<i64> {
    let s = value_to_string_or_null(v);
    if s.is_empty() {
        return None;
    }
    if let Ok(n) = s.parse::<i64>() {
        return Some(n);
    }
    Some(parse_time_order_key(&s))
}

fn run_stream_window_v1(req: StreamWindowV1Req) -> Result<Value, String> {
    maybe_inject_fault("stream_window_v1")?;
    if req.window_ms == 0 {
        return Err("stream_window_v1 window_ms must be > 0".to_string());
    }
    let watermark_ms = req.watermark_ms.unwrap_or(req.window_ms);
    let group_by = req.group_by.unwrap_or_default();
    let trigger = req.trigger.unwrap_or_else(|| "on_watermark".to_string());
    let value_field = req.value_field.unwrap_or_else(|| "value".to_string());
    let now_ms = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()) as i64;
    let mut buckets = HashMap::<String, (u64, f64, u64, i64)>::new();
    let mut dropped_late = 0usize;
    for r in &req.rows {
        let Some(o) = r.as_object() else {
            continue;
        };
        let Some(ts) = parse_event_ts_ms(o.get(&req.event_time_field)) else {
            continue;
        };
        if now_ms.saturating_sub(ts) > watermark_ms as i64 {
            dropped_late += 1;
            continue;
        }
        let start = (ts / req.window_ms as i64) * req.window_ms as i64;
        let mut parts = vec![start.to_string()];
        for g in &group_by {
            parts.push(value_to_string_or_null(o.get(g)));
        }
        let key = parts.join("|");
        let e = buckets.entry(key).or_insert((0, 0.0, 0, start));
        e.0 += 1;
        if let Some(v) = o.get(&value_field).and_then(value_to_f64) {
            e.1 += v;
            e.2 += 1;
        }
    }
    let mut rows = Vec::new();
    for (k, (cnt, sum, sum_n, start)) in buckets {
        let mut obj = Map::new();
        obj.insert("window_start_ms".to_string(), json!(start));
        obj.insert(
            "window_end_ms".to_string(),
            json!(start + req.window_ms as i64),
        );
        obj.insert("count".to_string(), json!(cnt));
        obj.insert("sum".to_string(), json!(sum));
        obj.insert(
            "avg".to_string(),
            if sum_n == 0 {
                Value::Null
            } else {
                json!(sum / sum_n as f64)
            },
        );
        let parts = k.split('|').collect::<Vec<_>>();
        for (i, g) in group_by.iter().enumerate() {
            if let Some(v) = parts.get(i + 1) {
                obj.insert(g.clone(), json!(*v));
            }
        }
        rows.push(Value::Object(obj));
    }
    rows.sort_by(|a, b| {
        let av = a.get("window_start_ms").and_then(value_to_i64).unwrap_or(0);
        let bv = b.get("window_start_ms").and_then(value_to_i64).unwrap_or(0);
        av.cmp(&bv)
    });
    let _ = run_stream_state_v2(StreamStateV2Req {
        run_id: req.run_id.clone(),
        op: "checkpoint".to_string(),
        stream_key: format!("stream_window_v1:{}", req.stream_key),
        state: Some(json!({"rows_out": rows.len(), "trigger": trigger})),
        offset: Some(req.rows.len() as u64),
        checkpoint_version: None,
        expected_version: None,
        backend: None,
        db_path: None,
        event_ts_ms: None,
        max_late_ms: None,
    });
    Ok(json!({
        "ok": true,
        "operator": "stream_window_v1",
        "status": "done",
        "run_id": req.run_id,
        "trigger": trigger,
        "rows": rows,
        "stats": {"input_rows": req.rows.len(), "output_rows": rows.len(), "dropped_late": dropped_late, "window_ms": req.window_ms, "watermark_ms": watermark_ms}
    }))
}

fn run_stream_window_v2(req: StreamWindowV2Req) -> Result<Value, String> {
    maybe_inject_fault("stream_window_v2")?;
    if req.window_ms == 0 {
        return Err("stream_window_v2 window_ms must be > 0".to_string());
    }
    let window_type = req
        .window_type
        .unwrap_or_else(|| "tumbling".to_string())
        .to_lowercase();
    let slide_ms = req.slide_ms.unwrap_or(req.window_ms).max(1);
    let session_gap_ms = req.session_gap_ms.unwrap_or(req.window_ms).max(1);
    let allowed_lateness_ms = req.allowed_lateness_ms.unwrap_or(req.window_ms);
    let watermark_ms = req.watermark_ms.unwrap_or(allowed_lateness_ms);
    let group_by = req.group_by.unwrap_or_default();
    let value_field = req.value_field.unwrap_or_else(|| "value".to_string());
    let trigger = req.trigger.unwrap_or_else(|| "on_watermark".to_string());
    let now_ms = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()) as i64;

    let mut late_rows = Vec::<Value>::new();
    let mut ontime = Vec::<(i64, Map<String, Value>)>::new();
    for r in &req.rows {
        let Some(o) = r.as_object().cloned() else {
            continue;
        };
        let Some(ts) = parse_event_ts_ms(o.get(&req.event_time_field)) else {
            continue;
        };
        if now_ms.saturating_sub(ts) > watermark_ms as i64 {
            late_rows.push(Value::Object(o));
        } else {
            ontime.push((ts, o));
        }
    }

    let mut buckets = HashMap::<String, (u64, f64, u64, i64, i64)>::new();
    let key_of = |o: &Map<String, Value>| -> String {
        group_by
            .iter()
            .map(|g| value_to_string_or_null(o.get(g)))
            .collect::<Vec<_>>()
            .join("|")
    };

    if window_type == "session" {
        let mut grouped = HashMap::<String, Vec<(i64, Map<String, Value>)>>::new();
        for (ts, o) in ontime {
            grouped.entry(key_of(&o)).or_default().push((ts, o));
        }
        for (gk, mut items) in grouped {
            items.sort_by_key(|x| x.0);
            let mut start = 0i64;
            let mut end = 0i64;
            let mut cnt = 0u64;
            let mut sum = 0.0f64;
            let mut sum_n = 0u64;
            for (i, (ts, o)) in items.iter().enumerate() {
                if i == 0 {
                    start = *ts;
                    end = *ts;
                }
                if *ts - end > session_gap_ms as i64 {
                    let k = format!("{start}|{gk}");
                    buckets.insert(k, (cnt, sum, sum_n, start, end + 1));
                    start = *ts;
                    cnt = 0;
                    sum = 0.0;
                    sum_n = 0;
                }
                end = *ts;
                cnt += 1;
                if let Some(v) = o.get(&value_field).and_then(value_to_f64) {
                    sum += v;
                    sum_n += 1;
                }
            }
            if cnt > 0 {
                let k = format!("{start}|{gk}");
                buckets.insert(k, (cnt, sum, sum_n, start, end + 1));
            }
        }
    } else if window_type == "sliding" {
        let overlap = (req.window_ms / slide_ms).max(1);
        for (ts, o) in ontime {
            let gk = key_of(&o);
            let base = (ts / slide_ms as i64) * slide_ms as i64;
            for j in 0..=overlap {
                let start = base - (j as i64 * slide_ms as i64);
                if ts < start || ts >= start + req.window_ms as i64 {
                    continue;
                }
                let end = start + req.window_ms as i64;
                let k = format!("{start}|{gk}");
                let e = buckets.entry(k).or_insert((0, 0.0, 0, start, end));
                e.0 += 1;
                if let Some(v) = o.get(&value_field).and_then(value_to_f64) {
                    e.1 += v;
                    e.2 += 1;
                }
            }
        }
    } else {
        for (ts, o) in ontime {
            let gk = key_of(&o);
            let start = (ts / req.window_ms as i64) * req.window_ms as i64;
            let end = start + req.window_ms as i64;
            let k = format!("{start}|{gk}");
            let e = buckets.entry(k).or_insert((0, 0.0, 0, start, end));
            e.0 += 1;
            if let Some(v) = o.get(&value_field).and_then(value_to_f64) {
                e.1 += v;
                e.2 += 1;
            }
        }
    }

    let mut rows = Vec::new();
    for (k, (cnt, sum, sum_n, start, end)) in buckets {
        let mut obj = Map::new();
        obj.insert("window_start_ms".to_string(), json!(start));
        obj.insert("window_end_ms".to_string(), json!(end));
        obj.insert("count".to_string(), json!(cnt));
        obj.insert("sum".to_string(), json!(sum));
        obj.insert(
            "avg".to_string(),
            if sum_n == 0 {
                Value::Null
            } else {
                json!(sum / sum_n as f64)
            },
        );
        let parts = k.split('|').collect::<Vec<_>>();
        for (i, g) in group_by.iter().enumerate() {
            if let Some(v) = parts.get(i + 1) {
                obj.insert(g.clone(), json!(*v));
            }
        }
        rows.push(Value::Object(obj));
    }
    rows.sort_by(|a, b| {
        let av = a.get("window_start_ms").and_then(value_to_i64).unwrap_or(0);
        let bv = b.get("window_start_ms").and_then(value_to_i64).unwrap_or(0);
        av.cmp(&bv)
    });

    let _ = run_stream_state_v2(StreamStateV2Req {
        run_id: req.run_id.clone(),
        op: "checkpoint".to_string(),
        stream_key: format!("stream_window_v2:{}", req.stream_key),
        state: Some(json!({"window_type": window_type, "rows_out": rows.len()})),
        offset: Some(req.rows.len() as u64),
        checkpoint_version: None,
        expected_version: None,
        backend: None,
        db_path: None,
        event_ts_ms: None,
        max_late_ms: None,
    });

    Ok(json!({
        "ok": true,
        "operator": "stream_window_v2",
        "status": "done",
        "run_id": req.run_id,
        "window_type": window_type,
        "trigger": trigger,
        "rows": rows,
        "late_rows": if req.emit_late_side.unwrap_or(false) { Value::Array(late_rows.clone()) } else { Value::Array(vec![]) },
        "stats": {
            "input_rows": req.rows.len(),
            "output_rows": rows.len(),
            "late_rows": late_rows.len(),
            "window_ms": req.window_ms,
            "slide_ms": slide_ms,
            "session_gap_ms": session_gap_ms,
            "allowed_lateness_ms": allowed_lateness_ms
        }
    }))
}

fn run_sketch_v1(req: SketchV1Req) -> Result<Value, String> {
    maybe_inject_fault("sketch_v1")?;
    let op = req.op.trim().to_lowercase();
    let kind = req
        .kind
        .clone()
        .unwrap_or_else(|| "hll".to_string())
        .to_lowercase();
    let field = req.field.clone().unwrap_or_else(|| "value".to_string());
    let mut state = req.state.unwrap_or_else(|| json!({}));
    let state_obj = state
        .as_object_mut()
        .ok_or_else(|| "sketch_v1 state must be object".to_string())?;
    if op == "update" || op == "create" {
        let rows = req.rows.unwrap_or_default();
        if kind == "hll" {
            let mut set = state_obj
                .get("set")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<HashSet<_>>();
            for r in &rows {
                if let Some(o) = r.as_object() {
                    set.insert(value_to_string_or_null(o.get(&field)));
                }
            }
            state_obj.insert("kind".to_string(), json!("hll"));
            state_obj.insert(
                "set".to_string(),
                Value::Array(set.into_iter().map(Value::String).collect()),
            );
        } else if kind == "tdigest" {
            let mut vals = state_obj
                .get("values")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| value_to_f64(&v))
                .collect::<Vec<_>>();
            for r in &rows {
                if let Some(o) = r.as_object()
                    && let Some(v) = o.get(&field).and_then(value_to_f64)
                {
                    vals.push(v);
                }
            }
            state_obj.insert("kind".to_string(), json!("tdigest"));
            state_obj.insert(
                "values".to_string(),
                Value::Array(vals.into_iter().map(Value::from).collect()),
            );
        } else if kind == "topk" {
            let mut freq = state_obj
                .get("freq")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            for r in &rows {
                if let Some(o) = r.as_object() {
                    let k = value_to_string_or_null(o.get(&field));
                    let n = freq.get(&k).and_then(|v| v.as_u64()).unwrap_or(0) + 1;
                    freq.insert(k, json!(n));
                }
            }
            state_obj.insert("kind".to_string(), json!("topk"));
            state_obj.insert("freq".to_string(), Value::Object(freq));
            state_obj.insert(
                "topk_n".to_string(),
                json!(req.topk_n.unwrap_or(5).clamp(1, 100)),
            );
        } else {
            return Err(format!("sketch_v1 unsupported kind: {kind}"));
        }
    } else if op == "merge" {
        let rhs = req.merge_state.unwrap_or_else(|| json!({}));
        if kind == "hll" {
            let mut set = state_obj
                .get("set")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<HashSet<_>>();
            for x in rhs
                .get("set")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
            {
                if let Some(s) = x.as_str() {
                    set.insert(s.to_string());
                }
            }
            state_obj.insert(
                "set".to_string(),
                Value::Array(set.into_iter().map(Value::String).collect()),
            );
            state_obj.insert("kind".to_string(), json!("hll"));
        } else if kind == "tdigest" {
            let mut vals = state_obj
                .get("values")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            vals.extend(
                rhs.get("values")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default(),
            );
            state_obj.insert("values".to_string(), Value::Array(vals));
            state_obj.insert("kind".to_string(), json!("tdigest"));
        } else if kind == "topk" {
            let mut freq = state_obj
                .get("freq")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            for (k, v) in rhs
                .get("freq")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default()
            {
                let n =
                    freq.get(&k).and_then(|x| x.as_u64()).unwrap_or(0) + v.as_u64().unwrap_or(0);
                freq.insert(k, json!(n));
            }
            state_obj.insert("freq".to_string(), Value::Object(freq));
            state_obj.insert("kind".to_string(), json!("topk"));
        }
    }
    let estimate = if kind == "hll" {
        state_obj
            .get("set")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0) as f64
    } else if kind == "tdigest" {
        let vals = state_obj
            .get("values")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| value_to_f64(&v))
            .collect::<Vec<_>>();
        approx_percentile(vals, 0.5, 2000).unwrap_or(0.0)
    } else {
        let mut items = state_obj
            .get("freq")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.as_u64().unwrap_or(0)))
            .collect::<Vec<_>>();
        items.sort_by(|a, b| b.1.cmp(&a.1));
        let n = state_obj
            .get("topk_n")
            .and_then(|v| v.as_u64())
            .unwrap_or(5) as usize;
        state_obj.insert(
            "topk".to_string(),
            Value::Array(
                items
                    .into_iter()
                    .take(n)
                    .map(|(k, c)| json!({"value": k, "count": c}))
                    .collect(),
            ),
        );
        n as f64
    };
    Ok(
        json!({"ok": true, "operator":"sketch_v1", "status":"done", "run_id": req.run_id, "kind": kind, "state": state, "estimate": estimate}),
    )
}

fn run_runtime_stats_v1(req: RuntimeStatsV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let mut store = load_kv_store(&runtime_stats_store_path());
    if op == "record" {
        let operator = req.operator.unwrap_or_else(|| "unknown".to_string());
        let entry = store.entry(operator.clone()).or_insert(json!({
            "calls": 0u64, "ok": 0u64, "err": 0u64, "durations": [], "rows_in": 0u64, "rows_out": 0u64, "errors": {}
        }));
        let obj = entry
            .as_object_mut()
            .ok_or_else(|| "runtime_stats_v1 bad entry".to_string())?;
        let calls = obj.get("calls").and_then(|v| v.as_u64()).unwrap_or(0) + 1;
        let ok =
            obj.get("ok").and_then(|v| v.as_u64()).unwrap_or(0) + u64::from(req.ok.unwrap_or(true));
        let err = obj.get("err").and_then(|v| v.as_u64()).unwrap_or(0)
            + u64::from(!req.ok.unwrap_or(true));
        let rows_in = obj.get("rows_in").and_then(|v| v.as_u64()).unwrap_or(0)
            + req.rows_in.unwrap_or(0) as u64;
        let rows_out = obj.get("rows_out").and_then(|v| v.as_u64()).unwrap_or(0)
            + req.rows_out.unwrap_or(0) as u64;
        let mut durs = obj
            .get("durations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        if let Some(d) = req.duration_ms {
            durs.push(json!(d as u64));
            if durs.len() > 500 {
                durs = durs.split_off(durs.len() - 500);
            }
        }
        let mut errs = obj
            .get("errors")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        if let Some(ec) = req.error_code {
            let n = errs.get(&ec).and_then(|v| v.as_u64()).unwrap_or(0) + 1;
            errs.insert(ec, json!(n));
        }
        obj.insert("calls".to_string(), json!(calls));
        obj.insert("ok".to_string(), json!(ok));
        obj.insert("err".to_string(), json!(err));
        obj.insert("rows_in".to_string(), json!(rows_in));
        obj.insert("rows_out".to_string(), json!(rows_out));
        obj.insert("durations".to_string(), Value::Array(durs));
        obj.insert("errors".to_string(), Value::Object(errs));
        save_kv_store(&runtime_stats_store_path(), &store)?;
        return Ok(
            json!({"ok": true, "operator":"runtime_stats_v1", "status":"done", "run_id": req.run_id, "op":"record", "target": operator}),
        );
    }
    if op == "reset" {
        store.clear();
        save_kv_store(&runtime_stats_store_path(), &store)?;
        return Ok(
            json!({"ok": true, "operator":"runtime_stats_v1", "status":"done", "run_id": req.run_id, "op":"reset"}),
        );
    }
    let mut items = Vec::new();
    for (k, v) in store {
        let durs = v
            .get("durations")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|x| x.as_u64())
            .collect::<Vec<_>>();
        let mut s = durs.clone();
        s.sort();
        let p50 = if s.is_empty() { 0 } else { s[s.len() / 2] };
        let p95 = if s.is_empty() {
            0
        } else {
            s[((s.len() as f64 * 0.95).floor() as usize).min(s.len() - 1)]
        };
        items.push(json!({
            "operator": k,
            "calls": v.get("calls").and_then(|x| x.as_u64()).unwrap_or(0),
            "ok": v.get("ok").and_then(|x| x.as_u64()).unwrap_or(0),
            "err": v.get("err").and_then(|x| x.as_u64()).unwrap_or(0),
            "rows_in": v.get("rows_in").and_then(|x| x.as_u64()).unwrap_or(0),
            "rows_out": v.get("rows_out").and_then(|x| x.as_u64()).unwrap_or(0),
            "p50_ms": p50,
            "p95_ms": p95,
            "errors": v.get("errors").cloned().unwrap_or_else(|| json!({}))
        }));
    }
    items.sort_by(|a, b| {
        let av = a.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        let bv = b.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        av.cmp(bv)
    });
    Ok(
        json!({"ok": true, "operator":"runtime_stats_v1", "status":"done", "run_id": req.run_id, "op":"summary", "items": items}),
    )
}

fn run_capabilities_v1(req: CapabilitiesV1Req) -> Result<Value, String> {
    let mut ops = vec![
        json!({"operator":"transform_rows_v3","version":"v3","streaming":true,"cache":true,"checkpoint":true,"io_contract":true}),
        json!({"operator":"load_rows_v3","version":"v3","streaming":true,"cache":false,"checkpoint":true,"io_contract":true}),
        json!({"operator":"join_rows_v4","version":"v4","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"aggregate_rows_v4","version":"v4","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"quality_check_v4","version":"v4","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"stream_window_v2","version":"v2","streaming":true,"cache":false,"checkpoint":true,"io_contract":true}),
        json!({"operator":"stream_state_v2","version":"v2","streaming":true,"cache":false,"checkpoint":true,"io_contract":true}),
        json!({"operator":"columnar_eval_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"runtime_stats_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"explain_plan_v2","version":"v2","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"finance_ratio_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"anomaly_explain_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"plugin_operator_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"capabilities_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"io_contract_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"failure_policy_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"incremental_plan_v1","version":"v1","streaming":false,"cache":true,"checkpoint":true,"io_contract":true}),
        json!({"operator":"tenant_isolation_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"operator_policy_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"optimizer_adaptive_v2","version":"v2","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"vector_index_v2_build","version":"v2","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"vector_index_v2_search","version":"v2","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"stream_reliability_v1","version":"v1","streaming":true,"cache":false,"checkpoint":true,"io_contract":true}),
        json!({"operator":"lineage_provenance_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"contract_regression_v1","version":"v1","streaming":false,"cache":false,"checkpoint":false,"io_contract":true}),
        json!({"operator":"perf_baseline_v1","version":"v1","streaming":false,"cache":true,"checkpoint":false,"io_contract":true}),
    ];
    if let Some(allow) = req.include_ops {
        let set: HashSet<String> = allow
            .into_iter()
            .map(|x| x.trim().to_lowercase())
            .filter(|x| !x.is_empty())
            .collect();
        if !set.is_empty() {
            ops.retain(|x| {
                x.get("operator")
                    .and_then(|v| v.as_str())
                    .map(|s| set.contains(&s.to_lowercase()))
                    .unwrap_or(false)
            });
        }
    }
    ops.sort_by(|a, b| {
        let av = a.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        let bv = b.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        av.cmp(bv)
    });
    Ok(json!({
        "ok": true,
        "operator": "capabilities_v1",
        "status": "done",
        "run_id": req.run_id,
        "schema_version": "aiwf.capabilities.v1",
        "items": ops
    }))
}

fn io_contract_errors(operator: &str, input: &Value, strict: bool) -> Vec<String> {
    let mut errs = Vec::new();
    let op = operator.trim().to_lowercase();
    let obj = if let Some(o) = input.as_object() {
        o
    } else {
        errs.push("input must be object".to_string());
        return errs;
    };
    let require_rows_or_uri = |errs: &mut Vec<String>, o: &Map<String, Value>| {
        let has_rows = o
            .get("rows")
            .and_then(|v| v.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        let has_uri = o
            .get("input_uri")
            .and_then(|v| v.as_str())
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        if !has_rows && !has_uri {
            errs.push("requires rows[] or input_uri".to_string());
        }
    };
    match op.as_str() {
        "transform_rows_v2" | "transform_rows_v3" | "load_rows_v3" => {
            require_rows_or_uri(&mut errs, obj)
        }
        "finance_ratio_v1" => {
            if !obj.get("rows").and_then(|v| v.as_array()).is_some() {
                errs.push("finance_ratio_v1 requires rows[]".to_string());
            }
        }
        "anomaly_explain_v1" => {
            if !obj.get("rows").and_then(|v| v.as_array()).is_some() {
                errs.push("anomaly_explain_v1 requires rows[]".to_string());
            }
            if obj
                .get("score_field")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
            {
                errs.push("anomaly_explain_v1 requires score_field".to_string());
            }
        }
        "stream_window_v2" => {
            if obj
                .get("stream_key")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
            {
                errs.push("stream_window_v2 requires stream_key".to_string());
            }
            if obj
                .get("event_time_field")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
            {
                errs.push("stream_window_v2 requires event_time_field".to_string());
            }
        }
        "plugin_operator_v1" => {
            if obj
                .get("plugin")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
            {
                errs.push("plugin_operator_v1 requires plugin".to_string());
            }
        }
        _ => {
            if strict {
                errs.push(format!("unsupported contract operator: {operator}"));
            }
        }
    }
    errs
}

fn run_io_contract_v1(req: IoContractV1Req) -> Result<Value, String> {
    let strict = req.strict.unwrap_or(false);
    let errors = io_contract_errors(&req.operator, &req.input, strict);
    Ok(json!({
        "ok": true,
        "operator": "io_contract_v1",
        "status": if errors.is_empty() { "done" } else { "invalid" },
        "run_id": req.run_id,
        "target_operator": req.operator,
        "strict": strict,
        "valid": errors.is_empty(),
        "errors": errors
    }))
}

fn classify_failure(error: &str, status_code: Option<u16>) -> (String, bool, String) {
    let e = error.trim().to_lowercase();
    if e.contains("timeout") || e.contains("timed out") {
        return (
            "transient_timeout".to_string(),
            true,
            "retry_with_backoff".to_string(),
        );
    }
    if e.contains("rust_http_5") || status_code.map(|s| s >= 500).unwrap_or(false) {
        return (
            "upstream_5xx".to_string(),
            true,
            "switch_upstream_or_retry".to_string(),
        );
    }
    if e.contains("quota") || e.contains("tenant") {
        return (
            "quota_reject".to_string(),
            false,
            "queue_and_throttle".to_string(),
        );
    }
    if e.contains("sandbox_limit_exceeded") {
        return (
            "sandbox_limit".to_string(),
            true,
            "reduce_payload_or_raise_limit".to_string(),
        );
    }
    if e.contains("schema") || e.contains("invalid") || e.contains("missing") {
        return (
            "input_invalid".to_string(),
            false,
            "fix_input_contract".to_string(),
        );
    }
    if e.contains("egress_blocked") || e.contains("not_allowed") {
        return (
            "policy_blocked".to_string(),
            false,
            "adjust_policy_or_localize".to_string(),
        );
    }
    ("unknown".to_string(), true, "manual_review".to_string())
}

fn run_failure_policy_v1(req: FailurePolicyV1Req) -> Result<Value, String> {
    if req.error.trim().is_empty() {
        return Err("failure_policy_v1 requires non-empty error".to_string());
    }
    let (class, retryable0, action) = classify_failure(&req.error, req.status_code);
    let attempts = req.attempts.unwrap_or(0);
    let max_retries = req.max_retries.unwrap_or(2);
    let retryable = retryable0 && attempts < max_retries;
    Ok(json!({
        "ok": true,
        "operator": "failure_policy_v1",
        "status": "done",
        "run_id": req.run_id,
        "target_operator": req.operator.unwrap_or_default(),
        "class": class,
        "retryable": retryable,
        "attempts": attempts,
        "max_retries": max_retries,
        "recovery_action": action
    }))
}

fn canonicalize_value(v: &Value) -> Value {
    match v {
        Value::Array(a) => Value::Array(a.iter().map(canonicalize_value).collect()),
        Value::Object(o) => {
            let mut bm: BTreeMap<String, Value> = BTreeMap::new();
            for (k, v) in o {
                bm.insert(k.clone(), canonicalize_value(v));
            }
            let mut out = Map::new();
            for (k, v) in bm {
                out.insert(k, v);
            }
            Value::Object(out)
        }
        _ => v.clone(),
    }
}

fn incremental_fingerprint(operator: &str, input: &Value) -> String {
    let c = canonicalize_value(input);
    let s = serde_json::to_vec(&json!({"operator": operator, "input": c})).unwrap_or_default();
    let mut h = Sha256::new();
    h.update(&s);
    format!("{:x}", h.finalize())
}

fn run_incremental_plan_v1(state: &AppState, req: IncrementalPlanV1Req) -> Result<Value, String> {
    let operator = req.operator.trim().to_lowercase();
    if operator.is_empty() {
        return Err("incremental_plan_v1 requires operator".to_string());
    }
    let fingerprint = incremental_fingerprint(&operator, &req.input);
    let mut cache_hit = false;
    let mut cache_key = String::new();
    if operator == "transform_rows_v2" {
        if let Ok(parsed) = serde_json::from_value::<TransformRowsReq>(req.input.clone()) {
            cache_key = transform_cache_key(&parsed);
            if let Ok(guard) = state.transform_cache.lock() {
                cache_hit = guard.contains_key(&cache_key);
            }
        }
    }
    let resume_checkpoint = req
        .checkpoint_key
        .as_deref()
        .map(read_stream_checkpoint)
        .transpose()?
        .flatten();
    Ok(json!({
        "ok": true,
        "operator": "incremental_plan_v1",
        "status": "done",
        "run_id": req.run_id,
        "target_operator": operator,
        "fingerprint": fingerprint,
        "cache_key": cache_key,
        "cache_hit": cache_hit,
        "resume_checkpoint": resume_checkpoint,
        "strategy": if cache_hit { "cache_reuse" } else if resume_checkpoint.is_some() { "resume_from_checkpoint" } else { "full_recompute" }
    }))
}

fn run_explain_plan_v2(req: ExplainPlanV2Req) -> Result<Value, String> {
    let v1 = run_explain_plan_v1(ExplainPlanV1Req {
        run_id: req.run_id.clone(),
        steps: req.steps.clone(),
        rows: req.rows.clone(),
        actual_stats: req.actual_stats.clone(),
        persist_feedback: req.persist_feedback,
    })?;
    let mut out = v1.as_object().cloned().unwrap_or_default();
    let mut actual_total_ms = 0.0f64;
    let mut stage = Vec::new();
    if let Some(actuals) = req.actual_stats {
        for a in actuals {
            let Some(o) = a.as_object() else { continue };
            let op = o
                .get("operator")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let est = o.get("estimated_ms").and_then(value_to_f64).unwrap_or(0.0);
            let act = o.get("actual_ms").and_then(value_to_f64).unwrap_or(0.0);
            actual_total_ms += act.max(0.0);
            stage.push(json!({
                "operator": op,
                "estimated_ms": est,
                "actual_ms": act,
                "ratio": if est > 0.0 { act / est } else { 0.0 }
            }));
        }
    }
    out.insert("operator".to_string(), json!("explain_plan_v2"));
    out.insert(
        "actual_total_ms".to_string(),
        json!((actual_total_ms * 100.0).round() / 100.0),
    );
    out.insert("stage_stats".to_string(), Value::Array(stage));
    if req.include_runtime_stats.unwrap_or(false) {
        let rs = run_runtime_stats_v1(RuntimeStatsV1Req {
            run_id: req.run_id,
            op: "summary".to_string(),
            operator: None,
            ok: None,
            error_code: None,
            duration_ms: None,
            rows_in: None,
            rows_out: None,
        })?;
        out.insert("runtime_stats".to_string(), rs);
    }
    Ok(Value::Object(out))
}

fn run_tenant_isolation_v1(req: TenantIsolationV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let tenant = req
        .tenant_id
        .clone()
        .unwrap_or_else(|| "default".to_string())
        .trim()
        .to_lowercase();
    if tenant.is_empty() {
        return Err("tenant_isolation_v1 requires tenant_id".to_string());
    }
    let mut store = load_kv_store(&tenant_isolation_store_path());
    if op == "get" {
        return Ok(json!({
            "ok": true,
            "operator": "tenant_isolation_v1",
            "status": "done",
            "run_id": req.run_id,
            "tenant_id": tenant,
            "policy": store.get(&tenant).cloned().unwrap_or_else(|| json!({}))
        }));
    }
    if op == "reset" || op == "delete" {
        let deleted = store.remove(&tenant).is_some();
        save_kv_store(&tenant_isolation_store_path(), &store)?;
        return Ok(
            json!({"ok": true, "operator":"tenant_isolation_v1","status":"done","run_id": req.run_id, "tenant_id":tenant,"deleted":deleted}),
        );
    }
    if op != "set" {
        return Err(format!("tenant_isolation_v1 unsupported op: {op}"));
    }
    let mut policy = store
        .get(&tenant)
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    if let Some(v) = req.max_concurrency {
        policy.insert("max_concurrency".to_string(), json!(v.max(1)));
    }
    if let Some(v) = req.max_rows {
        policy.insert("max_rows".to_string(), json!(v.max(1)));
    }
    if let Some(v) = req.max_payload_bytes {
        policy.insert("max_payload_bytes".to_string(), json!(v.max(1024)));
    }
    if let Some(v) = req.max_workflow_steps {
        policy.insert("max_workflow_steps".to_string(), json!(v.max(1)));
    }
    store.insert(tenant.clone(), Value::Object(policy.clone()));
    save_kv_store(&tenant_isolation_store_path(), &store)?;
    Ok(
        json!({"ok": true, "operator":"tenant_isolation_v1","status":"done","run_id": req.run_id, "tenant_id":tenant,"policy": policy}),
    )
}

fn run_operator_policy_v1(req: OperatorPolicyV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let tenant = req
        .tenant_id
        .clone()
        .unwrap_or_else(|| "default".to_string())
        .trim()
        .to_lowercase();
    if tenant.is_empty() {
        return Err("operator_policy_v1 requires tenant_id".to_string());
    }
    let mut store = load_kv_store(&operator_policy_store_path());
    if op == "get" {
        return Ok(
            json!({"ok": true,"operator":"operator_policy_v1","status":"done","run_id": req.run_id,"tenant_id":tenant,"policy": store.get(&tenant).cloned().unwrap_or_else(|| json!({}))}),
        );
    }
    if op == "reset" || op == "delete" {
        let deleted = store.remove(&tenant).is_some();
        save_kv_store(&operator_policy_store_path(), &store)?;
        return Ok(
            json!({"ok": true,"operator":"operator_policy_v1","status":"done","run_id": req.run_id,"tenant_id":tenant,"deleted": deleted}),
        );
    }
    if op != "set" {
        return Err(format!("operator_policy_v1 unsupported op: {op}"));
    }
    let mut m = Map::new();
    let allow = req
        .allow
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.trim().to_lowercase())
        .filter(|x| !x.is_empty())
        .map(Value::String)
        .collect::<Vec<_>>();
    let deny = req
        .deny
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.trim().to_lowercase())
        .filter(|x| !x.is_empty())
        .map(Value::String)
        .collect::<Vec<_>>();
    m.insert("allow".to_string(), Value::Array(allow));
    m.insert("deny".to_string(), Value::Array(deny));
    store.insert(tenant.clone(), Value::Object(m.clone()));
    save_kv_store(&operator_policy_store_path(), &store)?;
    Ok(
        json!({"ok": true,"operator":"operator_policy_v1","status":"done","run_id": req.run_id,"tenant_id":tenant,"policy": m}),
    )
}

fn run_optimizer_adaptive_v2(req: OptimizerAdaptiveV2Req) -> Result<Value, String> {
    let operator = req
        .operator
        .clone()
        .unwrap_or_else(|| "transform_rows_v3".to_string());
    let row_hint = req.row_count_hint.unwrap_or(0);
    let mut engine = if row_hint >= 120_000 {
        "columnar_arrow_v1"
    } else if row_hint >= 20_000 {
        "columnar_v1"
    } else {
        "row_v1"
    }
    .to_string();
    let stats = run_runtime_stats_v1(RuntimeStatsV1Req {
        run_id: req.run_id.clone(),
        op: "summary".to_string(),
        operator: None,
        ok: None,
        error_code: None,
        duration_ms: None,
        rows_in: None,
        rows_out: None,
    })?;
    let mut evidence = Vec::new();
    if let Some(items) = stats.get("items").and_then(|v| v.as_array()) {
        for it in items {
            if it.get("operator").and_then(|v| v.as_str()) == Some(&operator) {
                let p95 = it.get("p95_ms").and_then(|v| v.as_u64()).unwrap_or(0);
                if p95 > 1500 {
                    engine = "columnar_arrow_v1".to_string();
                } else if p95 > 500 {
                    engine = "columnar_v1".to_string();
                }
                evidence.push(it.clone());
            }
        }
    }
    if req.prefer_arrow.unwrap_or(false) {
        engine = "columnar_arrow_v1".to_string();
    }
    Ok(json!({
        "ok": true,
        "operator": "optimizer_adaptive_v2",
        "status": "done",
        "run_id": req.run_id,
        "target_operator": operator,
        "recommended_engine": engine,
        "row_count_hint": row_hint,
        "evidence": evidence
    }))
}

fn run_vector_index_build_v2(req: VectorIndexBuildV2Req) -> Result<Value, String> {
    let shard = req.shard.unwrap_or_else(|| "default".to_string());
    let mut docs = req
        .rows
        .iter()
        .filter_map(|r| r.as_object())
        .map(|o| {
            let id = value_to_string_or_null(o.get(&req.id_field));
            let text = value_to_string_or_null(o.get(&req.text_field));
            let mut meta = Map::new();
            for f in req.metadata_fields.clone().unwrap_or_default() {
                if let Some(v) = o.get(&f) {
                    meta.insert(f, v.clone());
                }
            }
            json!({"id": id, "text": text, "meta": meta})
        })
        .collect::<Vec<_>>();
    let mut store = load_kv_store(&vector_index_v2_store_path());
    if req.replace.unwrap_or(false) {
        store.insert(shard.clone(), Value::Array(docs.clone()));
    } else {
        let mut arr = store
            .get(&shard)
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        arr.append(&mut docs);
        store.insert(shard.clone(), Value::Array(arr));
    }
    save_kv_store(&vector_index_v2_store_path(), &store)?;
    let size = store
        .get(&shard)
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    Ok(
        json!({"ok": true,"operator":"vector_index_v2_build","status":"done","run_id":req.run_id,"shard":shard,"size":size}),
    )
}

fn run_vector_index_search_v2(req: VectorIndexSearchV2Req) -> Result<Value, String> {
    let q = req.query.to_lowercase();
    let top_k = req.top_k.unwrap_or(5).max(1);
    let store = load_kv_store(&vector_index_v2_store_path());
    let mut docs = Vec::new();
    if let Some(shard) = req.shard.as_ref() {
        docs.extend(
            store
                .get(shard)
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
        );
    } else {
        for v in store.values() {
            docs.extend(v.as_array().cloned().unwrap_or_default());
        }
    }
    let filter = req.filter_eq.and_then(|v| v.as_object().cloned());
    let mut scored = Vec::<(f64, Value)>::new();
    for d in docs {
        let Some(o) = d.as_object() else { continue };
        if let Some(fm) = filter.as_ref() {
            let meta = o
                .get("meta")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            let mut pass = true;
            for (k, v) in fm {
                if value_to_string_or_null(meta.get(k)) != value_to_string(v) {
                    pass = false;
                    break;
                }
            }
            if !pass {
                continue;
            }
        }
        let text = o
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        let overlap = q.chars().filter(|c| text.contains(*c)).count() as f64;
        let mut score = overlap / (q.len().max(1) as f64);
        if let Some(field) = req.rerank_meta_field.as_ref() {
            let w = req.rerank_meta_weight.unwrap_or(0.0);
            if w.abs() > 0.000001 {
                let meta = o
                    .get("meta")
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();
                let mv = meta.get(field).and_then(value_to_f64).unwrap_or(0.0);
                score += mv * w;
            }
        }
        scored.push((score, Value::Object(o.clone())));
    }
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let hits = scored
        .into_iter()
        .take(top_k)
        .map(|(s, v)| json!({"score": s, "doc": v}))
        .collect::<Vec<_>>();
    Ok(
        json!({"ok": true, "operator":"vector_index_v2_search","status":"done","run_id":req.run_id,"hits":hits}),
    )
}

fn run_vector_index_eval_v2(req: VectorIndexEvalV2Req) -> Result<Value, String> {
    if req.cases.is_empty() {
        return Err("vector_index_v2_eval requires cases".to_string());
    }
    let k = req.top_k.unwrap_or(5).max(1);
    let mut hit = 0usize;
    let mut mrr = 0.0f64;
    let mut total = 0usize;
    let mut details = Vec::new();
    for c in req.cases {
        let Some(o) = c.as_object() else { continue };
        let q = o.get("query").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if q.trim().is_empty() {
            continue;
        }
        let expected = o
            .get("expected_ids")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<HashSet<String>>();
        let out = run_vector_index_search_v2(VectorIndexSearchV2Req {
            run_id: req.run_id.clone(),
            query: q.clone(),
            top_k: Some(k),
            shard: req.shard.clone(),
            filter_eq: None,
            rerank_meta_field: None,
            rerank_meta_weight: None,
        })?;
        let hits_arr = out
            .get("hits")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut found_rank = 0usize;
        for (idx, h) in hits_arr.iter().enumerate() {
            let id = h
                .get("doc")
                .and_then(|v| v.get("id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if expected.contains(id) {
                found_rank = idx + 1;
                break;
            }
        }
        total += 1;
        if found_rank > 0 {
            hit += 1;
            mrr += 1.0 / (found_rank as f64);
        }
        details.push(json!({"query": q, "found_rank": found_rank, "hit": found_rank > 0}));
    }
    let recall = if total > 0 { hit as f64 / total as f64 } else { 0.0 };
    let mrr_score = if total > 0 { mrr / total as f64 } else { 0.0 };
    Ok(json!({
        "ok": true,
        "operator": "vector_index_v2_eval",
        "status": "done",
        "run_id": req.run_id,
        "top_k": k,
        "cases": total,
        "recall_at_k": recall,
        "mrr": mrr_score,
        "details": details
    }))
}

fn run_stream_reliability_v1(req: StreamReliabilityV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let mut store = load_kv_store(&stream_reliability_store_path());
    let key = req.stream_key;
    let mut root = store
        .remove(&key)
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    let mut dedup = root
        .get("dedup")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|x| x.as_str().map(|s| s.to_string()))
        .collect::<HashSet<String>>();
    let mut dlq = root
        .get("dlq")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let checkpoint = root.get("checkpoint").and_then(|v| v.as_u64()).unwrap_or(0);
    let out = if op == "record" {
        let msg = req.msg_id.unwrap_or_else(|| short_trace(&utc_now_iso()));
        if dedup.contains(&msg) {
            json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"duplicate":true,"msg_id":msg})
        } else {
            dedup.insert(msg.clone());
            if let Some(err) = req.error {
                dlq.push(json!({"msg_id": msg, "error": err, "row": req.row, "ts": utc_now_iso()}));
            }
            json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"duplicate":false,"msg_id":msg})
        }
    } else if op == "checkpoint" {
        let cp = req.checkpoint.unwrap_or(checkpoint);
        root.insert("checkpoint".to_string(), json!(cp));
        json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"checkpoint":cp})
    } else if op == "flush_dlq" {
        let n = dlq.len();
        dlq.clear();
        json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"flushed":n})
    } else if op == "replay" {
        let limit = req.checkpoint.unwrap_or(100) as usize;
        let items = dlq.iter().take(limit).cloned().collect::<Vec<_>>();
        json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"replay_items":items,"replay_count":items.len()})
    } else if op == "consistency_check" {
        let dedup_unique = dedup.len();
        let dlq_count = dlq.len();
        let consistent = dedup_unique >= dlq_count;
        json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"consistent":consistent,"dedup_size":dedup_unique,"dlq_size":dlq_count})
    } else if op == "stats" {
        json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"dedup_size":dedup.len(),"dlq_size":dlq.len(),"checkpoint":checkpoint})
    } else {
        return Err(format!("stream_reliability_v1 unsupported op: {op}"));
    };
    root.insert(
        "dedup".to_string(),
        Value::Array(dedup.into_iter().map(Value::String).collect()),
    );
    root.insert("dlq".to_string(), Value::Array(dlq));
    store.insert(key.clone(), Value::Object(root));
    save_kv_store(&stream_reliability_store_path(), &store)?;
    Ok(out)
}

fn run_lineage_provenance_v1(req: LineageProvenanceV1Req) -> Result<Value, String> {
    let lineage = run_lineage_v3(LineageV3Req {
        run_id: req.run_id.clone(),
        rules: req.rules,
        computed_fields_v3: req.computed_fields_v3,
        workflow_steps: req.workflow_steps,
        rows: req.rows,
    })?;
    let prov = run_provenance_sign_v1(ProvenanceSignReq {
        run_id: req.run_id.clone(),
        payload: req.payload.unwrap_or_else(|| lineage.clone()),
        prev_hash: req.prev_hash,
    })?;
    Ok(json!({
        "ok": true,
        "operator": "lineage_provenance_v1",
        "status": "done",
        "run_id": req.run_id,
        "lineage": lineage,
        "provenance": prov
    }))
}

fn run_contract_regression_v1(req: ContractRegressionV1Req) -> Result<Value, String> {
    let operators = req.operators.unwrap_or_else(|| {
        vec![
            "transform_rows_v3".to_string(),
            "finance_ratio_v1".to_string(),
            "anomaly_explain_v1".to_string(),
            "stream_window_v2".to_string(),
            "plugin_operator_v1".to_string(),
        ]
    });
    let mut cases = Vec::new();
    for op in operators {
        let sample = match op.as_str() {
            "finance_ratio_v1" => json!({"rows":[{"assets":100.0,"liabilities":50.0}]}),
            "anomaly_explain_v1" => {
                json!({"rows":[{"score":0.9}], "score_field":"score","threshold":0.8})
            }
            "stream_window_v2" => {
                json!({"stream_key":"s1","rows":[{"ts":"2025-01-01","value":1}],"event_time_field":"ts","window_ms":60000})
            }
            "plugin_operator_v1" => json!({"plugin":"demo","op":"run","payload":{}}),
            _ => json!({"rows":[{"id":"1","amount":"10"}]}),
        };
        let valid = io_contract_errors(&op, &sample, false).is_empty();
        cases.push(json!({"operator":op,"sample_input":sample,"expect_valid":valid}));
    }
    Ok(
        json!({"ok": true, "operator":"contract_regression_v1","status":"done","run_id":req.run_id,"cases":cases}),
    )
}

fn run_perf_baseline_v1(req: PerfBaselineV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let name = req
        .operator
        .unwrap_or_else(|| "transform_rows_v3".to_string());
    let mut store = load_kv_store(&perf_baseline_store_path());
    if op == "set" {
        let p95 = req.p95_ms.unwrap_or(500);
        store.insert(
            name.clone(),
            json!({"p95_ms": p95, "updated_at": utc_now_iso()}),
        );
        save_kv_store(&perf_baseline_store_path(), &store)?;
        return Ok(
            json!({"ok": true, "operator":"perf_baseline_v1","status":"done","run_id":req.run_id,"target_operator":name,"baseline_p95_ms":p95}),
        );
    }
    if op == "check" {
        let baseline = store
            .get(&name)
            .and_then(|v| v.get("p95_ms"))
            .and_then(|v| v.as_u64())
            .unwrap_or(500) as u128;
        let current = req.max_p95_ms.unwrap_or(baseline);
        let passed = current <= baseline;
        return Ok(
            json!({"ok": true, "operator":"perf_baseline_v1","status":"done","run_id":req.run_id,"target_operator":name,"baseline_p95_ms":baseline,"current_p95_ms":current,"passed":passed}),
        );
    }
    if op == "get" {
        return Ok(
            json!({"ok": true, "operator":"perf_baseline_v1","status":"done","run_id":req.run_id,"items":store}),
        );
    }
    Err(format!("perf_baseline_v1 unsupported op: {op}"))
}

fn run_window_rows_v1(req: WindowRowsV1Req) -> Result<Value, String> {
    if req.functions.is_empty() {
        return Err("window_rows_v1 requires at least one function".to_string());
    }
    let partition_fields = req.partition_by.unwrap_or_default();
    let mut groups: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for r in req.rows {
        let Some(obj) = r.as_object().cloned() else {
            continue;
        };
        let key = if partition_fields.is_empty() {
            "__all__".to_string()
        } else {
            partition_fields
                .iter()
                .map(|f| value_to_string_or_null(obj.get(f)))
                .collect::<Vec<_>>()
                .join("|")
        };
        groups.entry(key).or_default().push(obj);
    }
    let mut out = Vec::new();
    for (_pk, mut rows) in groups {
        rows.sort_by(|a, b| {
            let av = value_to_string_or_null(a.get(&req.order_by));
            let bv = value_to_string_or_null(b.get(&req.order_by));
            parse_time_order_key(&av).cmp(&parse_time_order_key(&bv))
        });
        let order_vals = rows
            .iter()
            .map(|r| value_to_string_or_null(r.get(&req.order_by)))
            .collect::<Vec<_>>();
        let mut dense_rank = 0usize;
        let mut last_val = String::new();
        let mut last_rank = 0usize;
        for i in 0..rows.len() {
            let mut row = rows[i].clone();
            let ov = &order_vals[i];
            if i == 0 || *ov != last_val {
                dense_rank += 1;
                last_rank = i + 1;
                last_val = ov.clone();
            }
            for f in &req.functions {
                let Some(cfg) = f.as_object() else {
                    continue;
                };
                let op = cfg
                    .get("op")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_lowercase();
                let as_name = cfg
                    .get("as")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if as_name.is_empty() {
                    continue;
                }
                let val = match op.as_str() {
                    "row_number" => json!(i + 1),
                    "rank" => json!(last_rank),
                    "dense_rank" => json!(dense_rank),
                    "lag" => {
                        let field = cfg.get("field").and_then(|v| v.as_str()).unwrap_or("");
                        let off = cfg.get("offset").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
                        if i >= off {
                            rows[i - off].get(field).cloned().unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    }
                    "lead" => {
                        let field = cfg.get("field").and_then(|v| v.as_str()).unwrap_or("");
                        let off = cfg.get("offset").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
                        if i + off < rows.len() {
                            rows[i + off].get(field).cloned().unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    }
                    "moving_avg" => {
                        let field = cfg.get("field").and_then(|v| v.as_str()).unwrap_or("");
                        let window =
                            cfg.get("window").and_then(|v| v.as_u64()).unwrap_or(3) as usize;
                        let w = window.max(1);
                        let start = i.saturating_sub(w - 1);
                        let vals = rows[start..=i]
                            .iter()
                            .filter_map(|x| x.get(field).and_then(value_to_f64))
                            .collect::<Vec<_>>();
                        if vals.is_empty() {
                            Value::Null
                        } else {
                            json!(vals.iter().sum::<f64>() / vals.len() as f64)
                        }
                    }
                    _ => Value::Null,
                };
                row.insert(as_name, val);
            }
            out.push(Value::Object(row));
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "window_rows_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out
    }))
}

fn run_optimizer_v1(req: OptimizerV1Req) -> Result<Value, String> {
    let row_count = req
        .row_count_hint
        .unwrap_or_else(|| req.rows.as_ref().map(|r| r.len()).unwrap_or(0));
    let avg_cols = req
        .rows
        .as_ref()
        .map(|rows| {
            if rows.is_empty() {
                0.0
            } else {
                rows.iter()
                    .filter_map(|r| r.as_object().map(|o| o.len() as f64))
                    .sum::<f64>()
                    / rows.len() as f64
            }
        })
        .unwrap_or(0.0);
    let mut engine = if row_count >= 120_000 && avg_cols >= 4.0 {
        "columnar_arrow_v1"
    } else if row_count >= 40_000 {
        "columnar_v1"
    } else {
        "row_v1"
    };
    if req.prefer_arrow.unwrap_or(false) && row_count >= 20_000 {
        engine = "columnar_arrow_v1";
    }
    let join_strategy = if row_count >= 100_000 {
        "sort_merge"
    } else if row_count >= 20_000 {
        "hash"
    } else {
        "auto"
    };
    let aggregate_mode = if row_count >= 60_000 {
        "approx_hybrid"
    } else {
        "exact"
    };
    Ok(json!({
        "ok": true,
        "operator": "optimizer_v1",
        "status": "done",
        "run_id": req.run_id,
        "plan": {
            "execution_engine": engine,
            "join_strategy": join_strategy,
            "aggregate_mode": aggregate_mode,
            "row_count": row_count,
            "avg_columns": avg_cols
        },
        "hints": {
            "join": req.join_hint,
            "aggregate": req.aggregate_hint
        }
    }))
}

fn run_parquet_io_v2(req: ParquetIoV2Req) -> Result<Value, String> {
    fn collect_parquet_paths(
        base: &Path,
        recursive: bool,
        out: &mut Vec<PathBuf>,
    ) -> Result<(), String> {
        let rd = fs::read_dir(base).map_err(|e| format!("read parquet dir: {e}"))?;
        for ent in rd {
            let ent = ent.map_err(|e| format!("read parquet dir entry: {e}"))?;
            let p = ent.path();
            if p.is_dir() && recursive {
                collect_parquet_paths(&p, true, out)?;
            } else if p
                .extension()
                .and_then(|x| x.to_str())
                .map(|x| x.eq_ignore_ascii_case("parquet"))
                .unwrap_or(false)
            {
                out.push(p);
            }
        }
        Ok(())
    }
    fn partition_predicate_match(path: &Path, field: Option<&str>, eqv: Option<&Value>) -> bool {
        let Some(field) = field else { return true };
        let Some(eqv) = eqv else { return true };
        let target = value_to_string(eqv);
        for seg in path.components() {
            let s = seg.as_os_str().to_string_lossy();
            if let Some((k, v)) = s.split_once('=')
                && k == field
            {
                return v == target;
            }
        }
        true
    }
    fn schema_file_path(base_path: &str, partitioned: bool) -> PathBuf {
        let p = Path::new(base_path);
        if partitioned || p.is_dir() || !base_path.to_lowercase().ends_with(".parquet") {
            return p.join("_schema.json");
        }
        PathBuf::from(format!("{base_path}.schema.json"))
    }
    fn json_type_name(v: &Value) -> String {
        if v.is_boolean() {
            "bool".to_string()
        } else if v.is_i64() || v.is_u64() {
            "int".to_string()
        } else if v.is_f64() {
            "float".to_string()
        } else if v.is_null() {
            "null".to_string()
        } else {
            "string".to_string()
        }
    }
    fn infer_rows_schema(rows: &[Value]) -> Map<String, Value> {
        let mut out = Map::new();
        for r in rows {
            let Some(o) = r.as_object() else {
                continue;
            };
            for (k, v) in o {
                let t = json_type_name(v);
                let prev = out.get(k).and_then(|x| x.as_str()).unwrap_or("");
                let merged = if prev.is_empty() || prev == t {
                    t
                } else if (prev == "int" && t == "float") || (prev == "float" && t == "int") {
                    "float".to_string()
                } else {
                    "string".to_string()
                };
                out.insert(k.clone(), json!(merged));
            }
        }
        out
    }
    fn schema_compatible(
        old_s: &Map<String, Value>,
        new_s: &Map<String, Value>,
        mode: &str,
    ) -> bool {
        let m = mode.trim().to_lowercase();
        if m == "strict" {
            if old_s.len() != new_s.len() {
                return false;
            }
        }
        for (k, ov) in old_s {
            let Some(nv) = new_s.get(k) else {
                return false;
            };
            let o = ov.as_str().unwrap_or("");
            let n = nv.as_str().unwrap_or("");
            if o == n {
                continue;
            }
            if m == "widen" && ((o == "int" && n == "float") || n == "string") {
                continue;
            }
            return false;
        }
        true
    }
    fn apply_schema_columns(rows: &mut [Value], schema: &Map<String, Value>) {
        for r in rows {
            let Some(o) = r.as_object_mut() else {
                continue;
            };
            for k in schema.keys() {
                if !o.contains_key(k) {
                    o.insert(k.clone(), Value::Null);
                }
            }
        }
    }
    let op = req.op.trim().to_lowercase();
    match op.as_str() {
        "write" | "save" => {
            let mut rows = req.rows.unwrap_or_default();
            let mode = req
                .parquet_mode
                .unwrap_or_else(|| "typed".to_string())
                .to_lowercase();
            let compression = req
                .compression
                .unwrap_or_else(|| "snappy".to_string())
                .to_lowercase();
            let comp = parquet_compression_from_name(&compression)?;
            let partition_by = req.partition_by.unwrap_or_default();
            let schema_mode = req
                .schema_mode
                .unwrap_or_else(|| "additive".to_string())
                .to_lowercase();
            let schema_path = schema_file_path(&req.path, !partition_by.is_empty());
            let new_schema = infer_rows_schema(&rows);
            let old_schema = if schema_path.exists() {
                let txt = fs::read_to_string(&schema_path).unwrap_or_default();
                serde_json::from_str::<Map<String, Value>>(&txt).unwrap_or_default()
            } else {
                Map::new()
            };
            if !old_schema.is_empty() && !schema_compatible(&old_schema, &new_schema, &schema_mode)
            {
                return Err(format!(
                    "parquet_io_v2 schema evolution incompatible under mode={schema_mode}"
                ));
            }
            if !old_schema.is_empty() {
                apply_schema_columns(&mut rows, &old_schema);
            }
            if let Some(parent) = schema_path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let final_schema = if old_schema.is_empty() {
                new_schema.clone()
            } else {
                let mut m = old_schema.clone();
                for (k, v) in new_schema {
                    m.insert(k, v);
                }
                m
            };
            let _ = fs::write(
                &schema_path,
                serde_json::to_string_pretty(&final_schema).unwrap_or_else(|_| "{}".to_string()),
            );
            if partition_by.is_empty() {
                if mode == "payload" {
                    save_rows_parquet_payload_with_compression(&req.path, &rows, comp)?;
                } else {
                    save_rows_parquet_typed_with_compression(&req.path, &rows, comp)?;
                }
                return Ok(
                    json!({"ok": true, "operator": "parquet_io_v2", "status": "done", "run_id": req.run_id, "op": op, "path": req.path, "written_rows": rows.len(), "mode": mode, "compression": compression, "schema_mode": schema_mode, "schema_path": schema_path.to_string_lossy().to_string()}),
                );
            }
            let mut parts = HashMap::<String, Vec<Value>>::new();
            for r in rows {
                let Some(obj) = r.as_object() else {
                    continue;
                };
                let mut key = Vec::new();
                for p in &partition_by {
                    let v = value_to_string_or_null(obj.get(p));
                    key.push(format!("{p}={v}"));
                }
                parts
                    .entry(key.join(std::path::MAIN_SEPARATOR_STR))
                    .or_default()
                    .push(Value::Object(obj.clone()));
            }
            let mut written = 0usize;
            let mut files = Vec::new();
            for (part, rows) in parts {
                let path = Path::new(&req.path).join(part).join("part-00001.parquet");
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)
                        .map_err(|e| format!("create parquet partition dir: {e}"))?;
                }
                let path_str = path.to_string_lossy().to_string();
                if mode == "payload" {
                    save_rows_parquet_payload_with_compression(&path_str, &rows, comp)?;
                } else {
                    save_rows_parquet_typed_with_compression(&path_str, &rows, comp)?;
                }
                written += rows.len();
                files.push(path_str);
            }
            Ok(
                json!({"ok": true, "operator": "parquet_io_v2", "status": "done", "run_id": req.run_id, "op": op, "path": req.path, "written_rows": written, "mode": mode, "compression": compression, "partition_by": partition_by, "files": files, "schema_mode": schema_mode, "schema_path": schema_path.to_string_lossy().to_string()}),
            )
        }
        "read" | "load" => {
            let base = Path::new(&req.path);
            let mut rows = if base.is_dir() {
                let mut files = Vec::new();
                collect_parquet_paths(base, req.recursive.unwrap_or(true), &mut files)?;
                let mut acc = Vec::new();
                let mut scanned_files = 0usize;
                let mut pruned_files = 0usize;
                for f in files {
                    if !partition_predicate_match(
                        &f,
                        req.predicate_field.as_deref(),
                        req.predicate_eq.as_ref(),
                    ) {
                        pruned_files += 1;
                        continue;
                    }
                    scanned_files += 1;
                    let path = f.to_string_lossy().to_string();
                    let mut part = load_parquet_rows(&path, req.limit.unwrap_or(10000))?;
                    acc.append(&mut part);
                }
                if let Some(obj) = acc.first_mut().and_then(|v| v.as_object_mut()) {
                    obj.insert("__parquet_scanned_files".to_string(), json!(scanned_files));
                    obj.insert("__parquet_pruned_files".to_string(), json!(pruned_files));
                }
                acc
            } else {
                load_parquet_rows(&req.path, req.limit.unwrap_or(10000))?
            };
            if let (Some(field), Some(eqv)) =
                (req.predicate_field.as_ref(), req.predicate_eq.as_ref())
            {
                let eq = value_to_string(eqv);
                rows.retain(|r| {
                    r.as_object()
                        .map(|o| value_to_string_or_null(o.get(field)) == eq)
                        .unwrap_or(false)
                });
            }
            if let Some(cols) = req.columns.as_ref() {
                rows = rows
                    .into_iter()
                    .filter_map(|r| {
                        let o = r.as_object()?;
                        let mut m = Map::new();
                        for c in cols {
                            if let Some(v) = o.get(c) {
                                m.insert(c.clone(), v.clone());
                            }
                        }
                        Some(Value::Object(m))
                    })
                    .collect();
            }
            let mut scanned_files = Value::Null;
            let mut pruned_files = Value::Null;
            if let Some(first) = rows.first_mut().and_then(|v| v.as_object_mut()) {
                scanned_files = first
                    .remove("__parquet_scanned_files")
                    .unwrap_or(Value::Null);
                pruned_files = first
                    .remove("__parquet_pruned_files")
                    .unwrap_or(Value::Null);
            }
            Ok(
                json!({"ok": true, "operator": "parquet_io_v2", "status": "done", "run_id": req.run_id, "op": op, "path": req.path, "rows": rows, "recursive": req.recursive.unwrap_or(true), "partition_pruning": {"scanned_files": scanned_files, "pruned_files": pruned_files}}),
            )
        }
        "inspect" | "inspect_schema" => {
            let md =
                fs::metadata(&req.path).map_err(|e| format!("parquet inspect metadata: {e}"))?;
            let sample = load_parquet_rows(&req.path, req.limit.unwrap_or(20))?;
            let columns = sample
                .first()
                .and_then(|v| v.as_object())
                .map(|o| o.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            let schema_hint = if let Some(first) = sample.first().and_then(|v| v.as_object()) {
                let mut m = Map::new();
                for (k, v) in first {
                    let t = if v.is_number() {
                        "number"
                    } else if v.is_boolean() {
                        "bool"
                    } else if v.is_null() {
                        "null"
                    } else {
                        "string"
                    };
                    m.insert(k.clone(), json!(t));
                }
                Value::Object(m)
            } else {
                json!({})
            };
            Ok(json!({
                "ok": true,
                "operator": "parquet_io_v2",
                "status": "done",
                "run_id": req.run_id,
                "op": op,
                "path": req.path,
                "bytes": md.len(),
                "sample_rows": sample.len(),
                "columns": columns,
                "schema_hint": schema_hint
            }))
        }
        "merge_small" => {
            let base = Path::new(&req.path);
            if !base.is_dir() {
                return Err("parquet_io_v2 merge_small requires directory path".to_string());
            }
            let mut files = Vec::new();
            collect_parquet_paths(base, true, &mut files)?;
            if files.is_empty() {
                return Ok(
                    json!({"ok": true, "operator":"parquet_io_v2", "status":"done", "run_id": req.run_id, "op": op, "merged": 0, "path": req.path}),
                );
            }
            let mut all_rows = Vec::new();
            for f in &files {
                let path = f.to_string_lossy().to_string();
                let mut part = load_parquet_rows(&path, 1_000_000)?;
                all_rows.append(&mut part);
            }
            let merged_path = base.join("_merged.parquet");
            save_rows_parquet_typed_with_compression(
                &merged_path.to_string_lossy(),
                &all_rows,
                parquet_compression_from_name(req.compression.as_deref().unwrap_or("snappy"))?,
            )?;
            Ok(json!({
                "ok": true,
                "operator": "parquet_io_v2",
                "status": "done",
                "run_id": req.run_id,
                "op": op,
                "input_files": files.len(),
                "merged_rows": all_rows.len(),
                "merged_path": merged_path.to_string_lossy().to_string()
            }))
        }
        _ => Err(format!("parquet_io_v2 unsupported op: {}", req.op)),
    }
}

fn run_stream_state_v2(req: StreamStateV2Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let backend = req
        .backend
        .as_deref()
        .unwrap_or("file")
        .trim()
        .to_lowercase();
    let event_late = if let (Some(ts), Some(max_late)) = (req.event_ts_ms, req.max_late_ms) {
        let now_ms = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()) as i64;
        now_ms.saturating_sub(ts) > max_late as i64
    } else {
        false
    };
    if event_late && (op == "save" || op == "upsert" || op == "checkpoint") {
        return Ok(json!({
            "ok": true,
            "operator": "stream_state_v2",
            "status": "done",
            "run_id": req.run_id,
            "op": op,
            "stream_key": req.stream_key,
            "late_dropped": true
        }));
    }
    if backend == "sqlite" {
        let conn = ensure_stream_state_sqlite(&stream_state_sqlite_path(&req))?;
        match op.as_str() {
            "load" | "get" | "restore" => {
                let mut stmt = conn
                    .prepare(
                        "SELECT state_json, offset_val, version, updated_at FROM stream_state WHERE stream_key=?1",
                    )
                    .map_err(|e| format!("stream sqlite prepare load: {e}"))?;
                let mut rows = stmt
                    .query([req.stream_key.as_str()])
                    .map_err(|e| format!("stream sqlite load query: {e}"))?;
                if let Some(r) = rows
                    .next()
                    .map_err(|e| format!("stream sqlite load next: {e}"))?
                {
                    let state_json: String = r
                        .get(0)
                        .map_err(|e| format!("stream sqlite get state: {e}"))?;
                    let offset: i64 = r
                        .get(1)
                        .map_err(|e| format!("stream sqlite get offset: {e}"))?;
                    let version: i64 = r
                        .get(2)
                        .map_err(|e| format!("stream sqlite get version: {e}"))?;
                    let updated_at: String = r
                        .get(3)
                        .map_err(|e| format!("stream sqlite get updated_at: {e}"))?;
                    let state = serde_json::from_str::<Value>(&state_json).unwrap_or(Value::Null);
                    return Ok(
                        json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"sqlite", "op": op, "stream_key": req.stream_key, "value": {"state": state, "offset": offset.max(0) as u64, "version": version.max(0) as u64, "updated_at": updated_at}}),
                    );
                }
                return Ok(
                    json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"sqlite", "op": op, "stream_key": req.stream_key, "value": Value::Null}),
                );
            }
            "delete" => {
                let n = conn
                    .execute(
                        "DELETE FROM stream_state WHERE stream_key=?1",
                        [req.stream_key.as_str()],
                    )
                    .map_err(|e| format!("stream sqlite delete: {e}"))?;
                return Ok(
                    json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"sqlite", "op": op, "stream_key": req.stream_key, "deleted": n > 0}),
                );
            }
            "save" | "upsert" | "checkpoint" => {
                let mut stmt = conn
                    .prepare("SELECT version FROM stream_state WHERE stream_key=?1")
                    .map_err(|e| format!("stream sqlite prepare version: {e}"))?;
                let mut rows = stmt
                    .query([req.stream_key.as_str()])
                    .map_err(|e| format!("stream sqlite query version: {e}"))?;
                let cur_ver = if let Some(r) = rows
                    .next()
                    .map_err(|e| format!("stream sqlite next version: {e}"))?
                {
                    let v: i64 = r
                        .get(0)
                        .map_err(|e| format!("stream sqlite get version: {e}"))?;
                    v.max(0) as u64
                } else {
                    0
                };
                if let Some(exp) = req.expected_version
                    && exp != cur_ver
                {
                    return Err(format!(
                        "stream_state_v2 version mismatch: expected={}, current={}",
                        exp, cur_ver
                    ));
                }
                let next_ver = req.checkpoint_version.unwrap_or(cur_ver + 1);
                let state = req.state.unwrap_or(Value::Null);
                let state_json = serde_json::to_string(&state)
                    .map_err(|e| format!("stream sqlite encode state: {e}"))?;
                conn.execute(
                    "INSERT INTO stream_state(stream_key, state_json, offset_val, version, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(stream_key) DO UPDATE SET
                       state_json=excluded.state_json,
                       offset_val=excluded.offset_val,
                       version=excluded.version,
                       updated_at=excluded.updated_at",
                    (
                        req.stream_key.as_str(),
                        state_json.as_str(),
                        req.offset.unwrap_or(0) as i64,
                        next_ver as i64,
                        utc_now_iso(),
                    ),
                )
                .map_err(|e| format!("stream sqlite upsert: {e}"))?;
                return Ok(
                    json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"sqlite", "op": op, "stream_key": req.stream_key, "version": next_ver}),
                );
            }
            "list" => {
                let mut stmt = conn
                    .prepare("SELECT stream_key, version, updated_at FROM stream_state ORDER BY updated_at DESC LIMIT 200")
                    .map_err(|e| format!("stream sqlite prepare list: {e}"))?;
                let mut rows = stmt
                    .query([])
                    .map_err(|e| format!("stream sqlite list query: {e}"))?;
                let mut items = Vec::new();
                while let Some(r) = rows
                    .next()
                    .map_err(|e| format!("stream sqlite list next: {e}"))?
                {
                    let stream_key: String = r
                        .get(0)
                        .map_err(|e| format!("stream sqlite list key: {e}"))?;
                    let version: i64 = r
                        .get(1)
                        .map_err(|e| format!("stream sqlite list version: {e}"))?;
                    let updated_at: String = r
                        .get(2)
                        .map_err(|e| format!("stream sqlite list updated_at: {e}"))?;
                    items.push(json!({"stream_key": stream_key, "version": version.max(0) as u64, "updated_at": updated_at}));
                }
                return Ok(
                    json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"sqlite", "op": op, "items": items}),
                );
            }
            _ => return Err(format!("stream_state_v2 unsupported op: {}", req.op)),
        }
    }
    let mut store = load_kv_store(&stream_state_store_path());
    match op.as_str() {
        "load" | "get" | "restore" => {
            let v = store.get(&req.stream_key).cloned().unwrap_or(Value::Null);
            Ok(
                json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"file", "op": op, "stream_key": req.stream_key, "value": v}),
            )
        }
        "delete" => {
            let existed = store.remove(&req.stream_key).is_some();
            save_kv_store(&stream_state_store_path(), &store)?;
            Ok(
                json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"file", "op": op, "stream_key": req.stream_key, "deleted": existed}),
            )
        }
        "save" | "upsert" | "checkpoint" => {
            let cur_ver = store
                .get(&req.stream_key)
                .and_then(|v| v.get("version"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if let Some(exp) = req.expected_version
                && exp != cur_ver
            {
                return Err(format!(
                    "stream_state_v2 version mismatch: expected={}, current={}",
                    exp, cur_ver
                ));
            }
            let next_ver = req.checkpoint_version.unwrap_or(cur_ver + 1);
            store.insert(
                req.stream_key.clone(),
                json!({
                    "state": req.state.unwrap_or(Value::Null),
                    "offset": req.offset.unwrap_or(0),
                    "version": next_ver,
                    "updated_at": utc_now_iso()
                }),
            );
            save_kv_store(&stream_state_store_path(), &store)?;
            Ok(
                json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"file", "op": op, "stream_key": req.stream_key, "version": next_ver}),
            )
        }
        "list" => {
            let items = store
                .iter()
                .map(|(k, v)| {
                    json!({
                        "stream_key": k,
                        "version": v.get("version").and_then(|x| x.as_u64()).unwrap_or(0),
                        "updated_at": v.get("updated_at").cloned().unwrap_or(Value::Null)
                    })
                })
                .collect::<Vec<_>>();
            Ok(
                json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"file", "op": op, "items": items}),
            )
        }
        _ => Err(format!("stream_state_v2 unsupported op: {}", req.op)),
    }
}

fn run_udf_wasm_v2(req: UdfWasmV2Req) -> Result<Value, String> {
    if let Ok(token) = env::var("AIWF_UDF_V2_TOKEN")
        && !token.trim().is_empty()
    {
        let got = req.signed_token.clone().unwrap_or_default();
        if got != token {
            return Err("udf_wasm_v2 invalid signed_token".to_string());
        }
    }
    let op = req.op.clone().unwrap_or_else(|| "identity".to_string());
    if let Some(allow) = req.allowed_ops.as_ref()
        && !allow.iter().any(|x| x.eq_ignore_ascii_case(&op))
    {
        return Err(format!("udf_wasm_v2 op not allowed: {op}"));
    }
    let mut out = run_udf_wasm_v1(UdfWasmReq {
        run_id: req.run_id.clone(),
        rows: req.rows,
        field: req.field,
        output_field: req.output_field,
        op: Some(op),
        wasm_base64: req.wasm_base64,
    })?;
    let max_bytes = req.max_output_bytes.unwrap_or(1_000_000).max(4096);
    let mut truncated = 0usize;
    while serde_json::to_vec(&out).map(|v| v.len()).unwrap_or(0) > max_bytes {
        let Some(rows) = out.get_mut("rows").and_then(|v| v.as_array_mut()) else {
            break;
        };
        if rows.pop().is_none() {
            break;
        }
        truncated += 1;
    }
    if let Some(stats) = out.get_mut("stats").and_then(|v| v.as_object_mut()) {
        stats.insert("max_output_bytes".to_string(), json!(max_bytes));
        stats.insert("truncated_rows".to_string(), json!(truncated));
        stats.insert("udf_v2".to_string(), json!(true));
    }
    if let Some(obj) = out.as_object_mut() {
        obj.insert("operator".to_string(), json!("udf_wasm_v2"));
    }
    Ok(out)
}

fn run_explain_plan_v1(req: ExplainPlanV1Req) -> Result<Value, String> {
    let row_count = req.rows.as_ref().map(|r| r.len()).unwrap_or(0);
    let feedback = load_kv_store(&explain_feedback_store_path());
    let opt = run_optimizer_v1(OptimizerV1Req {
        run_id: req.run_id.clone(),
        rows: req.rows.clone(),
        row_count_hint: Some(row_count),
        prefer_arrow: Some(true),
        join_hint: None,
        aggregate_hint: None,
    })?;
    let mut total = 0.0f64;
    let mut steps = Vec::new();
    for (i, s) in req.steps.iter().enumerate() {
        let op = s
            .as_object()
            .and_then(|o| o.get("operator"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let hist_factor = feedback
            .get(&op)
            .and_then(|v| v.get("scale"))
            .and_then(value_to_f64)
            .unwrap_or(1.0)
            .clamp(0.3, 3.0);
        let base = if op.contains("join") {
            5.0
        } else if op.contains("aggregate") {
            4.0
        } else if op.contains("quality") {
            3.0
        } else if op.contains("load") || op.contains("save") {
            2.0
        } else {
            1.0
        };
        let scale = 1.0 + (row_count as f64 / 50_000.0);
        let cost = base * scale * hist_factor;
        total += cost;
        steps.push(json!({
            "idx": i + 1,
            "operator": op,
            "estimated_cost": (cost * 100.0).round() / 100.0,
            "history_scale": hist_factor
        }));
    }
    let mut feedback_updates = Vec::new();
    if let Some(actuals) = req.actual_stats.as_ref() {
        let mut store = load_kv_store(&explain_feedback_store_path());
        for a in actuals {
            let Some(o) = a.as_object() else {
                continue;
            };
            let op = o
                .get("operator")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if op.is_empty() {
                continue;
            }
            let est = o.get("estimated_ms").and_then(value_to_f64).unwrap_or(0.0);
            let act = o.get("actual_ms").and_then(value_to_f64).unwrap_or(0.0);
            if est <= 0.0 || act <= 0.0 {
                continue;
            }
            let ratio = (act / est).clamp(0.2, 5.0);
            let prev = store
                .get(&op)
                .and_then(|v| v.get("scale"))
                .and_then(value_to_f64)
                .unwrap_or(1.0);
            let next = (0.7 * prev + 0.3 * ratio).clamp(0.3, 3.0);
            store.insert(
                op.clone(),
                json!({"scale": next, "updated_at": utc_now_iso()}),
            );
            feedback_updates
                .push(json!({"operator": op, "prev": prev, "next": next, "ratio": ratio}));
        }
        if req.persist_feedback.unwrap_or(true) {
            save_kv_store(&explain_feedback_store_path(), &store)?;
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "explain_plan_v1",
        "status": "done",
        "run_id": req.run_id,
        "estimated_total_cost": (total * 100.0).round() / 100.0,
        "optimizer_plan": opt.get("plan").cloned().unwrap_or_else(|| json!({})),
        "steps": steps,
        "feedback_updates": feedback_updates
    }))
}

fn run_save_rows_v1(req: SaveRowsReq) -> Result<SaveRowsResp, String> {
    let st = req.sink_type.to_lowercase();
    match st.as_str() {
        "jsonl" => save_rows_jsonl(&req.sink, &req.rows)?,
        "csv" => save_rows_csv(&req.sink, &req.rows)?,
        "sqlite" => save_rows_sqlite(&req.sink, req.table.as_deref().unwrap_or("data"), &req.rows)?,
        "sqlserver" => save_rows_sqlserver(
            &req.sink,
            req.table.as_deref().unwrap_or("dbo.aiwf_rows"),
            &req.rows,
        )?,
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

fn run_transform_rows_v2_stream(
    req: TransformRowsStreamReq,
) -> Result<TransformRowsStreamResp, String> {
    let chunk_size = req.chunk_size.unwrap_or(2000).max(1);
    let max_chunks_per_run = req.max_chunks_per_run.unwrap_or(usize::MAX).max(1);
    let mut rows_in = if let Some(rows) = req.rows.clone() {
        rows
    } else if let Some(uri) = req.input_uri.clone() {
        load_rows_from_uri_limited(&uri, tenant_max_rows(), tenant_max_payload_bytes())?
    } else {
        return Err("rows or input_uri is required".to_string());
    };
    let mut watermark_dropped = 0usize;
    if let (Some(field), Some(wv)) = (req.watermark_field.as_ref(), req.watermark_value.as_ref()) {
        let w_num = value_to_f64(wv);
        rows_in.retain(|r| {
            let Some(obj) = r.as_object() else {
                return false;
            };
            let Some(cur) = obj.get(field) else {
                return false;
            };
            let keep = match (value_to_f64(cur), w_num) {
                (Some(a), Some(b)) => a > b,
                _ => value_to_string(cur) > value_to_string(wv),
            };
            if !keep {
                watermark_dropped += 1;
            }
            keep
        });
    }
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
    let mut has_more = false;
    let mut next_checkpoint: Option<usize> = None;
    for (chunk_idx, chunk) in rows_in.chunks(chunk_size).enumerate() {
        if chunk_idx < start_chunk {
            continue;
        }
        if chunks >= max_chunks_per_run {
            has_more = true;
            break;
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
            next_checkpoint = Some(chunk_idx);
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
        has_more,
        next_checkpoint,
        stats: json!({
            "input_rows": total_input,
            "output_rows": total_output,
            "chunk_size": chunk_size,
            "max_chunks_per_run": if max_chunks_per_run == usize::MAX { Value::Null } else { json!(max_chunks_per_run) },
            "resumed_from_chunk": start_chunk,
            "watermark_field": req.watermark_field,
            "watermark_dropped_rows": watermark_dropped,
            "checkpoint_key": req.checkpoint_key
        }),
    })
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

fn tenant_policy_for(tenant: &str) -> Value {
    let store = load_kv_store(&tenant_isolation_store_path());
    store.get(tenant).cloned().unwrap_or_else(|| json!({}))
}

fn tenant_override_usize(tenant: &str, key: &str) -> Option<usize> {
    tenant_policy_for(tenant)
        .get(key)
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
}

fn tenant_max_concurrency_for(tenant: Option<&str>) -> usize {
    if let Some(t) = tenant {
        if let Some(v) = tenant_override_usize(t, "max_concurrency") {
            return v.max(1);
        }
    }
    tenant_max_concurrency()
}

fn tenant_max_rows() -> usize {
    env::var("AIWF_TENANT_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(250_000)
        .max(1)
}

fn tenant_max_rows_for(tenant: Option<&str>) -> usize {
    if let Some(t) = tenant {
        if let Some(v) = tenant_override_usize(t, "max_rows") {
            return v.max(1);
        }
    }
    tenant_max_rows()
}

fn tenant_max_payload_bytes() -> usize {
    env::var("AIWF_TENANT_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(128 * 1024 * 1024)
        .max(1024)
}

fn tenant_max_payload_bytes_for(tenant: Option<&str>) -> usize {
    if let Some(t) = tenant {
        if let Some(v) = tenant_override_usize(t, "max_payload_bytes") {
            return v.max(1024);
        }
    }
    tenant_max_payload_bytes()
}

fn tenant_max_workflow_steps() -> usize {
    env::var("AIWF_TENANT_MAX_WORKFLOW_STEPS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(128)
        .max(1)
}

fn tenant_max_workflow_steps_for(tenant: Option<&str>) -> usize {
    if let Some(t) = tenant {
        if let Some(v) = tenant_override_usize(t, "max_workflow_steps") {
            return v.max(1);
        }
    }
    tenant_max_workflow_steps()
}

fn enforce_tenant_payload_quota(
    state: Option<&AppState>,
    tenant: Option<&str>,
    rows: usize,
    payload_bytes: usize,
) -> Result<(), String> {
    let max_rows = tenant_max_rows_for(tenant);
    if rows > max_rows {
        if let Some(s) = state
            && let Ok(mut m) = s.metrics.lock()
        {
            m.quota_reject_total += 1;
        }
        return Err(format!("tenant row quota exceeded: {rows} > {max_rows}"));
    }
    let max_bytes = tenant_max_payload_bytes_for(tenant);
    if payload_bytes > max_bytes {
        if let Some(s) = state
            && let Ok(mut m) = s.metrics.lock()
        {
            m.quota_reject_total += 1;
        }
        return Err(format!(
            "tenant payload quota exceeded: {payload_bytes} > {max_bytes}"
        ));
    }
    Ok(())
}

fn try_acquire_tenant_slot(state: &AppState, tenant: &str) -> Result<(), String> {
    let limit = tenant_max_concurrency_for(Some(tenant));
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

fn parse_env_op_set(name: &str) -> HashSet<String> {
    env::var(name)
        .ok()
        .unwrap_or_default()
        .split([',', ';'])
        .map(|x| x.trim().to_lowercase())
        .filter(|x| !x.is_empty())
        .collect()
}

fn operator_allowed_for_tenant(operator: &str, tenant: Option<&str>) -> bool {
    let op = operator.trim().to_lowercase();
    if op.is_empty() {
        return false;
    }
    let global_allow = parse_env_op_set("AIWF_OPERATOR_ALLOWLIST");
    if !global_allow.is_empty() && !global_allow.contains(&op) {
        return false;
    }
    let global_deny = parse_env_op_set("AIWF_OPERATOR_DENYLIST");
    if global_deny.contains(&op) {
        return false;
    }
    let t = tenant.unwrap_or("default").trim().to_lowercase();
    if t.is_empty() {
        return true;
    }
    let store = load_kv_store(&operator_policy_store_path());
    let Some(rule) = store.get(&t).and_then(|v| v.as_object()) else {
        return true;
    };
    if let Some(deny) = rule.get("deny").and_then(|v| v.as_array()) {
        let set: HashSet<String> = deny
            .iter()
            .filter_map(|x| x.as_str())
            .map(|x| x.trim().to_lowercase())
            .collect();
        if set.contains(&op) {
            return false;
        }
    }
    if let Some(allow) = rule.get("allow").and_then(|v| v.as_array()) {
        let set: HashSet<String> = allow
            .iter()
            .filter_map(|x| x.as_str())
            .map(|x| x.trim().to_lowercase())
            .collect();
        if !set.is_empty() {
            return set.contains(&op);
        }
    }
    true
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

fn validate_readonly_query(query: &str) -> Result<String, String> {
    let q = query.trim();
    if q.is_empty() {
        return Err("query is empty".to_string());
    }
    if q.contains(';') {
        return Err("query contains ';' which is not allowed".to_string());
    }
    let lower = q.to_lowercase();
    if !lower.starts_with("select ") {
        return Err("only SELECT query is allowed".to_string());
    }
    let banned = [
        " insert ",
        " update ",
        " delete ",
        " drop ",
        " alter ",
        " create ",
        " truncate ",
        " exec ",
        " execute ",
        " attach ",
        " pragma ",
    ];
    if banned.iter().any(|kw| lower.contains(kw)) {
        return Err("query contains forbidden keyword".to_string());
    }
    Ok(q.to_string())
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

fn unix_now_sec() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn transform_cache_enabled() -> bool {
    env::var("AIWF_RUST_TRANSFORM_CACHE_ENABLED")
        .ok()
        .map(|v| {
            let t = v.trim().to_ascii_lowercase();
            t == "1" || t == "true" || t == "yes" || t == "on"
        })
        .unwrap_or(true)
}

fn transform_cache_ttl_sec() -> u64 {
    env::var("AIWF_RUST_TRANSFORM_CACHE_TTL_SEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(600)
}

fn transform_cache_max_entries() -> usize {
    env::var("AIWF_RUST_TRANSFORM_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256)
}

fn transform_cache_key(req: &TransformRowsReq) -> String {
    let key_obj = json!({
        "tenant_id": req.tenant_id,
        "rows": req.rows,
        "rules": req.rules,
        "rules_dsl": req.rules_dsl,
        "quality_gates": req.quality_gates,
        "schema_hint": req.schema_hint,
        "input_uri": req.input_uri,
    });
    let mut h = Sha256::new();
    h.update(key_obj.to_string().as_bytes());
    format!("{:x}", h.finalize())
}

fn prune_transform_cache_entries(
    cache: &mut HashMap<String, TransformCacheEntry>,
    now: u64,
    max_entries: usize,
) -> usize {
    let before = cache.len();
    cache.retain(|_, v| v.expires_at_epoch > now);
    if cache.len() <= max_entries {
        return before.saturating_sub(cache.len());
    }
    let mut pairs: Vec<(String, u64)> = cache
        .iter()
        .map(|(k, v)| (k.clone(), v.last_hit_epoch))
        .collect();
    pairs.sort_by_key(|(_, ts)| *ts);
    let mut evicted = before.saturating_sub(cache.len());
    for (k, _) in pairs {
        if cache.len() <= max_entries {
            break;
        }
        if cache.remove(&k).is_some() {
            evicted += 1;
        }
    }
    evicted
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

fn parse_expr_arg(token: &str, row: &Map<String, Value>) -> Value {
    let t = token.trim();
    if let Some(field) = t.strip_prefix('$') {
        return row.get(field).cloned().unwrap_or(Value::Null);
    }
    if (t.starts_with('"') && t.ends_with('"')) || (t.starts_with('\'') && t.ends_with('\'')) {
        return Value::String(t[1..t.len().saturating_sub(1)].to_string());
    }
    if let Ok(v) = t.parse::<i64>() {
        return Value::Number(v.into());
    }
    if let Ok(v) = t.parse::<f64>() {
        return serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    row.get(t).cloned().unwrap_or(Value::Null)
}

fn eval_simple_expr(expr: &str, row: &Map<String, Value>) -> Value {
    let e = expr.trim();
    let open = e.find('(');
    let close = e.rfind(')');
    let Some(l) = open else {
        return parse_expr_arg(e, row);
    };
    let Some(r) = close else {
        return parse_expr_arg(e, row);
    };
    if r <= l {
        return parse_expr_arg(e, row);
    }
    let fn_name = e[..l].trim().to_ascii_lowercase();
    let args_txt = &e[l + 1..r];
    let args = args_txt
        .split(',')
        .map(|s| parse_expr_arg(s, row))
        .collect::<Vec<_>>();
    match fn_name.as_str() {
        "add" => {
            let a = args.first().and_then(value_to_f64).unwrap_or(0.0);
            let b = args.get(1).and_then(value_to_f64).unwrap_or(0.0);
            json!(a + b)
        }
        "sub" => {
            let a = args.first().and_then(value_to_f64).unwrap_or(0.0);
            let b = args.get(1).and_then(value_to_f64).unwrap_or(0.0);
            json!(a - b)
        }
        "mul" => {
            let a = args.first().and_then(value_to_f64).unwrap_or(0.0);
            let b = args.get(1).and_then(value_to_f64).unwrap_or(0.0);
            json!(a * b)
        }
        "div" => {
            let a = args.first().and_then(value_to_f64).unwrap_or(0.0);
            let b = args.get(1).and_then(value_to_f64).unwrap_or(0.0);
            if b == 0.0 { Value::Null } else { json!(a / b) }
        }
        "concat" => Value::String(
            args.iter()
                .map(value_to_string)
                .collect::<Vec<_>>()
                .join(""),
        ),
        "coalesce" => args
            .into_iter()
            .find(|v| !is_missing(Some(v)))
            .unwrap_or(Value::Null),
        "lower" => Value::String(
            args.first()
                .map(value_to_string)
                .unwrap_or_default()
                .to_lowercase(),
        ),
        "upper" => Value::String(
            args.first()
                .map(value_to_string)
                .unwrap_or_default()
                .to_uppercase(),
        ),
        "trim" => Value::String(
            args.first()
                .map(value_to_string)
                .unwrap_or_default()
                .trim()
                .to_string(),
        ),
        _ => Value::Null,
    }
}

fn apply_expression_fields(
    rows: &mut [Map<String, Value>],
    rules: &Value,
    rule_hits: &mut HashMap<String, usize>,
) {
    let Some(exprs) = rule_get(rules, "computed_fields").and_then(|v| v.as_object()) else {
        return;
    };
    for r in rows {
        for (field, expr_v) in exprs {
            let Some(expr) = expr_v.as_str() else {
                continue;
            };
            let v = eval_simple_expr(expr, r);
            r.insert(field.clone(), v);
            *rule_hits.entry("computed_fields".to_string()).or_insert(0) += 1;
        }
    }
}

fn parse_ymd_simple(s: &str) -> Option<(i64, i64, i64)> {
    let t = s.trim();
    let sep = if t.contains('-') {
        '-'
    } else if t.contains('/') {
        '/'
    } else {
        return None;
    };
    let parts = t.split(sep).collect::<Vec<_>>();
    if parts.len() < 3 {
        return None;
    }
    let y = parts[0].trim().parse::<i64>().ok()?;
    let m = parts[1].trim().parse::<i64>().ok()?;
    let d = parts[2].trim().parse::<i64>().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some((y, m, d))
}

fn apply_string_and_date_ops(
    rows: &mut [Map<String, Value>],
    rules: &Value,
    rule_hits: &mut HashMap<String, usize>,
) {
    let string_ops = rule_get(rules, "string_ops")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let date_ops = rule_get(rules, "date_ops")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for r in rows {
        for op in &string_ops {
            let Some(obj) = op.as_object() else { continue };
            let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
            let kind = obj
                .get("op")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            if field.is_empty() || kind.is_empty() {
                continue;
            }
            let cur = r.get(field).map(value_to_string).unwrap_or_default();
            let next = match kind.as_str() {
                "trim" => cur.trim().to_string(),
                "lower" => cur.to_lowercase(),
                "upper" => cur.to_uppercase(),
                "replace" => {
                    let from = obj.get("from").map(value_to_string).unwrap_or_default();
                    let to = obj.get("to").map(value_to_string).unwrap_or_default();
                    cur.replace(&from, &to)
                }
                _ => cur,
            };
            r.insert(field.to_string(), Value::String(next));
            *rule_hits.entry("string_ops".to_string()).or_insert(0) += 1;
        }
        for op in &date_ops {
            let Some(obj) = op.as_object() else { continue };
            let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
            let kind = obj
                .get("op")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            let out_field = obj.get("as").and_then(|v| v.as_str()).unwrap_or(field);
            if field.is_empty() || kind.is_empty() {
                continue;
            }
            let raw = r.get(field).map(value_to_string).unwrap_or_default();
            let out = match parse_ymd_simple(&raw) {
                Some((y, m, d)) => match kind.as_str() {
                    "parse_ymd" => Value::String(format!("{y:04}-{m:02}-{d:02}")),
                    "year" => Value::Number(y.into()),
                    "month" => Value::Number(m.into()),
                    "day" => Value::Number(d.into()),
                    _ => Value::Null,
                },
                None => Value::Null,
            };
            r.insert(out_field.to_string(), out);
            *rule_hits.entry("date_ops".to_string()).or_insert(0) += 1;
        }
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
                        Regex::new(&pat)
                            .map(FilterOp::Regex)
                            .unwrap_or(FilterOp::Invalid)
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

fn compare_rows(
    a: &Map<String, Value>,
    b: &Map<String, Value>,
    sort_by: &[Value],
) -> std::cmp::Ordering {
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

fn resolve_transform_engine(rules: &Value) -> String {
    if let Some(v) = rule_get(rules, "execution_engine").and_then(|x| x.as_str()) {
        let t = v.trim().to_ascii_lowercase();
        if !t.is_empty() {
            return t;
        }
    }
    env::var("AIWF_RUST_TRANSFORM_ENGINE")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "auto_v1".to_string())
}

fn estimate_rule_complexity(rules: &Value) -> usize {
    let casts = rule_get(rules, "casts")
        .and_then(|v| v.as_object())
        .map(|m| m.len())
        .unwrap_or(0);
    let filters = rule_get(rules, "filters")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let dedup = rule_get(rules, "deduplicate_by")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let sort = rule_get(rules, "sort_by")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let has_agg = if rule_get(rules, "aggregate").is_some() {
        2
    } else {
        0
    };
    casts + filters + dedup + sort + has_agg
}

#[derive(Clone)]
struct EngineProfile {
    medium_rows_threshold: usize,
    large_rows_threshold: usize,
    medium_complexity_threshold: usize,
    medium_bytes_threshold: usize,
    large_bytes_threshold: usize,
    row_cost_per_row: f64,
    columnar_cost_per_row: f64,
    arrow_cost_per_row: f64,
    complexity_weight: f64,
}

fn default_engine_profile() -> EngineProfile {
    EngineProfile {
        medium_rows_threshold: 20_000,
        large_rows_threshold: 120_000,
        medium_complexity_threshold: 8,
        medium_bytes_threshold: 12 * 1024 * 1024,
        large_bytes_threshold: 48 * 1024 * 1024,
        row_cost_per_row: 1.0,
        columnar_cost_per_row: 0.9,
        arrow_cost_per_row: 0.8,
        complexity_weight: 0.08,
    }
}

fn load_engine_profile() -> EngineProfile {
    let default = default_engine_profile();
    let path = env::var("AIWF_RUST_ENGINE_PROFILE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| {
            Path::new(".")
                .join("conf")
                .join("transform_engine_profile.json")
                .to_string_lossy()
                .to_string()
        });
    let txt = match fs::read_to_string(&path) {
        Ok(v) => v,
        Err(_) => return default,
    };
    let v: Value = match serde_json::from_str(&txt) {
        Ok(v) => v,
        Err(_) => return default,
    };
    let get_u = |k: &str, dv: usize| -> usize {
        v.get(k)
            .and_then(|x| x.as_u64())
            .map(|x| x as usize)
            .unwrap_or(dv)
    };
    let get_f = |k: &str, dv: f64| -> f64 { v.get(k).and_then(|x| x.as_f64()).unwrap_or(dv) };
    EngineProfile {
        medium_rows_threshold: get_u("medium_rows_threshold", default.medium_rows_threshold),
        large_rows_threshold: get_u("large_rows_threshold", default.large_rows_threshold),
        medium_complexity_threshold: get_u(
            "medium_complexity_threshold",
            default.medium_complexity_threshold,
        ),
        medium_bytes_threshold: get_u("medium_bytes_threshold", default.medium_bytes_threshold),
        large_bytes_threshold: get_u("large_bytes_threshold", default.large_bytes_threshold),
        row_cost_per_row: get_f("row_cost_per_row", default.row_cost_per_row),
        columnar_cost_per_row: get_f("columnar_cost_per_row", default.columnar_cost_per_row),
        arrow_cost_per_row: get_f("arrow_cost_per_row", default.arrow_cost_per_row),
        complexity_weight: get_f("complexity_weight", default.complexity_weight),
    }
}

fn auto_select_engine(
    input_rows: usize,
    estimated_bytes: usize,
    rules: &Value,
) -> (String, String) {
    let p = load_engine_profile();
    let complexity = estimate_rule_complexity(rules);
    let row_cost =
        p.row_cost_per_row * input_rows as f64 * (1.0 + p.complexity_weight * complexity as f64);
    let col_cost = p.columnar_cost_per_row
        * input_rows as f64
        * (1.0 + 0.6 * p.complexity_weight * complexity as f64);
    let arrow_cost = p.arrow_cost_per_row
        * input_rows as f64
        * (1.0 + 0.5 * p.complexity_weight * complexity as f64);
    if input_rows >= p.large_rows_threshold || estimated_bytes >= p.large_bytes_threshold {
        return (
            "columnar_arrow_v1".to_string(),
            format!(
                "auto: cost large rows={} bytes={} complexity={} row={:.2} col={:.2} arrow={:.2}",
                input_rows, estimated_bytes, complexity, row_cost, col_cost, arrow_cost
            ),
        );
    }
    if input_rows >= p.medium_rows_threshold
        || estimated_bytes >= p.medium_bytes_threshold
        || complexity >= p.medium_complexity_threshold
    {
        let (eng, best) = if col_cost <= row_cost {
            ("columnar_v1", col_cost)
        } else {
            ("row_v1", row_cost)
        };
        return (
            eng.to_string(),
            format!(
                "auto: cost medium rows={} bytes={} complexity={} row={:.2} col={:.2} choose={:.2}",
                input_rows, estimated_bytes, complexity, row_cost, col_cost, best
            ),
        );
    }
    (
        "row_v1".to_string(),
        format!(
            "auto: cost small rows={} complexity={} row={:.2} col={:.2}",
            input_rows, complexity, row_cost, col_cost
        ),
    )
}

fn request_prefers_columnar(req: &TransformRowsReq) -> bool {
    if let Some(rules) = req.rules.as_ref() {
        let eng = resolve_transform_engine(rules);
        return eng == "columnar_v1" || eng == "columnar_arrow_v1" || eng == "auto_v1";
    }
    env::var("AIWF_RUST_TRANSFORM_ENGINE")
        .ok()
        .map(|v| {
            let t = v.trim().to_ascii_lowercase();
            t == "columnar_v1" || t == "columnar_arrow_v1" || t == "auto_v1"
        })
        .unwrap_or(false)
}

fn apply_transform_columnar_v1(
    mut rows: Vec<Map<String, Value>>,
    casts: &HashMap<String, String>,
    required_fields: &[String],
    compiled_filters: &[CompiledFilter],
    rule_hits: &mut HashMap<String, usize>,
) -> (Vec<Map<String, Value>>, usize, usize) {
    if rows.is_empty() {
        return (rows, 0, 0);
    }
    let n = rows.len();
    let mut invalid = vec![false; n];
    let mut filtered = vec![false; n];

    for (field, cast_type) in casts {
        for i in 0..n {
            if invalid[i] {
                continue;
            }
            if let Some(slot) = rows[i].get_mut(field) {
                let raw = std::mem::take(slot);
                match cast_value(raw, cast_type) {
                    Some(casted) => {
                        *slot = casted;
                    }
                    None => {
                        invalid[i] = true;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                    }
                }
            }
        }
    }

    if !required_fields.is_empty() {
        for i in 0..n {
            if invalid[i] {
                continue;
            }
            let mut missing = false;
            for f in required_fields {
                if is_missing(rows[i].get(f)) {
                    missing = true;
                    break;
                }
            }
            if missing {
                invalid[i] = true;
                *rule_hits.entry("required_missing".to_string()).or_insert(0) += 1;
            }
        }
    }

    if !compiled_filters.is_empty() {
        for i in 0..n {
            if invalid[i] {
                continue;
            }
            if !compiled_filters
                .iter()
                .all(|f| filter_match_compiled(&rows[i], f))
            {
                filtered[i] = true;
                *rule_hits.entry("filtered_by_rule".to_string()).or_insert(0) += 1;
            }
        }
    }

    let mut out = Vec::with_capacity(n);
    let mut invalid_rows = 0usize;
    let mut filtered_rows = 0usize;
    for i in 0..n {
        if invalid[i] {
            invalid_rows += 1;
            continue;
        }
        if filtered[i] {
            filtered_rows += 1;
            continue;
        }
        out.push(std::mem::take(&mut rows[i]));
    }
    (out, invalid_rows, filtered_rows)
}

fn value_to_arrow_string(v: Option<&Value>) -> Option<String> {
    match v {
        None => None,
        Some(Value::Null) => None,
        Some(Value::String(s)) => {
            if s.trim().is_empty() {
                None
            } else {
                Some(s.clone())
            }
        }
        Some(other) => {
            let s = value_to_string(other);
            if s.trim().is_empty() { None } else { Some(s) }
        }
    }
}

fn scalar_from_array(arr: &ArrayRef, idx: usize) -> Value {
    if idx >= arr.len() || arr.is_null(idx) {
        return Value::Null;
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Value::String(a.value(idx).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Value::Number(a.value(idx).into());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return serde_json::Number::from_f64(a.value(idx))
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Value::Bool(a.value(idx));
    }
    Value::Null
}

fn apply_transform_columnar_arrow_v1(
    rows: Vec<Map<String, Value>>,
    casts: &HashMap<String, String>,
    required_fields: &[String],
    compiled_filters: &[CompiledFilter],
    rule_hits: &mut HashMap<String, usize>,
) -> (Vec<Map<String, Value>>, usize, usize) {
    if rows.is_empty() {
        return (rows, 0, 0);
    }
    let n = rows.len();
    let mut field_set: HashSet<String> = HashSet::new();
    for r in &rows {
        for k in r.keys() {
            field_set.insert(k.clone());
        }
    }
    for k in casts.keys() {
        field_set.insert(k.clone());
    }
    for f in required_fields {
        field_set.insert(f.clone());
    }
    for f in compiled_filters {
        if !f.field.is_empty() {
            field_set.insert(f.field.clone());
        }
    }
    let mut fields: Vec<String> = field_set.into_iter().collect();
    fields.sort();
    if fields.is_empty() {
        return (Vec::new(), 0, 0);
    }

    let mut schema_fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for f in &fields {
        let mut b = StringBuilder::new();
        for r in &rows {
            match value_to_arrow_string(r.get(f)) {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            }
        }
        schema_fields.push(Field::new(f, DataType::Utf8, true));
        columns.push(Arc::new(b.finish()) as ArrayRef);
    }
    let mut field_types: Vec<DataType> = vec![DataType::Utf8; fields.len()];
    let mut batch = match RecordBatch::try_new(Arc::new(Schema::new(schema_fields)), columns) {
        Ok(b) => b,
        Err(_) => return (Vec::new(), n, 0),
    };

    let mut invalid = vec![false; n];
    for (field, cast_type) in casts {
        let Some(idx) = fields.iter().position(|x| x == field) else {
            continue;
        };
        let src = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
            .unwrap_or_else(|| StringArray::from(vec![Option::<String>::None; n]));
        let mut next: ArrayRef = batch.column(idx).clone();
        let cast_t = cast_type.as_str();
        if cast_t == "int" || cast_t == "integer" {
            let mut builder = Int64Builder::new();
            for i in 0..n {
                if src.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = src.value(i).trim().replace(',', "");
                if s.is_empty() {
                    builder.append_null();
                    continue;
                }
                match s.parse::<i64>() {
                    Ok(v) => builder.append_value(v),
                    Err(_) => {
                        builder.append_null();
                        invalid[i] = true;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                    }
                }
            }
            next = Arc::new(builder.finish());
            field_types[idx] = DataType::Int64;
        } else if cast_t == "float" || cast_t == "double" || cast_t == "number" {
            let mut builder = Float64Builder::new();
            for i in 0..n {
                if src.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = src.value(i).trim().replace(',', "");
                if s.is_empty() {
                    builder.append_null();
                    continue;
                }
                match s.parse::<f64>() {
                    Ok(v) => builder.append_value(v),
                    Err(_) => {
                        builder.append_null();
                        invalid[i] = true;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                    }
                }
            }
            next = Arc::new(builder.finish());
            field_types[idx] = DataType::Float64;
        } else if cast_t == "bool" || cast_t == "boolean" {
            let mut builder = BooleanBuilder::new();
            for i in 0..n {
                if src.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = src.value(i).trim().to_ascii_lowercase();
                match s.as_str() {
                    "1" | "true" | "yes" | "on" => builder.append_value(true),
                    "0" | "false" | "no" | "off" => builder.append_value(false),
                    "" => builder.append_null(),
                    _ => {
                        builder.append_null();
                        invalid[i] = true;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                    }
                }
            }
            next = Arc::new(builder.finish());
            field_types[idx] = DataType::Boolean;
        }
        let mut next_cols = batch.columns().to_vec();
        next_cols[idx] = next;
        let rebuilt_schema = Schema::new(
            fields
                .iter()
                .enumerate()
                .map(|(i, f)| Field::new(f, field_types[i].clone(), true))
                .collect::<Vec<Field>>(),
        );
        batch = match RecordBatch::try_new(Arc::new(rebuilt_schema), next_cols) {
            Ok(b) => b,
            Err(_) => return (Vec::new(), n, 0),
        };
    }

    let mut filtered = vec![false; n];
    for i in 0..n {
        if invalid[i] {
            continue;
        }
        let mut row = Map::<String, Value>::new();
        for (j, f) in fields.iter().enumerate() {
            row.insert(f.clone(), scalar_from_array(batch.column(j), i));
        }
        let mut missing = false;
        for f in required_fields {
            if is_missing(row.get(f)) {
                missing = true;
                break;
            }
        }
        if missing {
            invalid[i] = true;
            *rule_hits.entry("required_missing".to_string()).or_insert(0) += 1;
            continue;
        }
        if !compiled_filters.is_empty()
            && !compiled_filters
                .iter()
                .all(|flt| filter_match_compiled(&row, flt))
        {
            filtered[i] = true;
            *rule_hits.entry("filtered_by_rule".to_string()).or_insert(0) += 1;
        }
    }

    let keep_idx: Vec<u32> = (0..n)
        .filter(|i| !invalid[*i] && !filtered[*i])
        .map(|i| i as u32)
        .collect();
    let invalid_rows = invalid.iter().filter(|x| **x).count();
    let filtered_rows = filtered.iter().filter(|x| **x).count();
    if keep_idx.is_empty() {
        return (Vec::new(), invalid_rows, filtered_rows);
    }
    let idx_arr = UInt32Array::from(keep_idx);
    let mut taken_cols: Vec<ArrayRef> = Vec::new();
    for col in batch.columns() {
        match take(col.as_ref(), &idx_arr, None) {
            Ok(c) => taken_cols.push(c),
            Err(_) => return (Vec::new(), invalid_rows, filtered_rows),
        }
    }
    let final_schema = Schema::new(
        fields
            .iter()
            .enumerate()
            .map(|(i, f)| Field::new(f, field_types[i].clone(), true))
            .collect::<Vec<Field>>(),
    );
    let kept = match RecordBatch::try_new(Arc::new(final_schema), taken_cols) {
        Ok(b) => b,
        Err(_) => return (Vec::new(), invalid_rows, filtered_rows),
    };
    let m = kept.num_rows();
    let mut out: Vec<Map<String, Value>> = Vec::with_capacity(m);
    for i in 0..m {
        let mut row = Map::<String, Value>::new();
        for (j, f) in fields.iter().enumerate() {
            row.insert(f.clone(), scalar_from_array(kept.column(j), i));
        }
        out.push(row);
    }
    (out, invalid_rows, filtered_rows)
}

fn apply_dedup_sort_columnar_v1(
    mut rows: Vec<Map<String, Value>>,
    deduplicate_by: &[String],
    dedup_keep: &str,
    sort_by: &[Value],
) -> (Vec<Map<String, Value>>, usize) {
    if rows.is_empty() {
        return (rows, 0);
    }
    let mut indices: Vec<usize> = (0..rows.len()).collect();
    let mut duplicate_rows_removed = 0usize;
    if !deduplicate_by.is_empty() {
        let mut key_keep_idx: HashMap<String, usize> = HashMap::new();
        for idx in &indices {
            let key = dedup_key(&rows[*idx], deduplicate_by);
            if dedup_keep == "first" {
                key_keep_idx.entry(key).or_insert(*idx);
            } else {
                key_keep_idx.insert(key, *idx);
            }
        }
        let before = indices.len();
        indices = key_keep_idx.into_values().collect();
        duplicate_rows_removed = before.saturating_sub(indices.len());
    }
    if !sort_by.is_empty() {
        // For single-key sort, comparator sort is generally faster than
        // building Arrow sort columns and index remapping.
        if sort_by.len() == 1 {
            indices.sort_by(|a, b| compare_rows(&rows[*a], &rows[*b], sort_by));
        } else {
            let mut sort_cols: Vec<SortColumn> = Vec::new();
            for s in sort_by {
                let (field, desc) = match s {
                    Value::String(name) => (name.clone(), false),
                    Value::Object(obj) => (
                        obj.get("field")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        obj.get("order")
                            .and_then(|v| v.as_str())
                            .unwrap_or("asc")
                            .eq_ignore_ascii_case("desc"),
                    ),
                    _ => (String::new(), false),
                };
                if field.is_empty() {
                    continue;
                }
                let mut as_num = true;
                let mut fvals: Vec<Option<f64>> = Vec::with_capacity(indices.len());
                for idx in &indices {
                    match rows[*idx].get(&field).and_then(value_to_f64) {
                        Some(v) => fvals.push(Some(v)),
                        None => {
                            fvals.push(None);
                            if !is_missing(rows[*idx].get(&field)) {
                                as_num = false;
                            }
                        }
                    }
                }
                if as_num {
                    let arr = Float64Array::from(fvals);
                    sort_cols.push(SortColumn {
                        values: Arc::new(arr) as ArrayRef,
                        options: Some(SortOptions {
                            descending: desc,
                            nulls_first: false,
                        }),
                    });
                } else {
                    let svals: Vec<Option<String>> = indices
                        .iter()
                        .map(|idx| value_to_arrow_string(rows[*idx].get(&field)))
                        .collect();
                    let arr = StringArray::from(svals);
                    sort_cols.push(SortColumn {
                        values: Arc::new(arr) as ArrayRef,
                        options: Some(SortOptions {
                            descending: desc,
                            nulls_first: false,
                        }),
                    });
                }
            }
            if !sort_cols.is_empty() {
                if let Ok(order) = lexsort_to_indices(&sort_cols, None) {
                    let mut next: Vec<usize> = Vec::with_capacity(indices.len());
                    for i in 0..order.len() {
                        let pos = order.value(i) as usize;
                        if let Some(v) = indices.get(pos) {
                            next.push(*v);
                        }
                    }
                    if next.len() == indices.len() {
                        indices = next;
                    } else {
                        indices.sort_by(|a, b| compare_rows(&rows[*a], &rows[*b], sort_by));
                    }
                } else {
                    indices.sort_by(|a, b| compare_rows(&rows[*a], &rows[*b], sort_by));
                }
            } else {
                indices.sort_by(|a, b| compare_rows(&rows[*a], &rows[*b], sort_by));
            }
        }
    }
    let mut out: Vec<Map<String, Value>> = Vec::with_capacity(indices.len());
    for idx in indices {
        out.push(std::mem::take(&mut rows[idx]));
    }
    (out, duplicate_rows_removed)
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
    let filtered_rows = quality
        .get("filtered_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let duplicate_rows_removed = quality
        .get("duplicate_rows_removed")
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
    if let Some(max_filtered_rows) = gates.get("max_filtered_rows").and_then(|v| v.as_u64())
        && filtered_rows > max_filtered_rows
    {
        errors.push(format!(
            "filtered_rows={} exceeds max_filtered_rows={}",
            filtered_rows, max_filtered_rows
        ));
    }
    if let Some(max_duplicate_rows_removed) = gates
        .get("max_duplicate_rows_removed")
        .and_then(|v| v.as_u64())
        && duplicate_rows_removed > max_duplicate_rows_removed
    {
        errors.push(format!(
            "duplicate_rows_removed={} exceeds max_duplicate_rows_removed={}",
            duplicate_rows_removed, max_duplicate_rows_removed
        ));
    }
    if let Some(allow_empty_output) = gates.get("allow_empty_output").and_then(|v| v.as_bool())
        && !allow_empty_output
        && output_rows == 0
    {
        errors.push("output_rows=0 while allow_empty_output=false".to_string());
    }
    json!({
        "passed": errors.is_empty(),
        "errors": errors,
    })
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
        let tl = t.to_lowercase();
        if tl.contains("references")
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
        }
    }

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
            warning: Some(
                "forced placeholder mode by AIWF_ACCEL_OFFICE_FORCE_PLACEHOLDER=true".to_string(),
            ),
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
                    warning: Some(format!(
                        "python office generation failed, used placeholders: {e}"
                    )),
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
    let py = find_python_command()
        .ok_or_else(|| "python runtime not found for office generation".to_string())?;

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
        return Err(format!(
            "python office generation failed; stdout={stdout}; stderr={stderr}"
        ));
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
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("read for hash: {e}"))?;
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
        if let Ok(meta) = fs::metadata(uri)
            && meta.len() as usize > max_bytes
        {
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

fn load_jsonl_rows_limited(
    path: &str,
    limit: usize,
    max_bytes: usize,
) -> Result<Vec<Value>, String> {
    if let Ok(meta) = fs::metadata(path)
        && meta.len() as usize > max_bytes
    {
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
    if let Ok(meta) = fs::metadata(path)
        && meta.len() as usize > max_bytes
    {
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
    let cols: Vec<String> = header
        .trim_end()
        .split(',')
        .map(|x| x.trim().to_string())
        .collect();
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
                        .unwrap_or_else(|| {
                            Value::String(String::from_utf8_lossy(b.data()).to_string())
                        });
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
    let safe_query = validate_readonly_query(query)?;
    let conn = SqliteConnection::open(db_path).map_err(|e| format!("sqlite open: {e}"))?;
    let q = format!("{safe_query} LIMIT {}", limit);
    let mut stmt = conn
        .prepare(&q)
        .map_err(|e| format!("sqlite prepare: {e}"))?;
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
    let safe_query = validate_readonly_query(query)?;
    let cfg = parse_sqlserver_conn_str(conn_str);
    let q = format!("SET NOCOUNT ON; SELECT TOP {limit} * FROM ({safe_query}) x FOR JSON PATH;");
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

fn parquet_compression_from_name(name: &str) -> Result<Compression, String> {
    match name.trim().to_lowercase().as_str() {
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        other => Err(format!("unsupported parquet compression: {other}")),
    }
}

fn save_rows_parquet_payload(path: &str, rows: &[Value]) -> Result<(), String> {
    save_rows_parquet_payload_with_compression(path, rows, Compression::SNAPPY)
}

fn save_rows_parquet_payload_with_compression(
    path: &str,
    rows: &[Value],
    compression: Compression,
) -> Result<(), String> {
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
            .set_compression(compression)
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
    save_rows_parquet_typed_with_compression(path, rows, Compression::SNAPPY)
}

fn save_rows_parquet_typed_with_compression(
    path: &str,
    rows: &[Value],
    compression: Compression,
) -> Result<(), String> {
    let specs = infer_typed_parquet_columns(rows);
    if specs.is_empty() {
        return save_rows_parquet_payload_with_compression(path, rows, compression);
    }
    let mut fields = Vec::new();
    for c in &specs {
        let ty = match c.kind {
            TypedColKind::Bool => PhysicalType::BOOLEAN,
            TypedColKind::Int => PhysicalType::INT64,
            TypedColKind::Float => PhysicalType::DOUBLE,
            TypedColKind::Str => PhysicalType::BYTE_ARRAY,
        };
        fields.push(Arc::new(if c.kind == TypedColKind::Str {
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
        }));
    }
    let schema = Arc::new(
        Type::group_type_builder("aiwf_rows_typed")
            .with_fields(fields)
            .build()
            .map_err(|e| format!("build parquet typed schema: {e}"))?,
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(compression)
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
                    if let Some(v) = r
                        .as_object()
                        .and_then(|o| o.get(&c.name))
                        .and_then(|v| v.as_bool())
                    {
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
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| format!("sqlite tx: {e}"))?;
    for r in rows {
        let s = serde_json::to_string(r).map_err(|e| e.to_string())?;
        tx.execute(&format!("INSERT INTO {table}(payload) VALUES (?1)"), [&s])
            .map_err(|e| format!("sqlite insert: {e}"))?;
    }
    tx.commit().map_err(|e| format!("sqlite commit: {e}"))
}

fn save_rows_sqlserver(conn_str: &str, table: &str, rows: &[Value]) -> Result<(), String> {
    let cfg = parse_sqlserver_conn_str(conn_str);
    let q_create = format!(
        "IF OBJECT_ID('{table}','U') IS NULL CREATE TABLE {table}(payload NVARCHAR(MAX) NOT NULL);"
    );
    let _ = run_sqlcmd_query(&cfg, &q_create)?;
    for r in rows {
        let payload = escape_tsql(&serde_json::to_string(r).map_err(|e| e.to_string())?);
        let q = format!("INSERT INTO {table}(payload) VALUES (N'{payload}');");
        let _ = run_sqlcmd_query(&cfg, &q)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AggregateRowsReq, AggregateRowsV2Req, AggregateRowsV3Req, AggregateRowsV4Req,
        AnomalyExplainReq, AppState, CleanRow, ColumnarEvalV1Req, EvidenceRankReq,
        ExplainPlanV1Req, FactCrosscheckReq, FinanceRatioReq, JoinRowsV2Req, JoinRowsV3Req,
        JoinRowsV4Req, LineageV3Req, LoadRowsV3Req, OptimizerV1Req, ParquetIoV2Req, PluginExecReq,
        PluginOperatorV1Req, PluginRegistryV1Req, ProvenanceSignReq, QualityCheckV3Req,
        QualityCheckV4Req, QueryLangReq, RulesPackageGetReq, RulesPackagePublishReq,
        RuntimeStatsV1Req, SchemaCompatReq, SchemaGetReq, SchemaInferReq,
        SchemaMigrationSuggestReq, ServiceMetrics, SketchV1Req, StreamStateLoadReq,
        StreamStateSaveReq, StreamStateV2Req, StreamWindowV1Req, StreamWindowV2Req, TaskState,
        TaskStoreConfig, TemplateBindReq, TimeSeriesForecastReq, TransformCacheEntry,
        TransformRowsReq, TransformRowsV3Req, UdfWasmReq, UdfWasmV2Req, VectorIndexBuildReq,
        VectorIndexSearchReq, WindowRowsV1Req, WorkflowRunReq, build_router, can_cancel_status,
        evaluate_quality_gates, load_and_clean_rows, load_parquet_rows, load_rows_from_uri_limited,
        prune_tasks, run_aggregate_rows_v1, run_aggregate_rows_v2, run_aggregate_rows_v3,
        run_aggregate_rows_v4, run_anomaly_explain_v1, run_columnar_eval_v1, run_evidence_rank_v1,
        run_explain_plan_v1, run_fact_crosscheck_v1, run_finance_ratio_v1, run_join_rows_v2,
        run_join_rows_v3, run_join_rows_v4, run_lineage_v3, run_load_rows_v3, run_optimizer_v1,
        run_parquet_io_v2, run_plugin_exec_v1, run_plugin_operator_v1, run_plugin_registry_v1,
        run_provenance_sign_v1, run_quality_check_v1, run_quality_check_v3, run_quality_check_v4,
        run_query_lang_v1, run_rules_package_get_v1, run_rules_package_publish_v1,
        run_runtime_stats_v1, run_schema_registry_check_compat_v2, run_schema_registry_get_v1,
        run_schema_registry_infer_v1, run_schema_registry_suggest_migration_v2, run_sketch_v1,
        run_stream_state_load_v1, run_stream_state_save_v1, run_stream_state_v2,
        run_stream_window_v1, run_stream_window_v2, run_template_bind_v1,
        run_timeseries_forecast_v1, run_transform_rows_v2, run_transform_rows_v2_with_cache,
        run_transform_rows_v2_with_cancel, run_transform_rows_v3, run_udf_wasm_v1, run_udf_wasm_v2,
        run_vector_index_build_v1, run_vector_index_search_v1, run_window_rows_v1, run_workflow,
        save_rows_parquet, utc_now_iso, validate_where_clause, value_to_string,
        write_cleaned_parquet,
    };
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use serde_json::{Map, Value, json};
    use std::{
        collections::HashMap,
        fs,
        io::Write,
        sync::{Arc, Mutex, atomic::AtomicBool},
        time::{Instant, SystemTime, UNIX_EPOCH},
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
            transform_cache: Arc::new(Mutex::new(HashMap::new())),
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
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
            transform_cache: Arc::new(Mutex::new(HashMap::new())),
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
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
            transform_cache: Arc::new(Mutex::new(HashMap::new())),
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
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
            transform_cache: Arc::new(Mutex::new(HashMap::new())),
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
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
        assert!(
            out.steps[0]
                .error
                .as_deref()
                .unwrap_or("")
                .contains("unsupported")
        );
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
        assert!(
            out.report
                .get("violations")
                .and_then(|v| v.as_array())
                .map(|v| !v.is_empty())
                .unwrap_or(false)
        );
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

    #[test]
    fn transform_rows_v2_cache_hits_on_same_request() {
        let req = TransformRowsReq {
            run_id: Some("cache-1".to_string()),
            tenant_id: Some("default".to_string()),
            trace_id: None,
            traceparent: None,
            rows: Some(vec![json!({"id":"1","amount":"10.1"})]),
            rules: Some(json!({"casts":{"id":"int","amount":"float"}})),
            quality_gates: Some(json!({})),
            schema_hint: None,
            rules_dsl: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: None,
        };
        let cache = Arc::new(Mutex::new(HashMap::<String, TransformCacheEntry>::new()));
        let metrics = Arc::new(Mutex::new(ServiceMetrics::default()));
        let first =
            run_transform_rows_v2_with_cache(req.clone(), None, Some(&cache), Some(&metrics))
                .expect("first transform");
        let second = run_transform_rows_v2_with_cache(req, None, Some(&cache), Some(&metrics))
            .expect("second transform");
        assert!(first.ok && second.ok);
        let hit = second
            .audit
            .get("cache")
            .and_then(|v| v.get("hit"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(hit);
    }

    #[test]
    fn transform_rows_v2_supports_columnar_engine_flag() {
        let req = TransformRowsReq {
            run_id: Some("col-1".to_string()),
            tenant_id: None,
            trace_id: None,
            traceparent: None,
            rows: Some(vec![
                json!({"id":"1","amount":"10.0","currency":"cny"}),
                json!({"id":"2","amount":"-1","currency":"cny"}),
            ]),
            rules: Some(json!({
                "execution_engine":"columnar_v1",
                "casts":{"id":"int","amount":"float","currency":"string"},
                "filters":[{"field":"amount","op":"gte","value":0}]
            })),
            quality_gates: Some(json!({"min_output_rows":1})),
            schema_hint: None,
            rules_dsl: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: None,
        };
        let out = run_transform_rows_v2(req).expect("columnar run");
        let engine = out
            .audit
            .get("engine")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert_eq!(engine, "columnar_v1");
        assert_eq!(out.stats.output_rows, 1);
    }

    #[test]
    fn columnar_dedup_and_sort_match_expectation() {
        let req = TransformRowsReq {
            run_id: Some("col-dedup-sort".to_string()),
            tenant_id: None,
            trace_id: None,
            traceparent: None,
            rows: Some(vec![
                json!({"id":"2","amount":"20","city":"bj"}),
                json!({"id":"1","amount":"10","city":"sh"}),
                json!({"id":"2","amount":"25","city":"bj"}),
            ]),
            rules: Some(json!({
                "execution_engine":"columnar_v1",
                "casts":{"id":"int","amount":"float"},
                "deduplicate_by":["id"],
                "deduplicate_keep":"last",
                "sort_by":[{"field":"id","order":"asc"}]
            })),
            quality_gates: Some(json!({"min_output_rows":1})),
            schema_hint: None,
            rules_dsl: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: None,
        };
        let out = run_transform_rows_v2(req).expect("columnar dedup sort");
        assert_eq!(out.stats.output_rows, 2);
        let rows = out.rows;
        assert_eq!(
            rows[0]
                .get("id")
                .and_then(|v| v.as_i64())
                .unwrap_or_default(),
            1
        );
        assert_eq!(
            rows[1]
                .get("id")
                .and_then(|v| v.as_i64())
                .unwrap_or_default(),
            2
        );
        assert_eq!(
            rows[1]
                .get("amount")
                .and_then(|v| v.as_f64())
                .unwrap_or_default(),
            25.0
        );
    }

    #[test]
    fn auto_engine_selects_columnar_for_medium_payload() {
        let mut rows = Vec::new();
        for i in 0..25000 {
            rows.push(json!({"id": i, "amount": "10.5"}));
        }
        let req = TransformRowsReq {
            run_id: Some("auto-eng-1".to_string()),
            tenant_id: None,
            trace_id: None,
            traceparent: None,
            rows: Some(rows),
            rules: Some(json!({
                "execution_engine":"auto_v1",
                "casts":{"id":"int","amount":"float"},
                "filters":[{"field":"amount","op":"gte","value":0}]
            })),
            quality_gates: Some(json!({"min_output_rows":1})),
            schema_hint: None,
            rules_dsl: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: None,
        };
        let out = run_transform_rows_v2(req).expect("auto engine run");
        let eng = out
            .audit
            .get("engine")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert!(eng == "columnar_v1" || eng == "columnar_arrow_v1");
        let reason = out
            .audit
            .get("engine_reason")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert!(reason.contains("auto:"));
    }

    #[test]
    fn quality_gates_support_filtered_and_empty_constraints() {
        let quality = json!({
            "input_rows": 10,
            "output_rows": 0,
            "invalid_rows": 1,
            "filtered_rows": 6,
            "duplicate_rows_removed": 2,
            "required_missing_ratio": 0.0
        });
        let gates = json!({
            "max_filtered_rows": 5,
            "allow_empty_output": false
        });
        let out = evaluate_quality_gates(&quality, &gates);
        let passed = out.get("passed").and_then(|v| v.as_bool()).unwrap_or(true);
        assert!(!passed);
        let errors = out
            .get("errors")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(value_to_string)
            .collect::<Vec<String>>()
            .join(";");
        assert!(errors.contains("max_filtered_rows"));
        assert!(errors.contains("allow_empty_output"));
    }

    #[test]
    fn join_rows_v2_supports_multi_key_and_full_join() {
        let out = run_join_rows_v2(JoinRowsV2Req {
            run_id: Some("jv2-1".to_string()),
            left_rows: vec![
                json!({"id":1,"k":"a","lv":10}),
                json!({"id":2,"k":"b","lv":20}),
            ],
            right_rows: vec![
                json!({"rid":9,"k":"a","rv":99}),
                json!({"rid":8,"k":"c","rv":88}),
            ],
            left_on: json!(["k"]),
            right_on: json!(["k"]),
            join_type: Some("full".to_string()),
        })
        .expect("join v2");
        assert!(out.rows.len() >= 3);
    }

    #[test]
    fn aggregate_rows_v2_supports_stddev_and_percentile() {
        let out = run_aggregate_rows_v2(AggregateRowsV2Req {
            run_id: Some("agv2-1".to_string()),
            rows: vec![
                json!({"g":"x","amount":10.0}),
                json!({"g":"x","amount":20.0}),
                json!({"g":"x","amount":30.0}),
            ],
            group_by: vec!["g".to_string()],
            aggregates: vec![
                json!({"op":"stddev","field":"amount","as":"std"}),
                json!({"op":"percentile_p50","field":"amount","as":"p50"}),
            ],
        })
        .expect("agg v2");
        let row: Map<String, Value> = out
            .rows
            .first()
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        assert!(row.get("std").is_some());
        assert_eq!(
            row.get("p50").and_then(|v| v.as_f64()).unwrap_or_default(),
            20.0
        );
    }

    #[test]
    fn schema_registry_infer_and_get_work() {
        let state = AppState {
            service: "accel-rust".to_string(),
            tasks: Arc::new(Mutex::new(HashMap::<String, TaskState>::new())),
            metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
            task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
                ttl_sec: 3600,
                max_tasks: 1000,
                store_path: None,
                remote_enabled: false,
                backend: "memory".to_string(),
                base_api_url: None,
                base_api_key: None,
                sql_host: "localhost".to_string(),
                sql_port: 1433,
                sql_db: "master".to_string(),
                sql_user: None,
                sql_password: None,
                sql_use_windows_auth: true,
            })),
            cancel_flags: Arc::new(Mutex::new(HashMap::new())),
            tenant_running: Arc::new(Mutex::new(HashMap::new())),
            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
            transform_cache: Arc::new(Mutex::new(HashMap::new())),
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
        };
        let infer = run_schema_registry_infer_v1(
            &state,
            SchemaInferReq {
                name: Some("orders".to_string()),
                version: Some("v1".to_string()),
                rows: vec![json!({"id":"1","amount":"12.3","active":"true"})],
            },
        )
        .expect("infer");
        assert_eq!(infer.status, "done");
        let got = run_schema_registry_get_v1(
            &state,
            SchemaGetReq {
                name: "orders".to_string(),
                version: "v1".to_string(),
            },
        )
        .expect("get schema");
        assert!(got.schema.get("id").is_some());
    }

    #[test]
    fn transform_rows_v2_supports_computed_fields() {
        let req = TransformRowsReq {
            run_id: Some("expr-1".to_string()),
            tenant_id: None,
            trace_id: None,
            traceparent: None,
            rows: Some(vec![json!({"price":"2","qty":"3","name":" A "})]),
            rules: Some(json!({
                "casts":{"price":"float","qty":"int","name":"string"},
                "computed_fields":{"total":"mul($price,$qty)"},
                "string_ops":[{"field":"name","op":"trim"},{"field":"name","op":"upper"}]
            })),
            quality_gates: Some(json!({"min_output_rows":1})),
            schema_hint: None,
            rules_dsl: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: None,
        };
        let out = run_transform_rows_v2(req).expect("expr transform");
        let row = out
            .rows
            .first()
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            row.get("total")
                .and_then(|v| v.as_f64())
                .unwrap_or_default(),
            6.0
        );
        assert_eq!(row.get("name").and_then(|v| v.as_str()).unwrap_or(""), "A");
    }

    #[test]
    fn workflow_supports_schema_registry_ops() {
        let run_id = format!(
            "wf-schema-{}",
            utc_now_iso().replace(":", "").replace("-", "")
        );
        let wf = WorkflowRunReq {
            run_id: Some(run_id),
            trace_id: None,
            traceparent: None,
            tenant_id: Some("local".to_string()),
            context: Some(json!({})),
            steps: vec![
                json!({
                    "id":"infer",
                    "operator":"schema_registry_v1_infer",
                    "input":{"name":"wf_orders","version":"v1","rows":[{"id":"1","amount":"12.3"}]}
                }),
                json!({
                    "id":"get",
                    "operator":"schema_registry_v1_get",
                    "input":{"name":"wf_orders","version":"v1"}
                }),
            ],
        };
        let out = run_workflow(wf).expect("workflow schema ops");
        assert!(out.ok);
        let get_step = out
            .context
            .as_object()
            .and_then(|m| m.get("get"))
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            get_step
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "done"
        );
    }

    #[test]
    fn transform_rows_v3_supports_expr_and_lineage() {
        let out = run_transform_rows_v3(TransformRowsV3Req {
            run_id: Some("tv3-1".to_string()),
            tenant_id: None,
            trace_id: None,
            traceparent: None,
            rows: Some(vec![json!({"a":2,"b":3}), json!({"a":1,"b":0})]),
            rules: Some(json!({})),
            rules_dsl: None,
            quality_gates: None,
            schema_hint: None,
            input_uri: None,
            output_uri: None,
            request_signature: None,
            idempotency_key: None,
            computed_fields_v3: Some(vec![
                json!({"name":"c","expr":{"op":"add","args":[{"field":"a"},{"field":"b"}]}}),
            ]),
            filter_expr_v3: Some(json!({"op":"gt","args":[{"field":"a"},{"const":1}]})),
        })
        .expect("transform v3");
        assert_eq!(out.operator, "transform_rows_v3");
        assert_eq!(out.rows.len(), 1);
        let row = out.rows[0].as_object().cloned().unwrap_or_default();
        assert_eq!(row.get("c").and_then(|v| v.as_f64()).unwrap_or(0.0), 5.0);
        let lineage = out
            .audit
            .get("lineage_v3")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        assert!(lineage.get("c").is_some());
    }

    #[test]
    fn join_aggregate_quality_v3_basics_work() {
        let j = run_join_rows_v3(JoinRowsV3Req {
            run_id: Some("j3".to_string()),
            left_rows: vec![json!({"id":"1","x":10}), json!({"id":"2","x":20})],
            right_rows: vec![json!({"id":"1","y":3})],
            left_on: json!(["id"]),
            right_on: json!(["id"]),
            join_type: Some("left".to_string()),
            strategy: Some("auto".to_string()),
            spill_path: None,
            chunk_size: Some(1),
        })
        .expect("join v3");
        assert_eq!(j.operator, "join_rows_v3");
        assert_eq!(j.rows.len(), 2);

        let a = run_aggregate_rows_v3(AggregateRowsV3Req {
            run_id: Some("a3".to_string()),
            rows: vec![
                json!({"g":"x","v":1}),
                json!({"g":"x","v":2}),
                json!({"g":"x","v":2}),
            ],
            group_by: vec!["g".to_string()],
            aggregates: vec![
                json!({"op":"count","as":"n"}),
                json!({"op":"approx_count_distinct","field":"v","as":"d"}),
                json!({"op":"approx_percentile_p50","field":"v","as":"p50"}),
            ],
            approx_sample_size: Some(128),
        })
        .expect("agg v3");
        assert_eq!(a.operator, "aggregate_rows_v3");
        let r = a.rows[0].as_object().cloned().unwrap_or_default();
        assert_eq!(r.get("n").and_then(|v| v.as_u64()).unwrap_or(0), 3);
        assert!(r.get("d").is_some());
        assert!(r.get("p50").is_some());

        let q = run_quality_check_v3(QualityCheckV3Req {
            run_id: Some("q3".to_string()),
            rows: vec![json!({"v":1.0}), json!({"v":2.0}), json!({"v":100.0})],
            rules: json!({
                "anomaly_iqr":[{"field":"v"}],
                "drift_psi":{"field":"v","expected":[1,2,2,1,2,1],"max_psi":0.01}
            }),
        })
        .expect("qc v3");
        assert_eq!(q.operator, "quality_check_v3");
        assert!(!q.passed);
    }

    #[test]
    fn load_v3_schema_v2_udf_v1_basics_work() {
        let now = utc_now_iso().replace(':', "").replace('-', "");
        let p = std::env::temp_dir().join(format!("aiwf_load_v3_{now}.txt"));
        fs::write(&p, "l1\nl2\n").expect("write temp");
        let l = run_load_rows_v3(LoadRowsV3Req {
            source_type: "txt".to_string(),
            source: p.to_string_lossy().to_string(),
            query: None,
            limit: Some(10),
            max_retries: Some(1),
            retry_backoff_ms: Some(10),
            resume_token: Some("r1".to_string()),
            connector_options: Some(json!({"connector":"local"})),
        })
        .expect("load v3");
        assert_eq!(l.operator, "load_rows_v3");
        assert_eq!(l.rows.len(), 2);
        let _ = fs::remove_file(p);

        let state = AppState {
            service: "accel-rust".to_string(),
            tasks: Arc::new(Mutex::new(HashMap::<String, TaskState>::new())),
            metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
            task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
                ttl_sec: 3600,
                max_tasks: 1000,
                store_path: None,
                remote_enabled: false,
                backend: "memory".to_string(),
                base_api_url: None,
                base_api_key: None,
                sql_host: "localhost".to_string(),
                sql_port: 1433,
                sql_db: "master".to_string(),
                sql_user: None,
                sql_password: None,
                sql_use_windows_auth: true,
            })),
            cancel_flags: Arc::new(Mutex::new(HashMap::new())),
            tenant_running: Arc::new(Mutex::new(HashMap::new())),
            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
            transform_cache: Arc::new(Mutex::new(HashMap::new())),
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
        };
        let _ = super::run_schema_registry_register_v1(
            &state,
            super::SchemaRegisterReq {
                name: "s".to_string(),
                version: "v1".to_string(),
                schema: json!({"id":"int","amount":"int"}),
            },
        )
        .expect("register v1");
        let _ = super::run_schema_registry_register_v1(
            &state,
            super::SchemaRegisterReq {
                name: "s".to_string(),
                version: "v2".to_string(),
                schema: json!({"id":"int","amount":"float","extra":"string"}),
            },
        )
        .expect("register v2");
        let cc = run_schema_registry_check_compat_v2(
            &state,
            SchemaCompatReq {
                name: "s".to_string(),
                from_version: "v1".to_string(),
                to_version: "v2".to_string(),
                mode: Some("backward".to_string()),
            },
        )
        .expect("compat");
        assert!(cc.breaking_fields.is_empty());
        let mg = run_schema_registry_suggest_migration_v2(
            &state,
            SchemaMigrationSuggestReq {
                name: "s".to_string(),
                from_version: "v1".to_string(),
                to_version: "v2".to_string(),
            },
        )
        .expect("migration");
        assert!(!mg.steps.is_empty());

        let udf = run_udf_wasm_v1(UdfWasmReq {
            run_id: Some("u1".to_string()),
            rows: vec![json!({"x":3}), json!({"x":7})],
            field: "x".to_string(),
            output_field: "y".to_string(),
            op: Some("double".to_string()),
            wasm_base64: Some("AGFzbQEAAA==".to_string()),
        })
        .expect("udf");
        let out_rows = udf
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(out_rows.len(), 2);
    }

    #[test]
    fn new_ten_ops_basic_work() {
        let ts = super::run_time_series_v1(super::TimeSeriesReq {
            run_id: Some("ts1".to_string()),
            rows: vec![
                json!({"month":"2024-01","v":10}),
                json!({"month":"2024-02","v":12}),
            ],
            time_field: "month".to_string(),
            value_field: "v".to_string(),
            group_by: None,
            window: Some(2),
        })
        .expect("ts");
        assert_eq!(
            ts.get("status").and_then(|v| v.as_str()).unwrap_or(""),
            "done"
        );

        let st = super::run_stats_v1(super::StatsReq {
            run_id: Some("s1".to_string()),
            rows: vec![
                json!({"x":1,"y":2}),
                json!({"x":2,"y":4}),
                json!({"x":3,"y":6}),
            ],
            x_field: "x".to_string(),
            y_field: "y".to_string(),
        })
        .expect("stats");
        assert!(st.get("metrics").is_some());

        let el = super::run_entity_linking_v1(super::EntityLinkReq {
            run_id: None,
            rows: vec![json!({"entity":"Open AI"}), json!({"entity":"Open-AI"})],
            field: "entity".to_string(),
            id_field: Some("eid".to_string()),
        })
        .expect("entity");
        assert_eq!(
            el.get("status").and_then(|v| v.as_str()).unwrap_or(""),
            "done"
        );

        let tr = super::run_table_reconstruct_v1(super::TableReconstructReq {
            run_id: None,
            lines: Some(vec!["a  b  c".to_string(), "1  2  3".to_string()]),
            text: None,
            delimiter: None,
        })
        .expect("table");
        assert!(
            tr.get("rows")
                .and_then(|v| v.as_array())
                .map(|a| !a.is_empty())
                .unwrap_or(false)
        );

        let _ = super::run_feature_store_upsert_v1(super::FeatureStoreUpsertReq {
            run_id: None,
            key_field: "id".to_string(),
            rows: vec![json!({"id":"k1","f":1})],
        })
        .expect("fs upsert");
        let fg = super::run_feature_store_get_v1(super::FeatureStoreGetReq {
            run_id: None,
            key: "k1".to_string(),
        })
        .expect("fs get");
        assert!(fg.get("value").is_some());

        let lg = super::run_lineage_v2(super::LineageV2Req {
            run_id: None,
            rules: Some(json!({"computed_fields":{"total":"mul($price,$qty)"}})),
            computed_fields_v3: None,
        })
        .expect("lineage");
        assert!(lg.get("edges").is_some());

        let rs = super::run_rule_simulator_v1(super::RuleSimulatorReq {
            run_id: None,
            rows: vec![json!({"x":"1"}), json!({"x":"2"})],
            rules: json!({"casts":{"x":"int"}}),
            candidate_rules: json!({"casts":{"x":"int"},"filters":[{"field":"x","op":"gt","value":1}]}),
        })
        .expect("sim");
        assert!(rs.get("delta_rows").is_some());

        let cs = super::run_constraint_solver_v1(super::ConstraintSolverReq {
            run_id: None,
            rows: vec![json!({"a":1,"b":2,"sum":3}), json!({"a":1,"b":2,"sum":9})],
            constraints: vec![json!({"kind":"sum_equals","left":["a","b"],"right":"sum"})],
        })
        .expect("constraint");
        assert!(!cs.get("passed").and_then(|v| v.as_bool()).unwrap_or(true));

        let cp = super::run_chart_data_prep_v1(super::ChartDataPrepReq {
            run_id: None,
            rows: vec![
                json!({"c":"A","s":"S1","v":2}),
                json!({"c":"A","s":"S2","v":3}),
            ],
            category_field: "c".to_string(),
            value_field: "v".to_string(),
            series_field: Some("s".to_string()),
            top_n: Some(10),
        })
        .expect("chart");
        assert!(cp.get("chart").is_some());

        let da = super::run_diff_audit_v1(super::DiffAuditReq {
            run_id: None,
            left_rows: vec![json!({"id":"1","v":1}), json!({"id":"2","v":2})],
            right_rows: vec![json!({"id":"2","v":3}), json!({"id":"3","v":9})],
            keys: vec!["id".to_string()],
        })
        .expect("diff");
        assert_eq!(
            da.get("summary")
                .and_then(|s| s.get("added"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            1
        );
    }

    #[test]
    fn additional_ten_ops_basic_work() {
        let vb = run_vector_index_build_v1(VectorIndexBuildReq {
            run_id: Some("vb1".to_string()),
            rows: vec![
                json!({"id":"d1","text":"alpha beta gamma"}),
                json!({"id":"d2","text":"finance ratio cash flow"}),
            ],
            id_field: "id".to_string(),
            text_field: "text".to_string(),
        })
        .expect("vector build");
        assert_eq!(
            vb.get("status").and_then(|v| v.as_str()).unwrap_or(""),
            "done"
        );
        let vs = run_vector_index_search_v1(VectorIndexSearchReq {
            run_id: Some("vs1".to_string()),
            query: "cash flow".to_string(),
            top_k: Some(1),
        })
        .expect("vector search");
        assert_eq!(
            vs.get("hits")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0),
            1
        );

        let er = run_evidence_rank_v1(EvidenceRankReq {
            run_id: None,
            rows: vec![
                json!({"relevance":0.9,"source_score":0.8,"consistency":0.7,"time":"2025-01-01"}),
            ],
            time_field: Some("time".to_string()),
            source_field: Some("source_score".to_string()),
            relevance_field: Some("relevance".to_string()),
            consistency_field: Some("consistency".to_string()),
        })
        .expect("rank");
        assert!(er.get("rows").is_some());

        let fc = run_fact_crosscheck_v1(FactCrosscheckReq {
            run_id: None,
            rows: vec![
                json!({"claim":"GDP grows 5%","source":"a"}),
                json!({"claim":"GDP grows 5 %","source":"b"}),
            ],
            claim_field: "claim".to_string(),
            source_field: Some("source".to_string()),
        })
        .expect("cross");
        assert!(fc.get("results").is_some());

        let tf = run_timeseries_forecast_v1(TimeSeriesForecastReq {
            run_id: None,
            rows: vec![
                json!({"t":"2024-01","v":10}),
                json!({"t":"2024-02","v":12}),
                json!({"t":"2024-03","v":14}),
            ],
            time_field: "t".to_string(),
            value_field: "v".to_string(),
            horizon: Some(2),
            method: Some("naive_drift".to_string()),
        })
        .expect("forecast");
        assert_eq!(
            tf.get("forecast")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0),
            2
        );

        let fr = run_finance_ratio_v1(FinanceRatioReq {
            run_id: None,
            rows: vec![json!({"current_assets":100,"current_liabilities":50,"total_debt":40,"total_equity":20,"revenue":200,"net_income":20,"operating_cash_flow":30})],
        })
        .expect("finance ratio");
        assert!(fr.get("rows").is_some());

        let ax = run_anomaly_explain_v1(AnomalyExplainReq {
            run_id: None,
            rows: vec![
                json!({"score":0.95,"a":10,"b":2}),
                json!({"score":0.2,"a":1,"b":1}),
            ],
            score_field: "score".to_string(),
            threshold: Some(0.9),
        })
        .expect("anomaly explain");
        assert_eq!(
            ax.get("anomalies")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0),
            1
        );

        let tb = run_template_bind_v1(TemplateBindReq {
            run_id: None,
            template_text: "Hello {{user.name}}, score={{score}}".to_string(),
            data: json!({"user":{"name":"AIWF"},"score":99}),
        })
        .expect("bind");
        assert!(
            tb.get("bound_text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .contains("AIWF")
        );

        let ps = run_provenance_sign_v1(ProvenanceSignReq {
            run_id: None,
            payload: json!({"k":"v"}),
            prev_hash: Some("abc".to_string()),
        })
        .expect("sign");
        assert!(ps.get("record").and_then(|v| v.get("hash")).is_some());

        let _ = run_stream_state_save_v1(StreamStateSaveReq {
            run_id: None,
            stream_key: "s1".to_string(),
            state: json!({"x":1}),
            offset: Some(10),
        })
        .expect("stream save");
        let sl = run_stream_state_load_v1(StreamStateLoadReq {
            run_id: None,
            stream_key: "s1".to_string(),
        })
        .expect("stream load");
        assert!(sl.get("value").is_some());

        let ql = run_query_lang_v1(QueryLangReq {
            run_id: None,
            rows: vec![json!({"a":1,"b":2}), json!({"a":3,"b":4})],
            query: "where a > 1".to_string(),
        })
        .expect("query");
        assert_eq!(
            ql.get("rows")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0),
            1
        );
    }

    #[test]
    fn window_optimizer_and_explain_v1_work() {
        let win = run_window_rows_v1(WindowRowsV1Req {
            run_id: Some("w1".to_string()),
            rows: vec![
                json!({"g":"A","t":"2024-01","v":10}),
                json!({"g":"A","t":"2024-02","v":20}),
                json!({"g":"A","t":"2024-03","v":30}),
            ],
            partition_by: Some(vec!["g".to_string()]),
            order_by: "t".to_string(),
            functions: vec![
                json!({"op":"row_number","as":"rn"}),
                json!({"op":"lag","field":"v","as":"prev_v","offset":1}),
                json!({"op":"moving_avg","field":"v","as":"ma2","window":2}),
            ],
        })
        .expect("window");
        assert_eq!(
            win.get("rows")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0),
            3
        );

        let opt = run_optimizer_v1(OptimizerV1Req {
            run_id: Some("o1".to_string()),
            rows: None,
            row_count_hint: Some(150_000),
            prefer_arrow: Some(true),
            join_hint: None,
            aggregate_hint: None,
        })
        .expect("optimizer");
        assert_eq!(
            opt.get("plan")
                .and_then(|p| p.get("execution_engine"))
                .and_then(|v| v.as_str()),
            Some("columnar_arrow_v1")
        );

        let exp = run_explain_plan_v1(ExplainPlanV1Req {
            run_id: Some("e1".to_string()),
            rows: Some(vec![json!({"id":1}), json!({"id":2})]),
            steps: vec![
                json!({"operator":"load_rows_v3"}),
                json!({"operator":"join_rows_v4"}),
                json!({"operator":"aggregate_rows_v4"}),
            ],
            actual_stats: None,
            persist_feedback: None,
        })
        .expect("explain");
        assert!(
            exp.get("estimated_total_cost")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0)
                > 0.0
        );
    }

    #[test]
    fn join_aggregate_quality_lineage_v4_family_work() {
        let j = run_join_rows_v4(JoinRowsV4Req {
            run_id: Some("j4".to_string()),
            left_rows: vec![
                json!({"id":"1","v":10}),
                json!({"id":"2","v":20}),
                json!({"id":"x","v":999}),
            ],
            right_rows: vec![json!({"id":"1","r":"A"}), json!({"id":"2","r":"B"})],
            left_on: json!(["id"]),
            right_on: json!(["id"]),
            join_type: Some("inner".to_string()),
            strategy: None,
            spill_path: None,
            chunk_size: None,
            enable_bloom: Some(true),
            bloom_field: None,
        })
        .expect("join4");
        assert_eq!(j.operator, "join_rows_v4");
        assert!(j.rows.len() >= 2);

        let a = run_aggregate_rows_v4(AggregateRowsV4Req {
            run_id: Some("a4".to_string()),
            rows: vec![
                json!({"g":"A","x":1}),
                json!({"g":"A","x":2}),
                json!({"g":"B","x":3}),
            ],
            group_by: vec!["g".to_string()],
            aggregates: vec![json!({"op":"count","as":"cnt"})],
            approx_sample_size: Some(128),
            verify_exact: Some(true),
            parallel_workers: None,
        })
        .expect("agg4");
        assert_eq!(a.operator, "aggregate_rows_v4");

        let q = run_quality_check_v4(QualityCheckV4Req {
            run_id: Some("q4".to_string()),
            rows: vec![
                json!({"v":1}),
                json!({"v":2}),
                json!({"v":3}),
                json!({"v":1000}),
            ],
            rules: json!({"anomaly_iqr":{"field":"v","max_ratio":0.20}}),
            rules_dsl: None,
        })
        .expect("qc4");
        assert_eq!(q.operator, "quality_check_v4");
        assert!(q.report.get("anomaly_iqr").is_some());

        let l = run_lineage_v3(LineageV3Req {
            run_id: Some("l3".to_string()),
            rules: Some(json!({})),
            computed_fields_v3: Some(vec![
                json!({"name":"z","expr":{"op":"add","args":[{"field":"a"},{"field":"b"}]}}),
            ]),
            workflow_steps: Some(vec![
                json!({"id":"s2","depends_on":["s1"],"operator":"transform_rows_v3"}),
            ]),
            rows: Some(vec![json!({"a":1,"b":2})]),
        })
        .expect("lineage3");
        assert_eq!(
            l.get("operator").and_then(|v| v.as_str()),
            Some("lineage_v3")
        );
        assert!(l.get("step_lineage").is_some());
    }

    #[test]
    fn parquet_stream_udf_v2_work() {
        let now = utc_now_iso();
        let path = std::env::temp_dir().join(format!("aiwf_parquet_v2_{now}.parquet"));
        let p = path.to_string_lossy().to_string();
        let stream_key = format!("k1_{now}");
        let w = run_parquet_io_v2(ParquetIoV2Req {
            run_id: Some("p4".to_string()),
            op: "write".to_string(),
            path: p.clone(),
            rows: Some(vec![json!({"id":1,"txt":"a"}), json!({"id":2,"txt":"b"})]),
            parquet_mode: Some("typed".to_string()),
            limit: None,
            columns: None,
            predicate_field: None,
            predicate_eq: None,
            partition_by: None,
            compression: Some("gzip".to_string()),
            recursive: None,
            schema_mode: None,
        })
        .expect("parquet write");
        assert_eq!(w.get("ok").and_then(|v| v.as_bool()), Some(true));

        let r = run_parquet_io_v2(ParquetIoV2Req {
            run_id: Some("p4".to_string()),
            op: "read".to_string(),
            path: p.clone(),
            rows: None,
            parquet_mode: None,
            limit: Some(10),
            columns: Some(vec!["id".to_string()]),
            predicate_field: Some("id".to_string()),
            predicate_eq: Some(json!(2)),
            partition_by: None,
            compression: None,
            recursive: None,
            schema_mode: None,
        })
        .expect("parquet read");
        assert_eq!(
            r.get("rows")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0),
            1
        );

        let _ = run_stream_state_v2(StreamStateV2Req {
            run_id: Some("s2".to_string()),
            op: "delete".to_string(),
            stream_key: stream_key.clone(),
            state: None,
            offset: None,
            checkpoint_version: None,
            expected_version: None,
            backend: None,
            db_path: None,
            event_ts_ms: None,
            max_late_ms: None,
        });

        let s1 = run_stream_state_v2(StreamStateV2Req {
            run_id: Some("s2".to_string()),
            op: "save".to_string(),
            stream_key: stream_key.clone(),
            state: Some(json!({"x":1})),
            offset: Some(10),
            checkpoint_version: None,
            expected_version: Some(0),
            backend: None,
            db_path: None,
            event_ts_ms: None,
            max_late_ms: None,
        })
        .expect("stream save");
        assert_eq!(s1.get("version").and_then(|v| v.as_u64()), Some(1));
        let s2 = run_stream_state_v2(StreamStateV2Req {
            run_id: Some("s2".to_string()),
            op: "load".to_string(),
            stream_key: stream_key.clone(),
            state: None,
            offset: None,
            checkpoint_version: None,
            expected_version: None,
            backend: None,
            db_path: None,
            event_ts_ms: None,
            max_late_ms: None,
        })
        .expect("stream load");
        assert!(s2.get("value").is_some());

        let u = run_udf_wasm_v2(UdfWasmV2Req {
            run_id: Some("u2".to_string()),
            rows: vec![json!({"x":2}), json!({"x":3})],
            field: "x".to_string(),
            output_field: "y".to_string(),
            op: Some("double".to_string()),
            wasm_base64: Some("AGFzbQEAAA==".to_string()),
            max_output_bytes: Some(200_000),
            signed_token: None,
            allowed_ops: Some(vec!["double".to_string(), "identity".to_string()]),
        })
        .expect("udf2");
        assert_eq!(
            u.get("operator").and_then(|v| v.as_str()),
            Some("udf_wasm_v2")
        );

        let _ = run_stream_state_v2(StreamStateV2Req {
            run_id: Some("s2".to_string()),
            op: "delete".to_string(),
            stream_key,
            state: None,
            offset: None,
            checkpoint_version: None,
            expected_version: None,
            backend: None,
            db_path: None,
            event_ts_ms: None,
            max_late_ms: None,
        });
        let _ = fs::remove_file(path);
    }

    #[test]
    fn aggregate_v4_parallel_and_approx_ops_work() {
        let rows = vec![
            json!({"g":"A","v":1.0,"k":"x"}),
            json!({"g":"A","v":2.0,"k":"x"}),
            json!({"g":"A","v":3.0,"k":"y"}),
            json!({"g":"B","v":10.0,"k":"m"}),
            json!({"g":"B","v":20.0,"k":"m"}),
        ];
        let out = run_aggregate_rows_v4(AggregateRowsV4Req {
            run_id: Some("agg-par".to_string()),
            rows,
            group_by: vec!["g".to_string()],
            aggregates: vec![
                json!({"op":"count","as":"cnt"}),
                json!({"op":"hll_count","field":"k","as":"hll"}),
                json!({"op":"tdigest_p90","field":"v","as":"p90"}),
                json!({"op":"topk_2","field":"k","as":"topk"}),
            ],
            approx_sample_size: Some(128),
            verify_exact: Some(false),
            parallel_workers: Some(2),
        })
        .expect("aggregate v4 parallel");
        assert_eq!(out.operator, "aggregate_rows_v4");
        assert_eq!(out.rows.len(), 2);
        assert_eq!(
            out.stats
                .get("parallel_workers")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            2
        );
    }

    #[test]
    fn quality_dsl_and_stream_sqlite_and_plugin_registry_work() {
        let q = run_quality_check_v4(QualityCheckV4Req {
            run_id: Some("q-dsl".to_string()),
            rows: vec![
                json!({"id":"1","amount":10.0}),
                json!({"id":"1","amount":-2.0}),
            ],
            rules: json!({}),
            rules_dsl: Some("required:id,amount\nunique:id\nrange: amount >=0<=100".to_string()),
        })
        .expect("quality dsl");
        assert_eq!(q.operator, "quality_check_v4");
        assert!(!q.passed);

        let now = utc_now_iso();
        let sqlite = std::env::temp_dir().join(format!("aiwf_stream_{now}.sqlite"));
        let sqlite_path = sqlite.to_string_lossy().to_string();
        let s1 = run_stream_state_v2(StreamStateV2Req {
            run_id: Some("s-sqlite".to_string()),
            op: "save".to_string(),
            stream_key: "k-sqlite".to_string(),
            state: Some(json!({"x":1})),
            offset: Some(5),
            checkpoint_version: None,
            expected_version: Some(0),
            backend: Some("sqlite".to_string()),
            db_path: Some(sqlite_path.clone()),
            event_ts_ms: None,
            max_late_ms: None,
        })
        .expect("stream sqlite save");
        assert_eq!(s1.get("backend").and_then(|v| v.as_str()), Some("sqlite"));

        let s2 = run_stream_state_v2(StreamStateV2Req {
            run_id: Some("s-sqlite".to_string()),
            op: "load".to_string(),
            stream_key: "k-sqlite".to_string(),
            state: None,
            offset: None,
            checkpoint_version: None,
            expected_version: None,
            backend: Some("sqlite".to_string()),
            db_path: Some(sqlite_path.clone()),
            event_ts_ms: None,
            max_late_ms: None,
        })
        .expect("stream sqlite load");
        assert!(s2.get("value").is_some());
        let _ = fs::remove_file(sqlite);

        let plugin = run_plugin_registry_v1(PluginRegistryV1Req {
            run_id: Some("p-reg".to_string()),
            op: "register".to_string(),
            plugin: Some("demo_reg".to_string()),
            manifest: Some(json!({"name":"demo_reg","api_version":"v1","command":"cmd","args":[],"version":"1.0.0"})),
        })
        .expect("plugin register");
        assert_eq!(plugin.get("ok").and_then(|v| v.as_bool()), Some(true));
    }

    #[test]
    fn columnar_stream_sketch_runtime_and_explain_feedback_work() {
        let c = run_columnar_eval_v1(ColumnarEvalV1Req {
            run_id: Some("c1".to_string()),
            rows: vec![
                json!({"id":"1","k":"A"}),
                json!({"id":"2","k":"B"}),
                json!({"id":"3","k":"A"}),
            ],
            select_fields: Some(vec!["id".to_string()]),
            filter_eq: Some(json!({"k":"A"})),
            limit: Some(10),
        })
        .expect("columnar");
        assert_eq!(
            c.get("operator").and_then(|v| v.as_str()),
            Some("columnar_eval_v1")
        );
        assert_eq!(
            c.get("rows").and_then(|v| v.as_array()).map(|a| a.len()),
            Some(2)
        );

        let now_ms = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()) as i64;
        let sw = run_stream_window_v1(StreamWindowV1Req {
            run_id: Some("sw1".to_string()),
            stream_key: "demo".to_string(),
            rows: vec![
                json!({"ts": now_ms - 1000, "value": 10, "g":"x"}),
                json!({"ts": now_ms - 500, "value": 20, "g":"x"}),
                json!({"ts": now_ms - 120000, "value": 30, "g":"x"}),
            ],
            event_time_field: "ts".to_string(),
            window_ms: 60_000,
            watermark_ms: Some(60_000),
            group_by: Some(vec!["g".to_string()]),
            value_field: Some("value".to_string()),
            trigger: Some("on_watermark".to_string()),
        })
        .expect("stream window");
        assert_eq!(
            sw.get("operator").and_then(|v| v.as_str()),
            Some("stream_window_v1")
        );
        assert_eq!(
            sw.get("stats")
                .and_then(|v| v.get("dropped_late"))
                .and_then(|v| v.as_u64()),
            Some(1)
        );

        let sw2 = run_stream_window_v2(StreamWindowV2Req {
            run_id: Some("sw2".to_string()),
            stream_key: "demo2".to_string(),
            rows: vec![
                json!({"ts": now_ms - 5000, "value": 10, "g":"x"}),
                json!({"ts": now_ms - 3000, "value": 20, "g":"x"}),
                json!({"ts": now_ms - 1000, "value": 30, "g":"x"}),
            ],
            event_time_field: "ts".to_string(),
            window_type: Some("sliding".to_string()),
            window_ms: 4000,
            slide_ms: Some(2000),
            session_gap_ms: None,
            watermark_ms: Some(60000),
            allowed_lateness_ms: Some(60000),
            group_by: Some(vec!["g".to_string()]),
            value_field: Some("value".to_string()),
            trigger: Some("on_watermark".to_string()),
            emit_late_side: Some(true),
        })
        .expect("stream window v2");
        assert_eq!(
            sw2.get("operator").and_then(|v| v.as_str()),
            Some("stream_window_v2")
        );

        let sk = run_sketch_v1(SketchV1Req {
            run_id: Some("sk1".to_string()),
            op: "create".to_string(),
            kind: Some("topk".to_string()),
            state: Some(json!({})),
            rows: Some(vec![json!({"k":"a"}), json!({"k":"a"}), json!({"k":"b"})]),
            field: Some("k".to_string()),
            topk_n: Some(2),
            merge_state: None,
        })
        .expect("sketch");
        assert_eq!(
            sk.get("operator").and_then(|v| v.as_str()),
            Some("sketch_v1")
        );

        let _ = run_runtime_stats_v1(RuntimeStatsV1Req {
            run_id: Some("rs1".to_string()),
            op: "record".to_string(),
            operator: Some("demo_op".to_string()),
            ok: Some(true),
            error_code: None,
            duration_ms: Some(12),
            rows_in: Some(10),
            rows_out: Some(9),
        })
        .expect("stats record");
        let sm = run_runtime_stats_v1(RuntimeStatsV1Req {
            run_id: Some("rs1".to_string()),
            op: "summary".to_string(),
            operator: None,
            ok: None,
            error_code: None,
            duration_ms: None,
            rows_in: None,
            rows_out: None,
        })
        .expect("stats summary");
        assert!(sm.get("items").and_then(|v| v.as_array()).is_some());

        let e = run_explain_plan_v1(ExplainPlanV1Req {
            run_id: Some("exp1".to_string()),
            steps: vec![json!({"operator":"join_rows_v4"})],
            rows: Some(vec![json!({"id":1})]),
            actual_stats: Some(vec![
                json!({"operator":"join_rows_v4","estimated_ms":10,"actual_ms":25}),
            ]),
            persist_feedback: Some(true),
        })
        .expect("explain feedback");
        assert_eq!(
            e.get("operator").and_then(|v| v.as_str()),
            Some("explain_plan_v1")
        );
    }

    #[test]
    fn plugin_operator_v1_is_wired() {
        let res = run_plugin_operator_v1(PluginOperatorV1Req {
            run_id: Some("po1".to_string()),
            tenant_id: Some("default".to_string()),
            plugin: "demo".to_string(),
            op: Some("run".to_string()),
            payload: Some(json!({"x":1})),
        });
        assert!(res.is_err());
    }

    #[test]
    #[ignore]
    fn benchmark_new_ops_gate() {
        let loops = 10usize;
        let max_col_ms = std::env::var("AIWF_BENCH_MAX_COLUMNAR_MS")
            .ok()
            .and_then(|v| v.parse::<u128>().ok())
            .unwrap_or(1200);
        let max_stream_ms = std::env::var("AIWF_BENCH_MAX_STREAM_WINDOW_MS")
            .ok()
            .and_then(|v| v.parse::<u128>().ok())
            .unwrap_or(1200);
        let max_sketch_ms = std::env::var("AIWF_BENCH_MAX_SKETCH_MS")
            .ok()
            .and_then(|v| v.parse::<u128>().ok())
            .unwrap_or(1200);

        let rows = (0..20_000)
            .map(|i| json!({"id": i.to_string(), "k": if i % 2 == 0 { "A" } else { "B" }, "v": i as f64, "ts": 1_700_000_000_000i64 + i as i64}))
            .collect::<Vec<_>>();

        let t0 = Instant::now();
        for _ in 0..loops {
            let _ = run_columnar_eval_v1(ColumnarEvalV1Req {
                run_id: Some("bench-col".to_string()),
                rows: rows.clone(),
                select_fields: Some(vec!["id".to_string(), "k".to_string()]),
                filter_eq: Some(json!({"k":"A"})),
                limit: Some(5000),
            })
            .expect("bench columnar");
        }
        let col_ms = t0.elapsed().as_millis();

        let t1 = Instant::now();
        for _ in 0..loops {
            let _ = run_stream_window_v1(StreamWindowV1Req {
                run_id: Some("bench-stream".to_string()),
                stream_key: "bench".to_string(),
                rows: rows.clone(),
                event_time_field: "ts".to_string(),
                window_ms: 60_000,
                watermark_ms: Some(120_000),
                group_by: Some(vec!["k".to_string()]),
                value_field: Some("v".to_string()),
                trigger: Some("on_watermark".to_string()),
            })
            .expect("bench stream");
        }
        let stream_ms = t1.elapsed().as_millis();

        let t2 = Instant::now();
        for _ in 0..loops {
            let _ = run_sketch_v1(SketchV1Req {
                run_id: Some("bench-sk".to_string()),
                op: "update".to_string(),
                kind: Some("topk".to_string()),
                state: Some(json!({})),
                rows: Some(rows.clone()),
                field: Some("k".to_string()),
                topk_n: Some(5),
                merge_state: None,
            })
            .expect("bench sketch");
        }
        let sketch_ms = t2.elapsed().as_millis();

        assert!(
            col_ms <= max_col_ms,
            "columnar benchmark too slow: {col_ms}ms > {max_col_ms}ms"
        );
        assert!(
            stream_ms <= max_stream_ms,
            "stream window benchmark too slow: {stream_ms}ms > {max_stream_ms}ms"
        );
        assert!(
            sketch_ms <= max_sketch_ms,
            "sketch benchmark too slow: {sketch_ms}ms > {max_sketch_ms}ms"
        );
    }

    #[tokio::test]
    async fn http_routes_for_new_ops_work() {
        let state = AppState {
            service: "accel-rust".to_string(),
            tasks: Arc::new(Mutex::new(HashMap::<String, TaskState>::new())),
            metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
            task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
                ttl_sec: 3600,
                max_tasks: 1000,
                store_path: None,
                remote_enabled: false,
                backend: "memory".to_string(),
                base_api_url: None,
                base_api_key: None,
                sql_host: "localhost".to_string(),
                sql_port: 1433,
                sql_db: "master".to_string(),
                sql_user: None,
                sql_password: None,
                sql_use_windows_auth: true,
            })),
            cancel_flags: Arc::new(Mutex::new(HashMap::new())),
            tenant_running: Arc::new(Mutex::new(HashMap::new())),
            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
            transform_cache: Arc::new(Mutex::new(HashMap::new())),
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
        };
        let app = build_router(state);

        let req1 = Request::builder()
            .method("POST")
            .uri("/operators/stats_v1")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"rows":[{"x":1,"y":2},{"x":2,"y":4},{"x":3,"y":6}],"x_field":"x","y_field":"y"}).to_string(),
            ))
            .expect("stats req");
        let resp1 = app.clone().oneshot(req1).await.expect("stats resp");
        assert_eq!(resp1.status(), StatusCode::OK);

        let req2 = Request::builder()
            .method("POST")
            .uri("/operators/time_series_v1")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"rows":[{"t":"2024-01","v":1},{"t":"2024-02","v":2}],"time_field":"t","value_field":"v","window":2}).to_string(),
            ))
            .expect("ts req");
        let resp2 = app.clone().oneshot(req2).await.expect("ts resp");
        assert_eq!(resp2.status(), StatusCode::OK);

        let req3 = Request::builder()
            .method("GET")
            .uri("/metrics_v2/prom")
            .body(Body::empty())
            .expect("prom req");
        let resp3 = app.oneshot(req3).await.expect("prom resp");
        assert_eq!(resp3.status(), StatusCode::OK);
    }
}
