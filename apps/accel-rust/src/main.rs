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
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env, fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
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

mod analysis_ops;
mod api_types;
mod cleaning_runtime;
mod execution_ops;
mod governance_ops;
mod http;
mod load_ops;
mod misc_ops;
mod operators;
mod platform_ops;
mod plugin_runtime;
mod row_io;
mod schema_registry;
mod transform_support;
mod wasm_ops;
#[allow(unused_imports)]
use analysis_ops::*;
#[allow(unused_imports)]
use api_types::*;
#[allow(unused_imports)]
use cleaning_runtime::*;
#[allow(unused_imports)]
use execution_ops::*;
#[allow(unused_imports)]
use governance_ops::*;
#[allow(unused_imports)]
use http::handlers_core::*;
#[allow(unused_imports)]
use http::handlers_extended::*;
use http::routes::build_router;
#[allow(unused_imports)]
use load_ops::*;
#[allow(unused_imports)]
use misc_ops::*;
pub(crate) use operators::analytics::{
    AggregateRowsReq, AggregateRowsV2Req, AggregateRowsV3Req, AggregateRowsV4Req, QualityCheckReq,
    QualityCheckV2Req, QualityCheckV3Req, QualityCheckV4Req, approx_percentile, compute_aggregate,
    parse_agg_specs, run_aggregate_rows_v1, run_aggregate_rows_v2, run_aggregate_rows_v3,
    run_aggregate_rows_v4, run_quality_check_v1, run_quality_check_v2, run_quality_check_v3,
    run_quality_check_v4,
};
pub(crate) use operators::join::{
    JoinRowsReq, JoinRowsV2Req, JoinRowsV3Req, JoinRowsV4Req, run_join_rows_v1, run_join_rows_v2,
    run_join_rows_v3, run_join_rows_v4,
};
pub(crate) use operators::transform::{
    TransformRowsReq, TransformRowsV3Req, collect_expr_lineage, observe_transform_success,
    run_transform_rows_v2, run_transform_rows_v2_with_cache, run_transform_rows_v3,
};
#[cfg(test)]
pub(crate) use operators::workflow::run_workflow;
pub(crate) use operators::workflow::{
    LineageV2Req, LineageV3Req, WorkflowRunReq, run_lineage_v2, run_lineage_v3,
};
#[allow(unused_imports)]
use platform_ops::*;
#[allow(unused_imports)]
use plugin_runtime::*;
#[allow(unused_imports)]
use row_io::*;
#[allow(unused_imports)]
use schema_registry::*;
#[allow(unused_imports)]
use transform_support::*;
#[allow(unused_imports)]
use wasm_ops::*;

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
    let metrics0 = ServiceMetrics {
        operator_latency_samples: load_metrics_v2_samples(),
        ..ServiceMetrics::default()
    };
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
    refresh_remote_task_store_probe_once(state);
    let state = state.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(30)).await;
            refresh_remote_task_store_probe_once_async(&state).await;
        }
    });
}

fn refresh_remote_task_store_probe_metrics(
    metrics: &Arc<Mutex<ServiceMetrics>>,
    enabled: bool,
    ok: Option<bool>,
) {
    if let Ok(mut m) = metrics.lock() {
        if !enabled {
            m.task_store_remote_ok = false;
            m.task_store_remote_last_probe_epoch = 0;
            return;
        }
        let status = ok.unwrap_or(false);
        m.task_store_remote_ok = status;
        m.task_store_remote_last_probe_epoch = unix_now_sec();
        if !status {
            m.task_store_remote_probe_failures += 1;
        }
    }
}

fn refresh_remote_task_store_probe_once_with<F>(state: &AppState, probe: F) -> bool
where
    F: FnOnce(&TaskStoreConfig) -> bool,
{
    let cfg = current_task_cfg(state);
    let enabled = task_store_remote_enabled(&cfg);
    if !enabled {
        refresh_remote_task_store_probe_metrics(&state.metrics, false, None);
        return false;
    }
    let ok = probe(&cfg);
    refresh_remote_task_store_probe_metrics(&state.metrics, true, Some(ok));
    true
}

fn refresh_remote_task_store_probe_once(state: &AppState) -> bool {
    refresh_remote_task_store_probe_once_with(state, probe_remote_task_store)
}

async fn refresh_remote_task_store_probe_once_async(state: &AppState) -> bool {
    let cfg = current_task_cfg(state);
    let enabled = task_store_remote_enabled(&cfg);
    if !enabled {
        refresh_remote_task_store_probe_metrics(&state.metrics, false, None);
        return false;
    }
    let ok = tokio::task::spawn_blocking(move || probe_remote_task_store(&cfg))
        .await
        .unwrap_or(false);
    refresh_remote_task_store_probe_metrics(&state.metrics, true, Some(ok));
    true
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

#[cfg(test)]
mod main_tests;

#[cfg(test)]
mod tests {
    use super::{
        AppState, ServiceMetrics, TaskStoreConfig, refresh_remote_task_store_probe_once_with,
    };
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex, atomic::AtomicBool},
    };

    fn test_state(remote_enabled: bool) -> AppState {
        AppState {
            service: "accel-rust".to_string(),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
            task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
                ttl_sec: 3600,
                max_tasks: 1000,
                store_path: None,
                remote_enabled,
                backend: "sqlcmd".to_string(),
                base_api_url: None,
                base_api_key: None,
                sql_host: "127.0.0.1".to_string(),
                sql_port: 1433,
                sql_db: "AIWF".to_string(),
                sql_user: None,
                sql_password: None,
                sql_use_windows_auth: false,
            })),
            cancel_flags: Arc::new(Mutex::new(HashMap::<String, Arc<AtomicBool>>::new())),
            tenant_running: Arc::new(Mutex::new(HashMap::new())),
            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
            transform_cache: Arc::new(Mutex::new(HashMap::new())),
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[test]
    fn remote_probe_metrics_clear_when_disabled() {
        let state = test_state(false);
        let enabled = refresh_remote_task_store_probe_once_with(&state, |_| true);
        assert!(!enabled);
        let metrics = state.metrics.lock().expect("metrics lock");
        assert!(!metrics.task_store_remote_ok);
        assert_eq!(metrics.task_store_remote_last_probe_epoch, 0);
        assert_eq!(metrics.task_store_remote_probe_failures, 0);
    }

    #[test]
    fn remote_probe_refresh_uses_latest_runtime_config() {
        let state = test_state(false);
        let first = refresh_remote_task_store_probe_once_with(&state, |_| true);
        assert!(!first);
        {
            let mut cfg = state.task_cfg.lock().expect("cfg lock");
            cfg.remote_enabled = true;
        }
        let second = refresh_remote_task_store_probe_once_with(&state, |_| true);
        assert!(second);
        let metrics = state.metrics.lock().expect("metrics lock");
        assert!(metrics.task_store_remote_ok);
        assert!(metrics.task_store_remote_last_probe_epoch > 0);
    }
}
