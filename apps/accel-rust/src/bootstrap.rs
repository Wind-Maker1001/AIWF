use odbc_api::Environment;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace as sdktrace};
use std::{
    collections::HashMap,
    env,
    sync::{
        Arc, Mutex,
        atomic::AtomicBool,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    app_state::{AppState, ServiceMetrics, TaskStoreConfig},
    config::{allow_egress_enabled, is_local_endpoint},
    metrics::load_metrics_v2_samples,
    task_store::{
        load_tasks_from_store, probe_remote_task_store, resolve_task_store_backend,
        task_store_config_from_env, task_store_remote_enabled,
    },
};

pub fn build_app_state_from_env() -> AppState {
    let task_cfg = prepare_task_store_config_from_env();
    build_app_state(task_cfg)
}

pub fn build_app_state(task_cfg: TaskStoreConfig) -> AppState {
    let tasks_loaded = load_tasks_from_store(task_cfg.store_path.as_ref());
    let metrics = ServiceMetrics {
        operator_latency_samples: load_metrics_v2_samples(),
        ..ServiceMetrics::default()
    };
    AppState {
        service: "accel-rust".to_string(),
        tasks: Arc::new(Mutex::new(tasks_loaded)),
        metrics: Arc::new(Mutex::new(metrics)),
        task_cfg: Arc::new(Mutex::new(task_cfg)),
        cancel_flags: Arc::new(Mutex::new(HashMap::<String, Arc<AtomicBool>>::new())),
        tenant_running: Arc::new(Mutex::new(HashMap::new())),
        idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        transform_cache: Arc::new(Mutex::new(HashMap::new())),
        schema_registry: Arc::new(Mutex::new(HashMap::new())),
    }
}

pub fn init_observability() {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,h2=warn,tower_http=warn"));
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let endpoint = env::var("AIWF_OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if let Some(endpoint) = endpoint {
        if !allow_egress_enabled() && !is_local_endpoint(&endpoint) {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .init();
            warn!(
                "otel exporter endpoint blocked by AIWF_ALLOW_EGRESS=false: {}",
                endpoint
            );
            return;
        }
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
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
            Err(error) => {
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(fmt_layer)
                    .init();
                error!("failed to init otel exporter: {error}");
            }
        }
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
    }
}

pub fn init_remote_task_store_probe(state: &AppState) {
    refresh_remote_task_store_probe_once(state);
    let state = state.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(30)).await;
            refresh_remote_task_store_probe_once_async(&state).await;
        }
    });
}

pub async fn shutdown_signal() {
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

fn prepare_task_store_config_from_env() -> TaskStoreConfig {
    let task_cfg = resolve_task_store_backend(task_store_config_from_env());
    if task_cfg.remote_enabled && task_cfg.backend == "odbc" {
        let _ = unsafe {
            Environment::set_connection_pooling(odbc_api::sys::AttrConnectionPooling::DriverAware)
        };
    }
    task_cfg
}

fn refresh_remote_task_store_probe_metrics(
    metrics: &Arc<Mutex<ServiceMetrics>>,
    enabled: bool,
    ok: Option<bool>,
) {
    if let Ok(mut metrics) = metrics.lock() {
        if !enabled {
            metrics.task_store_remote_ok = false;
            metrics.task_store_remote_last_probe_epoch = 0;
            return;
        }
        let status = ok.unwrap_or(false);
        metrics.task_store_remote_ok = status;
        metrics.task_store_remote_last_probe_epoch = unix_now_sec();
        if !status {
            metrics.task_store_remote_probe_failures += 1;
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

pub fn refresh_remote_task_store_probe_once(state: &AppState) -> bool {
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

pub fn current_task_cfg(state: &AppState) -> TaskStoreConfig {
    state
        .task_cfg
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_else(|_| task_store_config_from_env())
}

fn unix_now_sec() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{build_app_state, refresh_remote_task_store_probe_once_with};
    use crate::app_state::TaskStoreConfig;

    fn test_state(remote_enabled: bool) -> crate::app_state::AppState {
        build_app_state(TaskStoreConfig {
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
        })
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
