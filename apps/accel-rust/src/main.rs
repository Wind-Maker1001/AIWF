use opentelemetry::global;
use tracing::info;

use accel_rust::{
    bootstrap::{
        build_app_state_from_env, init_observability, init_remote_task_store_probe, shutdown_signal,
    },
    config::ServerBind,
};

pub(crate) use accel_rust::bootstrap::{current_task_cfg, refresh_remote_task_store_probe_once};

#[cfg(test)]
use accel_rust::app_state::{AppState, ServiceMetrics, TaskState, TaskStoreConfig, TransformCacheEntry};

mod analysis_ops;
mod api_types;
mod cleaning_runtime;
mod execution_ops;
mod governance_ops;
mod http;
mod load_ops;
mod misc_ops;
mod operator_catalog;
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
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use operators::analytics::{
    AggregateRowsReq, AggregateRowsV2Req, AggregateRowsV3Req, AggregateRowsV4Req, QualityCheckReq,
    QualityCheckV2Req, QualityCheckV3Req, QualityCheckV4Req, approx_percentile, compute_aggregate,
    parse_agg_specs, run_aggregate_rows_v1, run_aggregate_rows_v2, run_aggregate_rows_v3,
    run_aggregate_rows_v4, run_quality_check_v1, run_quality_check_v2, run_quality_check_v3,
    run_quality_check_v4,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use operators::join::{
    JoinRowsReq, JoinRowsV2Req, JoinRowsV3Req, JoinRowsV4Req, run_join_rows_v1, run_join_rows_v2,
    run_join_rows_v3, run_join_rows_v4,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use operators::transform::{
    TransformRowsReq, TransformRowsV3Req, collect_expr_lineage, observe_transform_success,
    run_transform_rows_v2, run_transform_rows_v2_with_cache, run_transform_rows_v3,
};
#[cfg(test)]
pub(crate) use operators::workflow::run_workflow;
#[cfg(test)]
#[allow(unused_imports)]
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

    let state = build_app_state_from_env();
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

#[cfg(test)]
mod main_tests;
