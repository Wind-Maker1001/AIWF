#![allow(unused_imports)]

use super::{
    AggregateRowsReq, AggregateRowsV2Req, AggregateRowsV3Req, AggregateRowsV4Req,
    AnomalyExplainReq, AppState, CleanRow, ColumnarEvalV1Req, EvidenceRankReq, ExplainPlanV1Req,
    FactCrosscheckReq, FinanceRatioReq, JoinRowsV2Req, JoinRowsV3Req, JoinRowsV4Req, LineageV3Req,
    LoadRowsV3Req, OptimizerV1Req, ParquetIoV2Req, PluginExecReq, PluginOperatorV1Req,
    PluginRegistryV1Req, ProvenanceSignReq, QualityCheckV3Req, QualityCheckV4Req, QueryLangReq,
    RulesPackageGetReq, RulesPackagePublishReq, RuntimeStatsV1Req, SchemaCompatReq, SchemaGetReq,
    SchemaInferReq, SchemaMigrationSuggestReq, ServiceMetrics, SketchV1Req, StreamStateLoadReq,
    StreamStateSaveReq, StreamStateV2Req, StreamWindowV1Req, StreamWindowV2Req, TaskState,
    TaskStoreConfig, TemplateBindReq, TimeSeriesForecastReq, TransformCacheEntry, TransformRowsReq,
    TransformRowsV3Req, UdfWasmReq, UdfWasmV2Req, VectorIndexBuildReq, VectorIndexSearchReq,
    WindowRowsV1Req, WorkflowRunReq, build_router, can_cancel_status, evaluate_quality_gates,
    load_and_clean_rows, load_parquet_rows, load_rows_from_uri_limited, run_aggregate_rows_v1,
    run_aggregate_rows_v2, run_aggregate_rows_v3, run_aggregate_rows_v4, run_anomaly_explain_v1,
    run_columnar_eval_v1, run_evidence_rank_v1, run_explain_plan_v1, run_fact_crosscheck_v1,
    run_finance_ratio_v1, run_join_rows_v2, run_join_rows_v3, run_join_rows_v4, run_lineage_v3,
    run_load_rows_v3, run_optimizer_v1, run_parquet_io_v2, run_plugin_exec_v1,
    run_plugin_operator_v1, run_plugin_registry_v1, run_provenance_sign_v1, run_quality_check_v1,
    run_quality_check_v3, run_quality_check_v4, run_query_lang_v1, run_rules_package_get_v1,
    run_rules_package_publish_v1, run_runtime_stats_v1, run_schema_registry_check_compat_v2,
    run_schema_registry_get_v1, run_schema_registry_infer_v1,
    run_schema_registry_suggest_migration_v2, run_sketch_v1, run_stream_state_load_v1,
    run_stream_state_save_v1, run_stream_state_v2, run_stream_window_v1, run_stream_window_v2,
    run_template_bind_v1, run_timeseries_forecast_v1, run_transform_rows_v2,
    run_transform_rows_v2_with_cache, run_transform_rows_v3, run_udf_wasm_v1, run_udf_wasm_v2,
    run_vector_index_build_v1, run_vector_index_search_v1, run_window_rows_v1, run_workflow,
    save_rows_parquet, utc_now_iso, validate_where_clause, value_to_string, write_cleaned_parquet,
};
use crate::operators::transform::run_transform_rows_v2_with_cancel;
use accel_rust::task_store::prune_tasks;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    io::Write,
    sync::{Arc, Mutex, OnceLock, atomic::AtomicBool},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;

fn test_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct TestEnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl TestEnvGuard {
    fn set(vars: &[(&'static str, String)]) -> Self {
        let saved = vars
            .iter()
            .map(|(key, _)| (*key, std::env::var(key).ok()))
            .collect::<Vec<_>>();
        for (key, value) in vars {
            unsafe {
                std::env::set_var(key, value);
            }
        }
        Self { saved }
    }
}

impl Drop for TestEnvGuard {
    fn drop(&mut self) {
        for (key, value) in &self.saved {
            match value {
                Some(v) => unsafe {
                    std::env::set_var(key, v);
                },
                None => unsafe {
                    std::env::remove_var(key);
                },
            }
        }
    }
}

fn plugin_signature(secret: &str, plugin: &str, cmd: &str, args: &[String]) -> String {
    let mut h = Sha256::new();
    h.update(format!("{secret}:{plugin}:{cmd}:{}", args.join("\u{1f}")).as_bytes());
    format!("{:x}", h.finalize())
}

#[path = "main_tests_part1.rs"]
mod part1;

#[path = "main_tests_part2.rs"]
mod part2;

#[path = "main_tests_part3.rs"]
mod part3;
