use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, atomic::AtomicBool},
};

#[derive(Clone)]
pub struct AppState {
    pub service: String,
    pub tasks: Arc<Mutex<HashMap<String, TaskState>>>,
    pub metrics: Arc<Mutex<ServiceMetrics>>,
    pub task_cfg: Arc<Mutex<TaskStoreConfig>>,
    pub cancel_flags: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
    pub tenant_running: Arc<Mutex<HashMap<String, usize>>>,
    pub idempotency_index: Arc<Mutex<HashMap<String, String>>>,
    pub transform_cache: Arc<Mutex<HashMap<String, TransformCacheEntry>>>,
    pub schema_registry: Arc<Mutex<HashMap<String, Value>>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TaskState {
    pub task_id: String,
    pub tenant_id: String,
    pub operator: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub idempotency_key: String,
    pub attempts: u32,
}

#[derive(Clone)]
pub struct TaskStoreConfig {
    pub ttl_sec: u64,
    pub max_tasks: usize,
    pub store_path: Option<PathBuf>,
    pub remote_enabled: bool,
    pub backend: String,
    pub base_api_url: Option<String>,
    pub base_api_key: Option<String>,
    pub sql_host: String,
    pub sql_port: u16,
    pub sql_db: String,
    pub sql_user: Option<String>,
    pub sql_password: Option<String>,
    pub sql_use_windows_auth: bool,
}

#[derive(Default)]
pub struct ServiceMetrics {
    pub transform_rows_v2_calls: u64,
    pub transform_rows_v2_errors: u64,
    pub transform_rows_v2_success_total: u64,
    pub transform_rows_v2_columnar_calls: u64,
    pub transform_rows_v2_columnar_success_total: u64,
    pub transform_rows_v2_latency_ms_sum: u128,
    pub transform_rows_v2_latency_ms_max: u128,
    pub transform_rows_v2_output_rows_sum: u64,
    pub text_preprocess_v2_calls: u64,
    pub text_preprocess_v2_errors: u64,
    pub task_store_remote_ok: bool,
    pub task_store_remote_probe_failures: u64,
    pub task_store_remote_last_probe_epoch: u64,
    pub task_cancel_requested_total: u64,
    pub task_cancel_effective_total: u64,
    pub task_flag_cleanup_total: u64,
    pub tasks_active: i64,
    pub task_retry_total: u64,
    pub tenant_reject_total: u64,
    pub quota_reject_total: u64,
    pub latency_le_10ms: u64,
    pub latency_le_50ms: u64,
    pub latency_le_200ms: u64,
    pub latency_gt_200ms: u64,
    pub transform_cache_hit_total: u64,
    pub transform_cache_miss_total: u64,
    pub transform_cache_evict_total: u64,
    pub join_rows_v2_calls: u64,
    pub aggregate_rows_v2_calls: u64,
    pub quality_check_v2_calls: u64,
    pub schema_registry_register_total: u64,
    pub schema_registry_get_total: u64,
    pub schema_registry_infer_total: u64,
    pub transform_rows_v3_calls: u64,
    pub join_rows_v3_calls: u64,
    pub aggregate_rows_v3_calls: u64,
    pub quality_check_v3_calls: u64,
    pub load_rows_v3_calls: u64,
    pub schema_registry_v2_calls: u64,
    pub udf_wasm_v1_calls: u64,
    pub operator_latency_samples: HashMap<String, Vec<u128>>,
}

#[derive(Clone, Serialize)]
pub struct TransformRowsStats {
    pub input_rows: usize,
    pub output_rows: usize,
    pub invalid_rows: usize,
    pub filtered_rows: usize,
    pub duplicate_rows_removed: usize,
    pub latency_ms: u128,
}

#[derive(Clone, Serialize)]
pub struct TransformRowsResp {
    pub ok: bool,
    pub operator: String,
    pub status: String,
    pub run_id: Option<String>,
    pub trace_id: String,
    pub rows: Vec<Value>,
    pub quality: Value,
    pub gate_result: Value,
    pub stats: TransformRowsStats,
    pub rust_v2_used: bool,
    pub schema_hint: Option<Value>,
    pub aggregate: Option<Value>,
    pub audit: Value,
}

#[derive(Clone)]
pub struct TransformCacheEntry {
    pub resp: TransformRowsResp,
    pub expires_at_epoch: u64,
    pub last_hit_epoch: u64,
    pub hits: u64,
}
