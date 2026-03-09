use crate::api_types::StreamStateV2Req;
use accel_rust::metrics::{acquire_file_lock, release_file_lock};
use rusqlite::Connection as SqliteConnection;
use serde_json::Value;
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
};

pub(crate) fn vector_index_store_path() -> PathBuf {
    env::var("AIWF_VECTOR_INDEX_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("vector_index.json"))
}

pub(crate) fn stream_state_store_path() -> PathBuf {
    env::var("AIWF_STREAM_STATE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("stream_state.json"))
}

pub(crate) fn runtime_stats_store_path() -> PathBuf {
    env::var("AIWF_RUNTIME_STATS_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("runtime_stats.json"))
}

pub(crate) fn explain_feedback_store_path() -> PathBuf {
    env::var("AIWF_EXPLAIN_FEEDBACK_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("explain_feedback.json"))
}

pub(crate) fn vector_index_v2_store_path() -> PathBuf {
    env::var("AIWF_VECTOR_INDEX_V2_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("vector_index_v2.json"))
}

pub(crate) fn stream_reliability_store_path() -> PathBuf {
    env::var("AIWF_STREAM_RELIABILITY_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("stream_reliability.json"))
}

pub(crate) fn tenant_isolation_store_path() -> PathBuf {
    env::var("AIWF_TENANT_ISOLATION_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("tenant_isolation.json"))
}

pub(crate) fn operator_policy_store_path() -> PathBuf {
    env::var("AIWF_OPERATOR_POLICY_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("operator_policy.json"))
}

pub(crate) fn perf_baseline_store_path() -> PathBuf {
    env::var("AIWF_PERF_BASELINE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("perf_baseline.json"))
}

pub(crate) fn normalize_error_code(err: &str) -> String {
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

pub(crate) fn maybe_inject_fault(operator: &str) -> Result<(), String> {
    let cfg = env::var("AIWF_FAULT_INJECT").unwrap_or_default();
    if cfg.trim().is_empty() {
        return Ok(());
    }
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

pub(crate) fn stream_state_sqlite_path(req: &StreamStateV2Req) -> PathBuf {
    if let Some(p) = req.db_path.as_ref().filter(|s| !s.trim().is_empty()) {
        return PathBuf::from(p);
    }
    env::var("AIWF_STREAM_STATE_SQLITE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("stream_state.sqlite"))
}

pub(crate) fn ensure_stream_state_sqlite(db: &Path) -> Result<SqliteConnection, String> {
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

pub(crate) fn load_kv_store(path: &Path) -> HashMap<String, Value> {
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

pub(crate) fn save_kv_store(path: &Path, store: &HashMap<String, Value>) -> Result<(), String> {
    let lock = acquire_file_lock(path)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create kv dir: {e}"))?;
    }
    let s = serde_json::to_string_pretty(store).map_err(|e| format!("serialize kv: {e}"))?;
    let out = fs::write(path, s).map_err(|e| format!("write kv: {e}"));
    release_file_lock(&lock);
    out
}
