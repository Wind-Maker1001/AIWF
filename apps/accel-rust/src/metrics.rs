use crate::app_state::ServiceMetrics;
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub fn observe_operator_latency_v2(metrics: &Arc<Mutex<ServiceMetrics>>, op: &str, latency_ms: u128) {
    if let Ok(mut m) = metrics.lock() {
        let entry = m
            .operator_latency_samples
            .entry(op.to_string())
            .or_insert_with(Vec::new);
        entry.push(latency_ms);
        if entry.len() > 2048 {
            let drop_n = entry.len().saturating_sub(2048);
            entry.drain(0..drop_n);
        }
        let _ = persist_metrics_v2_samples(&m.operator_latency_samples);
    }
}

pub fn percentile_from_sorted(vals: &[u128], p: f64) -> u128 {
    if vals.is_empty() {
        return 0;
    }
    let pos = ((vals.len() - 1) as f64 * p.clamp(0.0, 1.0)).round() as usize;
    vals[pos.min(vals.len() - 1)]
}

pub fn metrics_v2_store_path() -> PathBuf {
    env::var("AIWF_METRICS_V2_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("metrics_v2.json"))
}

pub fn acquire_file_lock(base_path: &Path) -> Result<PathBuf, String> {
    let lock = base_path.with_extension("lock");
    if let Some(parent) = lock.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create lock dir: {e}"))?;
    }
    for _ in 0..100 {
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock)
        {
            Ok(_) => return Ok(lock),
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(10)),
        }
    }
    Err(format!(
        "acquire file lock timeout: {}",
        lock.to_string_lossy()
    ))
}

pub fn release_file_lock(lock_path: &Path) {
    let _ = fs::remove_file(lock_path);
}

pub fn persist_metrics_v2_samples(samples: &HashMap<String, Vec<u128>>) -> Result<(), String> {
    let p = metrics_v2_store_path();
    let lock = acquire_file_lock(&p)?;
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create metrics dir: {e}"))?;
    }
    let s = serde_json::to_string_pretty(samples).map_err(|e| format!("serialize metrics: {e}"))?;
    let out = fs::write(&p, s).map_err(|e| format!("write metrics store: {e}"));
    release_file_lock(&lock);
    out
}

pub fn load_metrics_v2_samples() -> HashMap<String, Vec<u128>> {
    let p = metrics_v2_store_path();
    if let Ok(lock) = acquire_file_lock(&p) {
        let out = (|| {
            let Ok(txt) = fs::read_to_string(&p) else {
                return HashMap::new();
            };
            serde_json::from_str::<HashMap<String, Vec<u128>>>(&txt).unwrap_or_default()
        })();
        release_file_lock(&lock);
        return out;
    }
    HashMap::new()
}
