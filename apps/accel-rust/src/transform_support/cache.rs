use crate::operators::transform::TransformRowsReq;
use accel_rust::app_state::TransformCacheEntry;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    env,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static UNIQUE_TRACE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn utc_now_iso() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{ts}")
}

pub(crate) fn short_trace(seed: &str) -> String {
    let mut h = Sha256::new();
    h.update(seed.as_bytes());
    let hex = format!("{:x}", h.finalize());
    hex[..16].to_string()
}

pub(crate) fn unique_trace(seed: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seq = UNIQUE_TRACE_COUNTER.fetch_add(1, Ordering::Relaxed);
    short_trace(&format!("{seed}:{ts}:{seq}"))
}

pub(crate) fn unix_now_sec() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub(crate) fn transform_cache_enabled() -> bool {
    env::var("AIWF_RUST_TRANSFORM_CACHE_ENABLED")
        .ok()
        .map(|v| {
            let t = v.trim().to_ascii_lowercase();
            t == "1" || t == "true" || t == "yes" || t == "on"
        })
        .unwrap_or(true)
}

pub(crate) fn transform_cache_ttl_sec() -> u64 {
    env::var("AIWF_RUST_TRANSFORM_CACHE_TTL_SEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(600)
}

pub(crate) fn transform_cache_max_entries() -> usize {
    env::var("AIWF_RUST_TRANSFORM_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256)
}

pub(crate) fn transform_cache_key(req: &TransformRowsReq) -> String {
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

pub(crate) fn prune_transform_cache_entries(
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

#[cfg(test)]
mod tests {
    use super::unique_trace;

    #[test]
    fn unique_trace_is_distinct_across_calls() {
        let first = unique_trace("trace");
        let second = unique_trace("trace");
        assert_ne!(first, second);
    }
}
