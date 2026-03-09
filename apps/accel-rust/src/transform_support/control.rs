use crate::{
    operators::transform::TransformRowsReq,
    platform_ops::{load_kv_store, operator_policy_store_path, tenant_isolation_store_path},
};
use accel_rust::app_state::{AppState, ServiceMetrics};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    env,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

pub(crate) fn collapse_ws(s: &str) -> String {
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

pub(crate) fn can_cancel_status(status: &str) -> bool {
    status == "queued" || status == "running"
}

pub(crate) fn tenant_max_concurrency() -> usize {
    env::var("AIWF_TENANT_MAX_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(4)
        .max(1)
}

pub(crate) fn tenant_policy_for(tenant: &str) -> Value {
    let store = load_kv_store(&tenant_isolation_store_path());
    store.get(tenant).cloned().unwrap_or_else(|| json!({}))
}

pub(crate) fn tenant_override_usize(tenant: &str, key: &str) -> Option<usize> {
    tenant_policy_for(tenant)
        .get(key)
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
}

pub(crate) fn tenant_max_concurrency_for(tenant: Option<&str>) -> usize {
    if let Some(t) = tenant
        && let Some(v) = tenant_override_usize(t, "max_concurrency")
    {
        return v.max(1);
    }
    tenant_max_concurrency()
}

pub(crate) fn tenant_max_rows() -> usize {
    env::var("AIWF_TENANT_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(250_000)
        .max(1)
}

pub(crate) fn tenant_max_rows_for(tenant: Option<&str>) -> usize {
    if let Some(t) = tenant
        && let Some(v) = tenant_override_usize(t, "max_rows")
    {
        return v.max(1);
    }
    tenant_max_rows()
}

pub(crate) fn tenant_max_payload_bytes() -> usize {
    env::var("AIWF_TENANT_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(128 * 1024 * 1024)
        .max(1024)
}

pub(crate) fn tenant_max_payload_bytes_for(tenant: Option<&str>) -> usize {
    if let Some(t) = tenant
        && let Some(v) = tenant_override_usize(t, "max_payload_bytes")
    {
        return v.max(1024);
    }
    tenant_max_payload_bytes()
}

pub(crate) fn tenant_max_workflow_steps() -> usize {
    env::var("AIWF_TENANT_MAX_WORKFLOW_STEPS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(128)
        .max(1)
}

pub(crate) fn tenant_max_workflow_steps_for(tenant: Option<&str>) -> usize {
    if let Some(t) = tenant
        && let Some(v) = tenant_override_usize(t, "max_workflow_steps")
    {
        return v.max(1);
    }
    tenant_max_workflow_steps()
}

pub(crate) fn enforce_tenant_payload_quota(
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

pub(crate) fn try_acquire_tenant_slot(state: &AppState, tenant: &str) -> Result<(), String> {
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

pub(crate) fn release_tenant_slot(state: &AppState, tenant: &str) {
    if let Ok(mut running) = state.tenant_running.lock()
        && let Some(v) = running.get_mut(tenant)
        && *v > 0
    {
        *v -= 1;
    }
}

pub(crate) fn parse_env_op_set(name: &str) -> HashSet<String> {
    env::var(name)
        .ok()
        .unwrap_or_default()
        .split([',', ';'])
        .map(|x| x.trim().to_lowercase())
        .filter(|x| !x.is_empty())
        .collect()
}

pub(crate) fn operator_allowed_for_tenant(operator: &str, tenant: Option<&str>) -> bool {
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

pub(crate) fn verify_request_signature(req: &TransformRowsReq) -> Result<(), String> {
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

pub(crate) fn resolve_trace_id(
    explicit: Option<&str>,
    traceparent: Option<&str>,
    seed: &str,
) -> String {
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

pub(crate) fn is_cancelled(flag: &Option<Arc<AtomicBool>>) -> bool {
    match flag {
        Some(v) => v.load(Ordering::Relaxed),
        None => false,
    }
}

pub(crate) fn cleanup_task_flag(
    task_id: &str,
    cancel_flags: &Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
    metrics: &Arc<Mutex<ServiceMetrics>>,
) {
    let mut removed = false;
    if let Ok(mut flags) = cancel_flags.lock() {
        removed = flags.remove(task_id).is_some();
    }
    if let Ok(mut m) = metrics.lock()
        && removed
    {
        m.tasks_active = (m.tasks_active - 1).max(0);
        m.task_flag_cleanup_total += 1;
    }
}

pub(crate) fn validate_sql_identifier(s: &str) -> Result<String, String> {
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

pub(crate) fn validate_where_clause(s: &str) -> Result<String, String> {
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

pub(crate) fn validate_readonly_query(query: &str) -> Result<String, String> {
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
