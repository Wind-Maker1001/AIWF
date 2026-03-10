use super::*;

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
