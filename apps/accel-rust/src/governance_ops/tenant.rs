use crate::{
    api_types::{
        OperatorPolicyV1Req, OptimizerAdaptiveV2Req, RuntimeStatsV1Req, TenantIsolationV1Req,
    },
    governance_ops::run_runtime_stats_v1,
    platform_ops::{
        load_kv_store, operator_policy_store_path, save_kv_store, tenant_isolation_store_path,
    },
};
use serde_json::{Map, Value, json};

pub(crate) fn run_tenant_isolation_v1(req: TenantIsolationV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let tenant = req
        .tenant_id
        .clone()
        .unwrap_or_else(|| "default".to_string())
        .trim()
        .to_lowercase();
    if tenant.is_empty() {
        return Err("tenant_isolation_v1 requires tenant_id".to_string());
    }
    let mut store = load_kv_store(&tenant_isolation_store_path());
    if op == "get" {
        return Ok(json!({
            "ok": true,
            "operator": "tenant_isolation_v1",
            "status": "done",
            "run_id": req.run_id,
            "tenant_id": tenant,
            "policy": store.get(&tenant).cloned().unwrap_or_else(|| json!({}))
        }));
    }
    if op == "reset" || op == "delete" {
        let deleted = store.remove(&tenant).is_some();
        save_kv_store(&tenant_isolation_store_path(), &store)?;
        return Ok(
            json!({"ok": true, "operator":"tenant_isolation_v1","status":"done","run_id": req.run_id, "tenant_id":tenant,"deleted":deleted}),
        );
    }
    if op != "set" {
        return Err(format!("tenant_isolation_v1 unsupported op: {op}"));
    }
    let mut policy = store
        .get(&tenant)
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    if let Some(v) = req.max_concurrency {
        policy.insert("max_concurrency".to_string(), json!(v.max(1)));
    }
    if let Some(v) = req.max_rows {
        policy.insert("max_rows".to_string(), json!(v.max(1)));
    }
    if let Some(v) = req.max_payload_bytes {
        policy.insert("max_payload_bytes".to_string(), json!(v.max(1024)));
    }
    if let Some(v) = req.max_workflow_steps {
        policy.insert("max_workflow_steps".to_string(), json!(v.max(1)));
    }
    store.insert(tenant.clone(), Value::Object(policy.clone()));
    save_kv_store(&tenant_isolation_store_path(), &store)?;
    Ok(
        json!({"ok": true, "operator":"tenant_isolation_v1","status":"done","run_id": req.run_id, "tenant_id":tenant,"policy": policy}),
    )
}

pub(crate) fn run_operator_policy_v1(req: OperatorPolicyV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let tenant = req
        .tenant_id
        .clone()
        .unwrap_or_else(|| "default".to_string())
        .trim()
        .to_lowercase();
    if tenant.is_empty() {
        return Err("operator_policy_v1 requires tenant_id".to_string());
    }
    let mut store = load_kv_store(&operator_policy_store_path());
    if op == "get" {
        return Ok(
            json!({"ok": true,"operator":"operator_policy_v1","status":"done","run_id": req.run_id,"tenant_id":tenant,"policy": store.get(&tenant).cloned().unwrap_or_else(|| json!({}))}),
        );
    }
    if op == "reset" || op == "delete" {
        let deleted = store.remove(&tenant).is_some();
        save_kv_store(&operator_policy_store_path(), &store)?;
        return Ok(
            json!({"ok": true,"operator":"operator_policy_v1","status":"done","run_id": req.run_id,"tenant_id":tenant,"deleted": deleted}),
        );
    }
    if op != "set" {
        return Err(format!("operator_policy_v1 unsupported op: {op}"));
    }
    let mut m = Map::new();
    let allow = req
        .allow
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.trim().to_lowercase())
        .filter(|x| !x.is_empty())
        .map(Value::String)
        .collect::<Vec<_>>();
    let deny = req
        .deny
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.trim().to_lowercase())
        .filter(|x| !x.is_empty())
        .map(Value::String)
        .collect::<Vec<_>>();
    m.insert("allow".to_string(), Value::Array(allow));
    m.insert("deny".to_string(), Value::Array(deny));
    store.insert(tenant.clone(), Value::Object(m.clone()));
    save_kv_store(&operator_policy_store_path(), &store)?;
    Ok(
        json!({"ok": true,"operator":"operator_policy_v1","status":"done","run_id": req.run_id,"tenant_id":tenant,"policy": m}),
    )
}

pub(crate) fn run_optimizer_adaptive_v2(req: OptimizerAdaptiveV2Req) -> Result<Value, String> {
    let operator = req
        .operator
        .clone()
        .unwrap_or_else(|| "transform_rows_v3".to_string());
    let row_hint = req.row_count_hint.unwrap_or(0);
    let mut engine = if row_hint >= 120_000 {
        "columnar_arrow_v1"
    } else if row_hint >= 20_000 {
        "columnar_v1"
    } else {
        "row_v1"
    }
    .to_string();
    let stats = run_runtime_stats_v1(RuntimeStatsV1Req {
        run_id: req.run_id.clone(),
        op: "summary".to_string(),
        operator: None,
        ok: None,
        error_code: None,
        duration_ms: None,
        rows_in: None,
        rows_out: None,
    })?;
    let mut evidence = Vec::new();
    if let Some(items) = stats.get("items").and_then(|v| v.as_array()) {
        for it in items {
            if it.get("operator").and_then(|v| v.as_str()) == Some(&operator) {
                let p95 = it.get("p95_ms").and_then(|v| v.as_u64()).unwrap_or(0);
                if p95 > 1500 {
                    engine = "columnar_arrow_v1".to_string();
                } else if p95 > 500 {
                    engine = "columnar_v1".to_string();
                }
                evidence.push(it.clone());
            }
        }
    }
    if req.prefer_arrow.unwrap_or(false) {
        engine = "columnar_arrow_v1".to_string();
    }
    Ok(json!({
        "ok": true,
        "operator": "optimizer_adaptive_v2",
        "status": "done",
        "run_id": req.run_id,
        "target_operator": operator,
        "recommended_engine": engine,
        "row_count_hint": row_hint,
        "evidence": evidence
    }))
}
