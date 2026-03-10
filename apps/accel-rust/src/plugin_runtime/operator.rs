use super::*;

struct PluginCircuitBreaker {
    threshold: u64,
    open_ms: u64,
}

struct PluginFailureContext<'a> {
    key: &'a str,
    now_ms: u64,
    run_id: Option<&'a str>,
    tenant: &'a str,
    plugin: &'a str,
    op: &'a str,
    started: &'a Instant,
}

fn record_plugin_operator_failure(
    runtime_store: &mut HashMap<String, Value>,
    circuit: &PluginCircuitBreaker,
    ctx: &PluginFailureContext<'_>,
    error: &str,
) -> String {
    let fail = runtime_store
        .get(ctx.key)
        .and_then(|v| v.get("fail_count"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
        + 1;
    let next_open = if fail >= circuit.threshold {
        ctx.now_ms + circuit.open_ms
    } else {
        0
    };
    runtime_store.insert(
        ctx.key.to_string(),
        json!({
            "fail_count": fail,
            "open_until": next_open,
            "updated_at": utc_now_iso()
        }),
    );
    let _ = save_kv_store(&plugin_runtime_store_path(), runtime_store);
    let _ = append_plugin_audit(&json!({
        "ts": utc_now_iso(),
        "run_id": ctx.run_id,
        "tenant_id": ctx.tenant,
        "plugin": ctx.plugin,
        "op": ctx.op,
        "status": "failed",
        "duration_ms": ctx.started.elapsed().as_millis(),
        "error": error
    }));
    error.to_string()
}

pub(crate) fn run_plugin_operator_v1(req: PluginOperatorV1Req) -> Result<Value, String> {
    let plugin = safe_pkg_token(&req.plugin)?;
    let op = req.op.unwrap_or_else(|| "run".to_string());
    let tenant = req
        .tenant_id
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let payload = req.payload.unwrap_or(Value::Null);
    let max_bytes = env::var("AIWF_PLUGIN_OPERATOR_MAX_INPUT_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000);
    let bytes = serde_json::to_vec(&payload).map(|v| v.len()).unwrap_or(0);
    if bytes > max_bytes {
        return Err(format!(
            "plugin_operator_v1 input exceeds limit: {} > {}",
            bytes, max_bytes
        ));
    }
    let max_out = env::var("AIWF_PLUGIN_OPERATOR_MAX_OUTPUT_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(2_000_000);
    let max_concurrent = env::var("AIWF_PLUGIN_OPERATOR_MAX_CONCURRENT_PER_TENANT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(2)
        .max(1);
    let cb_threshold = env::var("AIWF_PLUGIN_OPERATOR_CB_FAIL_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(3)
        .max(1);
    let cb_open_ms = env::var("AIWF_PLUGIN_OPERATOR_CB_OPEN_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(30_000)
        .max(1000);
    let circuit = PluginCircuitBreaker {
        threshold: cb_threshold,
        open_ms: cb_open_ms,
    };

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut rt = load_kv_store(&plugin_runtime_store_path());
    let key = format!("{}::{}", tenant, plugin);
    let open_until = rt
        .get(&key)
        .and_then(|v| v.get("open_until"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    if open_until > now_ms {
        return Err(format!(
            "plugin_operator_v1 circuit open for {} ms",
            open_until.saturating_sub(now_ms)
        ));
    }

    {
        let mut m = plugin_tenant_running_map()
            .lock()
            .map_err(|_| "plugin_operator_v1 tenant runtime lock poisoned".to_string())?;
        let cur = m.get(&tenant).copied().unwrap_or(0);
        if cur >= max_concurrent {
            return Err(format!(
                "plugin_operator_v1 tenant concurrent limit exceeded: {} >= {}",
                cur, max_concurrent
            ));
        }
        m.insert(tenant.clone(), cur + 1);
    }

    let started = Instant::now();
    let result = run_plugin_exec_v1(PluginExecReq {
        run_id: req.run_id.clone(),
        tenant_id: Some(tenant.clone()),
        trace_id: None,
        plugin: plugin.clone(),
        input: json!({
            "op": op,
            "payload": payload
        }),
    });
    {
        if let Ok(mut m) = plugin_tenant_running_map().lock() {
            let cur = m.get(&tenant).copied().unwrap_or(1);
            if cur <= 1 {
                m.remove(&tenant);
            } else {
                m.insert(tenant.clone(), cur - 1);
            }
        }
    }

    match result {
        Ok(exec) => {
            if !exec.ok {
                let error = if exec.stderr.trim().is_empty() {
                    format!(
                        "plugin_operator_v1 execution failed: status={}",
                        exec.status
                    )
                } else {
                    format!(
                        "plugin_operator_v1 execution failed: {}",
                        exec.stderr.trim()
                    )
                };
                let ctx = PluginFailureContext {
                    key: &key,
                    now_ms,
                    run_id: req.run_id.as_deref(),
                    tenant: &tenant,
                    plugin: &plugin,
                    op: &op,
                    started: &started,
                };
                return Err(record_plugin_operator_failure(
                    &mut rt, &circuit, &ctx, &error,
                ));
            }
            let out_bytes = serde_json::to_vec(&exec.output)
                .map(|v| v.len())
                .unwrap_or(0);
            if out_bytes > max_out {
                return Err(format!(
                    "plugin_operator_v1 output exceeds limit: {} > {}",
                    out_bytes, max_out
                ));
            }
            rt.insert(
                key.clone(),
                json!({
                    "fail_count": 0u64,
                    "open_until": 0u64,
                    "updated_at": utc_now_iso()
                }),
            );
            let _ = save_kv_store(&plugin_runtime_store_path(), &rt);
            let _ = append_plugin_audit(&json!({
                "ts": utc_now_iso(),
                "run_id": req.run_id,
                "tenant_id": tenant,
                "plugin": plugin,
                "op": op,
                "status": "done",
                "duration_ms": started.elapsed().as_millis(),
                "stderr_len": exec.stderr.len(),
                "output_bytes": out_bytes
            }));
            Ok(json!({
                "ok": true,
                "operator": "plugin_operator_v1",
                "status": "done",
                "run_id": req.run_id,
                "plugin": plugin,
                "output": exec.output,
                "stderr": exec.stderr
            }))
        }
        Err(e) => {
            let ctx = PluginFailureContext {
                key: &key,
                now_ms,
                run_id: req.run_id.as_deref(),
                tenant: &tenant,
                plugin: &plugin,
                op: &op,
                started: &started,
            };
            Err(record_plugin_operator_failure(&mut rt, &circuit, &ctx, &e))
        }
    }
}
