use crate::{
    api_types::{
        PluginExecReq, PluginExecResp, PluginManifest, PluginOperatorV1Req, PluginRegistryV1Req,
    },
    load_kv_store, resolve_trace_id, safe_pkg_token, save_kv_store, utc_now_iso,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    env, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{Mutex, OnceLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

pub(crate) fn plugin_dir() -> PathBuf {
    env::var("AIWF_PLUGIN_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("bus").join("plugins"))
}

pub(crate) fn plugin_registry_store_path() -> PathBuf {
    env::var("AIWF_PLUGIN_REGISTRY_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("plugin_registry.json"))
}

pub(crate) fn plugin_runtime_store_path() -> PathBuf {
    env::var("AIWF_PLUGIN_RUNTIME_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("plugin_runtime.json"))
}

pub(crate) fn plugin_tenant_running_map() -> &'static Mutex<HashMap<String, usize>> {
    static RUN: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    RUN.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn plugin_audit_log_path() -> PathBuf {
    env::var("AIWF_PLUGIN_AUDIT_LOG")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("plugin_audit.log"))
}

pub(crate) fn append_plugin_audit(record: &Value) -> Result<(), String> {
    let p = plugin_audit_log_path();
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create plugin audit dir: {e}"))?;
    }
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&p)
        .map_err(|e| format!("open plugin audit log: {e}"))?;
    let line = serde_json::to_string(record).map_err(|e| format!("encode plugin audit: {e}"))?;
    writeln!(f, "{line}").map_err(|e| format!("write plugin audit: {e}"))?;
    Ok(())
}

pub(crate) fn load_plugin_registry_store() -> HashMap<String, Value> {
    load_kv_store(&plugin_registry_store_path())
}

pub(crate) fn save_plugin_registry_store(store: &HashMap<String, Value>) -> Result<(), String> {
    save_kv_store(&plugin_registry_store_path(), store)
}

pub(crate) fn run_plugin_registry_v1(req: PluginRegistryV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let mut store = load_plugin_registry_store();
    match op.as_str() {
        "list" => {
            let mut items = store
                .iter()
                .map(|(k, v)| json!({"plugin": k, "manifest": v}))
                .collect::<Vec<_>>();
            items.sort_by(|a, b| {
                let ak = a
                    .get("plugin")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let bk = b
                    .get("plugin")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                ak.cmp(&bk)
            });
            Ok(
                json!({"ok": true, "operator":"plugin_registry_v1", "status":"done", "run_id": req.run_id, "op": op, "items": items}),
            )
        }
        "get" => {
            let plugin = safe_pkg_token(req.plugin.as_deref().unwrap_or(""))?;
            let manifest = store.get(&plugin).cloned().unwrap_or(Value::Null);
            Ok(
                json!({"ok": true, "operator":"plugin_registry_v1", "status":"done", "run_id": req.run_id, "op": op, "plugin": plugin, "manifest": manifest}),
            )
        }
        "register" | "upsert" => {
            let plugin = safe_pkg_token(req.plugin.as_deref().unwrap_or(""))?;
            let manifest = req.manifest.unwrap_or(Value::Null);
            let pm: PluginManifest = serde_json::from_value(manifest.clone())
                .map_err(|e| format!("plugin manifest invalid: {e}"))?;
            if pm.command.trim().is_empty() {
                return Err("plugin manifest missing command".to_string());
            }
            store.insert(plugin.clone(), manifest);
            save_plugin_registry_store(&store)?;
            Ok(
                json!({"ok": true, "operator":"plugin_registry_v1", "status":"done", "run_id": req.run_id, "op": op, "plugin": plugin, "size": store.len()}),
            )
        }
        "delete" | "unregister" => {
            let plugin = safe_pkg_token(req.plugin.as_deref().unwrap_or(""))?;
            let deleted = store.remove(&plugin).is_some();
            save_plugin_registry_store(&store)?;
            Ok(
                json!({"ok": true, "operator":"plugin_registry_v1", "status":"done", "run_id": req.run_id, "op": op, "plugin": plugin, "deleted": deleted, "size": store.len()}),
            )
        }
        _ => Err(format!("plugin_registry_v1 unsupported op: {}", req.op)),
    }
}

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

pub(crate) fn load_plugin_manifest(plugin: &str) -> Result<PluginManifest, String> {
    let m: PluginManifest = {
        let reg = load_plugin_registry_store();
        if let Some(v) = reg.get(plugin) {
            serde_json::from_value(v.clone())
                .map_err(|e| format!("parse plugin registry config: {e}"))?
        } else {
            let cfg_path = plugin_dir().join(format!("{plugin}.json"));
            let cfg_txt =
                fs::read_to_string(&cfg_path).map_err(|e| format!("read plugin config: {e}"))?;
            serde_json::from_str(&cfg_txt).map_err(|e| format!("parse plugin config: {e}"))?
        }
    };
    if m.command.trim().is_empty() {
        return Err("plugin config missing command".to_string());
    }
    if let Some(n) = &m.name
        && !n.trim().is_empty()
    {
        let nn = safe_pkg_token(n)?;
        if !nn.eq_ignore_ascii_case(plugin) {
            return Err(format!(
                "plugin name mismatch: manifest={nn}, request={plugin}"
            ));
        }
    }
    let api = m
        .api_version
        .as_deref()
        .unwrap_or("v1")
        .trim()
        .to_lowercase();
    if api != "v1" {
        return Err(format!("unsupported plugin api_version: {api}"));
    }
    if let Some(ver) = &m.version
        && ver.trim().is_empty()
    {
        return Err("plugin version is empty".to_string());
    }
    Ok(m)
}

pub(crate) fn run_plugin_healthcheck(plugin: &str, tenant: Option<&str>) -> Result<Value, String> {
    if !plugin_enabled_for_tenant(tenant) {
        return Err("plugin execution disabled for tenant".to_string());
    }
    enforce_plugin_allowlist(plugin)?;
    let m = load_plugin_manifest(plugin)?;
    let cmd = m
        .healthcheck
        .as_ref()
        .and_then(|h| h.command.clone())
        .unwrap_or_else(|| m.command.clone());
    if cmd.trim().is_empty() {
        return Err("plugin healthcheck command is empty".to_string());
    }
    enforce_plugin_command_allowlist(&cmd)?;
    let args = m
        .healthcheck
        .as_ref()
        .and_then(|h| h.args.clone())
        .unwrap_or_else(|| m.args.clone().unwrap_or_default());
    verify_plugin_signature(plugin, &cmd, &args, m.signature.as_deref())?;
    let timeout_ms = m
        .healthcheck
        .as_ref()
        .and_then(|h| h.timeout_ms)
        .or(m.timeout_ms)
        .unwrap_or(3000)
        .min(15_000);
    let max_out = env::var("AIWF_PLUGIN_MAX_OUTPUT_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8 * 1024 * 1024);
    let mut child = Command::new(&cmd)
        .args(&args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn plugin healthcheck: {e}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "plugin health stdout pipe missing".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "plugin health stderr pipe missing".to_string())?;
    let stdout_handle = read_pipe_capped(stdout, max_out, "health_stdout");
    let stderr_handle = read_pipe_capped(stderr, max_out, "health_stderr");
    let start = Instant::now();
    let status = loop {
        if start.elapsed().as_millis() as u64 > timeout_ms {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("plugin healthcheck timeout: {timeout_ms}ms"));
        }
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) => std::thread::sleep(std::time::Duration::from_millis(20)),
            Err(e) => return Err(format!("plugin healthcheck wait error: {e}")),
        }
    };
    let out_stdout = stdout_handle
        .join()
        .map_err(|_| "plugin health stdout reader thread panicked".to_string())??;
    let out_stderr = stderr_handle
        .join()
        .map_err(|_| "plugin health stderr reader thread panicked".to_string())??;
    if out_stdout.len().saturating_add(out_stderr.len()) > max_out {
        return Err("plugin health output exceeds limit".to_string());
    }
    Ok(json!({
        "manifest_name": m.name,
        "manifest_version": m.version,
        "api_version": m.api_version.unwrap_or_else(|| "v1".to_string()),
        "command": cmd,
        "args": args,
        "status_code": status.code(),
        "ok": status.success(),
        "stderr": String::from_utf8_lossy(&out_stderr).to_string(),
    }))
}

pub(crate) fn plugin_enabled_for_tenant(tenant: Option<&str>) -> bool {
    let global_on = env::var("AIWF_PLUGIN_ENABLE")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true");
    if !global_on {
        return false;
    }
    let allowed = env::var("AIWF_PLUGIN_TENANT_ALLOWLIST")
        .ok()
        .unwrap_or_default();
    if allowed.trim().is_empty() {
        return true;
    }
    let t = tenant.unwrap_or("default");
    allowed
        .split(',')
        .map(|s| s.trim())
        .any(|x| !x.is_empty() && x.eq_ignore_ascii_case(t))
}

pub(crate) fn enforce_plugin_allowlist(plugin: &str) -> Result<(), String> {
    let allow = env::var("AIWF_PLUGIN_ALLOWLIST").ok().unwrap_or_default();
    if allow.trim().is_empty() {
        return Err("AIWF_PLUGIN_ALLOWLIST is required when plugin is enabled".to_string());
    }
    if allow
        .split(',')
        .map(|s| s.trim())
        .any(|x| !x.is_empty() && x.eq_ignore_ascii_case(plugin))
    {
        Ok(())
    } else {
        Err(format!("plugin not allowed: {plugin}"))
    }
}

pub(crate) fn enforce_plugin_command_allowlist(cmd: &str) -> Result<(), String> {
    let allow = env::var("AIWF_PLUGIN_COMMAND_ALLOWLIST")
        .ok()
        .unwrap_or_default();
    if allow.trim().is_empty() {
        return Err("AIWF_PLUGIN_COMMAND_ALLOWLIST is required when plugin is enabled".to_string());
    }
    if allow
        .split(',')
        .map(|s| s.trim())
        .any(|x| !x.is_empty() && x.eq_ignore_ascii_case(cmd))
    {
        Ok(())
    } else {
        Err(format!("plugin command not allowed: {cmd}"))
    }
}

pub(crate) fn verify_plugin_signature(
    plugin: &str,
    cmd: &str,
    args: &[String],
    signature: Option<&str>,
) -> Result<(), String> {
    let secret = env::var("AIWF_PLUGIN_SIGNING_SECRET")
        .ok()
        .unwrap_or_default();
    if secret.trim().is_empty() {
        return Err("plugin signing secret not configured".to_string());
    }
    let mut h = Sha256::new();
    h.update(format!("{secret}:{plugin}:{cmd}:{}", args.join("\u{1f}")).as_bytes());
    let expected = format!("{:x}", h.finalize());
    let got = signature.unwrap_or("").trim().to_lowercase();
    if got == expected {
        Ok(())
    } else {
        Err("plugin signature verification failed".to_string())
    }
}

pub(crate) fn read_pipe_capped<R: Read + Send + 'static>(
    mut reader: R,
    cap: usize,
    label: &'static str,
) -> std::thread::JoinHandle<Result<Vec<u8>, String>> {
    std::thread::spawn(move || {
        let mut out = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            let n = reader
                .read(&mut buf)
                .map_err(|e| format!("read plugin {label}: {e}"))?;
            if n == 0 {
                break;
            }
            if out.len().saturating_add(n) > cap {
                return Err(format!(
                    "plugin {label} exceeds limit: {} > {}",
                    out.len() + n,
                    cap
                ));
            }
            out.extend_from_slice(&buf[..n]);
        }
        Ok(out)
    })
}

pub(crate) fn run_plugin_exec_v1(req: PluginExecReq) -> Result<PluginExecResp, String> {
    if !plugin_enabled_for_tenant(req.tenant_id.as_deref()) {
        return Err("plugin execution disabled for tenant".to_string());
    }
    let plugin = safe_pkg_token(&req.plugin)?;
    enforce_plugin_allowlist(&plugin)?;
    let manifest = load_plugin_manifest(&plugin)?;
    let cmd = manifest.command.clone();
    enforce_plugin_command_allowlist(&cmd)?;
    let args = manifest.args.clone().unwrap_or_default();
    let timeout_ms = manifest.timeout_ms.unwrap_or(20_000).min(120_000);
    verify_plugin_signature(&plugin, &cmd, &args, manifest.signature.as_deref())?;
    let trace_id = resolve_trace_id(
        req.trace_id.as_deref(),
        None,
        &format!(
            "plugin:{}:{}:{}",
            plugin,
            req.run_id.clone().unwrap_or_default(),
            utc_now_iso()
        ),
    );
    let payload = json!({
        "run_id": req.run_id,
        "tenant_id": req.tenant_id,
        "trace_id": trace_id,
        "plugin": plugin,
        "input": req.input,
    });
    let max_out = env::var("AIWF_PLUGIN_MAX_OUTPUT_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8 * 1024 * 1024);
    let mut child = Command::new(cmd)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn plugin process: {e}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "plugin stdout pipe missing".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "plugin stderr pipe missing".to_string())?;
    let stdout_handle = read_pipe_capped(stdout, max_out, "stdout");
    let stderr_handle = read_pipe_capped(stderr, max_out, "stderr");

    if let Some(mut stdin) = child.stdin.take() {
        let s = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
        stdin
            .write_all(s.as_bytes())
            .map_err(|e| format!("write plugin stdin: {e}"))?;
    }

    let start = Instant::now();
    let status = loop {
        if start.elapsed().as_millis() as u64 > timeout_ms {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("plugin timeout: {timeout_ms}ms"));
        }
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) => std::thread::sleep(std::time::Duration::from_millis(20)),
            Err(e) => return Err(format!("plugin wait error: {e}")),
        }
    };

    let out_stdout = stdout_handle
        .join()
        .map_err(|_| "plugin stdout reader thread panicked".to_string())??;
    let out_stderr = stderr_handle
        .join()
        .map_err(|_| "plugin stderr reader thread panicked".to_string())??;
    if out_stdout.len().saturating_add(out_stderr.len()) > max_out {
        return Err(format!(
            "plugin output exceeds limit: {} > {}",
            out_stdout.len() + out_stderr.len(),
            max_out
        ));
    }
    let stdout = String::from_utf8_lossy(&out_stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&out_stderr).to_string();
    let output = if stdout.is_empty() {
        Value::Null
    } else {
        serde_json::from_str::<Value>(&stdout).unwrap_or(Value::String(stdout))
    };
    Ok(PluginExecResp {
        ok: status.success(),
        operator: "plugin_exec_v1".to_string(),
        status: if status.success() {
            "done".to_string()
        } else {
            "failed".to_string()
        },
        run_id: req.run_id,
        trace_id,
        plugin,
        output,
        stderr,
    })
}
