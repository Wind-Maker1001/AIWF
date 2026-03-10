use super::*;

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
