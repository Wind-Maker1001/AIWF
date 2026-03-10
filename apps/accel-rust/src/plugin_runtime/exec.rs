use super::*;

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
