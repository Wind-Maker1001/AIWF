use super::*;

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
