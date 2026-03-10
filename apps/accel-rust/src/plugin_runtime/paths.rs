use super::*;

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
