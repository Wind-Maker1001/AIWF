use super::*;

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
