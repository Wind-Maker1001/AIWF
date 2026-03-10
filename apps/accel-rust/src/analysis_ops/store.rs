use super::*;

pub(crate) fn feature_store_path() -> PathBuf {
    env::var("AIWF_FEATURE_STORE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("feature_store.json"))
}

pub(crate) fn load_feature_store() -> HashMap<String, Value> {
    let p = feature_store_path();
    if let Ok(lock) = acquire_file_lock(&p) {
        let out = (|| {
            let Ok(txt) = fs::read_to_string(&p) else {
                return HashMap::new();
            };
            serde_json::from_str::<HashMap<String, Value>>(&txt).unwrap_or_default()
        })();
        release_file_lock(&lock);
        return out;
    }
    let Ok(txt) = fs::read_to_string(&p) else {
        return HashMap::new();
    };
    serde_json::from_str::<HashMap<String, Value>>(&txt).unwrap_or_default()
}

pub(crate) fn save_feature_store(store: &HashMap<String, Value>) -> Result<(), String> {
    let p = feature_store_path();
    let lock = acquire_file_lock(&p)?;
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create feature store dir: {e}"))?;
    }
    let s =
        serde_json::to_string_pretty(store).map_err(|e| format!("serialize feature store: {e}"))?;
    let out = fs::write(&p, s).map_err(|e| format!("write feature store: {e}"));
    release_file_lock(&lock);
    out
}

pub(crate) fn run_feature_store_upsert_v1(req: FeatureStoreUpsertReq) -> Result<Value, String> {
    let mut store = load_feature_store();
    let mut upserted = 0usize;
    for r in req.rows {
        let Some(obj) = r.as_object() else { continue };
        let key = value_to_string_or_null(obj.get(&req.key_field));
        if key.trim().is_empty() || key == "null" {
            continue;
        }
        store.insert(key, Value::Object(obj.clone()));
        upserted += 1;
    }
    save_feature_store(&store)?;
    Ok(json!({
        "ok": true,
        "operator": "feature_store_v1_upsert",
        "status": "done",
        "run_id": req.run_id,
        "upserted": upserted,
        "total_keys": store.len()
    }))
}

pub(crate) fn run_feature_store_get_v1(req: FeatureStoreGetReq) -> Result<Value, String> {
    let store = load_feature_store();
    Ok(json!({
        "ok": true,
        "operator": "feature_store_v1_get",
        "status": "done",
        "run_id": req.run_id,
        "key": req.key,
        "value": store.get(&req.key).cloned().unwrap_or(Value::Null)
    }))
}
