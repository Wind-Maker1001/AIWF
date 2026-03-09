use super::*;

pub(crate) fn schema_registry_key(name: &str, version: &str) -> Result<String, String> {
    let n = safe_pkg_token(name)?;
    let v = safe_pkg_token(version)?;
    Ok(format!("{n}@{v}"))
}

pub(crate) fn infer_schema_from_rows(rows: &[Value]) -> Value {
    let mut fields: HashMap<String, HashSet<String>> = HashMap::new();
    for r in rows {
        let Some(obj) = r.as_object() else { continue };
        for (k, v) in obj {
            let t = match v {
                Value::Null => "null",
                Value::Bool(_) => "bool",
                Value::Number(n) => {
                    if n.is_i64() || n.is_u64() {
                        "int"
                    } else {
                        "float"
                    }
                }
                Value::String(s) => {
                    let ts = s.trim().to_ascii_lowercase();
                    if ts == "true" || ts == "false" {
                        "bool_like"
                    } else if s.parse::<i64>().is_ok() {
                        "int_like"
                    } else if s.parse::<f64>().is_ok() {
                        "float_like"
                    } else {
                        "string"
                    }
                }
                Value::Array(_) => "array",
                Value::Object(_) => "object",
            };
            fields.entry(k.clone()).or_default().insert(t.to_string());
        }
    }
    let mut out = Map::new();
    for (k, tset) in fields {
        let t = if tset.contains("string") {
            "string"
        } else if tset.contains("float") || tset.contains("float_like") {
            "float"
        } else if tset.contains("int") || tset.contains("int_like") {
            "int"
        } else if tset.contains("bool") || tset.contains("bool_like") {
            "bool"
        } else if tset.contains("object") {
            "object"
        } else if tset.contains("array") {
            "array"
        } else {
            "unknown"
        };
        out.insert(k, Value::String(t.to_string()));
    }
    Value::Object(out)
}

pub(crate) fn schema_registry_store_path() -> PathBuf {
    env::var("AIWF_SCHEMA_REGISTRY_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("schema_registry.json"))
}

pub(crate) fn load_schema_registry_store() -> HashMap<String, Value> {
    let p = schema_registry_store_path();
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

pub(crate) fn save_schema_registry_store(store: &HashMap<String, Value>) -> Result<(), String> {
    let p = schema_registry_store_path();
    let lock = acquire_file_lock(&p)?;
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create schema store dir: {e}"))?;
    }
    let s =
        serde_json::to_string_pretty(store).map_err(|e| format!("serialize schema store: {e}"))?;
    let out = fs::write(&p, s).map_err(|e| format!("write schema store: {e}"));
    release_file_lock(&lock);
    out
}

pub(crate) fn run_schema_registry_register_local(
    req: &SchemaRegisterReq,
) -> Result<SchemaRegistryResp, String> {
    let key = schema_registry_key(&req.name, &req.version)?;
    let mut store = load_schema_registry_store();
    store.insert(key, req.schema.clone());
    save_schema_registry_store(&store)?;
    Ok(SchemaRegistryResp {
        ok: true,
        operator: "schema_registry_v1_register".to_string(),
        status: "done".to_string(),
        name: req.name.clone(),
        version: req.version.clone(),
        schema: req.schema.clone(),
    })
}

pub(crate) fn run_schema_registry_get_local(
    req: &SchemaGetReq,
) -> Result<SchemaRegistryResp, String> {
    let key = schema_registry_key(&req.name, &req.version)?;
    let store = load_schema_registry_store();
    let schema = store
        .get(&key)
        .cloned()
        .ok_or_else(|| "schema not found".to_string())?;
    Ok(SchemaRegistryResp {
        ok: true,
        operator: "schema_registry_v1_get".to_string(),
        status: "done".to_string(),
        name: req.name.clone(),
        version: req.version.clone(),
        schema,
    })
}

pub(crate) fn run_schema_registry_infer_local(
    req: &SchemaInferReq,
) -> Result<SchemaInferResp, String> {
    let schema = infer_schema_from_rows(&req.rows);
    if let (Some(name), Some(version)) = (req.name.as_ref(), req.version.as_ref()) {
        let key = schema_registry_key(name, version)?;
        let mut store = load_schema_registry_store();
        store.insert(key, schema.clone());
        save_schema_registry_store(&store)?;
    }
    Ok(SchemaInferResp {
        ok: true,
        operator: "schema_registry_v1_infer".to_string(),
        status: "done".to_string(),
        name: req.name.clone(),
        version: req.version.clone(),
        schema: schema.clone(),
        stats: json!({"rows": req.rows.len(), "fields": schema.as_object().map(|m| m.len()).unwrap_or(0)}),
    })
}
