use crate::*;

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

pub(crate) fn run_schema_registry_register_v1(
    state: &AppState,
    req: SchemaRegisterReq,
) -> Result<SchemaRegistryResp, String> {
    let key = schema_registry_key(&req.name, &req.version)?;
    let resp = run_schema_registry_register_local(&req)?;
    if let Ok(mut reg) = state.schema_registry.lock() {
        reg.insert(key, req.schema.clone());
    }
    Ok(resp)
}

pub(crate) fn run_schema_registry_get_v1(
    state: &AppState,
    req: SchemaGetReq,
) -> Result<SchemaRegistryResp, String> {
    let key = schema_registry_key(&req.name, &req.version)?;
    if let Ok(reg) = state.schema_registry.lock()
        && let Some(schema) = reg.get(&key).cloned()
    {
        return Ok(SchemaRegistryResp {
            ok: true,
            operator: "schema_registry_v1_get".to_string(),
            status: "done".to_string(),
            name: req.name,
            version: req.version,
            schema,
        });
    }
    run_schema_registry_get_local(&req)
}

pub(crate) fn run_schema_registry_infer_v1(
    state: &AppState,
    req: SchemaInferReq,
) -> Result<SchemaInferResp, String> {
    let resp = run_schema_registry_infer_local(&req)?;
    if let (Some(name), Some(version)) = (req.name.as_ref(), req.version.as_ref())
        && let Ok(key) = schema_registry_key(name, version)
        && let Ok(mut reg) = state.schema_registry.lock()
    {
        reg.insert(key, resp.schema.clone());
    }
    Ok(resp)
}

pub(crate) fn schema_field_map(schema: &Value) -> HashMap<String, String> {
    schema
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("unknown").to_string()))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default()
}

pub(crate) fn run_schema_registry_check_compat_v2(
    state: &AppState,
    req: SchemaCompatReq,
) -> Result<SchemaCompatResp, String> {
    let from = run_schema_registry_get_v1(
        state,
        SchemaGetReq {
            name: req.name.clone(),
            version: req.from_version.clone(),
        },
    )?;
    let to = run_schema_registry_get_v1(
        state,
        SchemaGetReq {
            name: req.name.clone(),
            version: req.to_version.clone(),
        },
    )?;
    let from_map = schema_field_map(&from.schema);
    let to_map = schema_field_map(&to.schema);
    let mode = req
        .mode
        .unwrap_or_else(|| "backward".to_string())
        .to_lowercase();
    let mut breaking = Vec::new();
    let mut widening = Vec::new();
    for (k, old_t) in &from_map {
        match to_map.get(k) {
            None => breaking.push(format!("{k}: removed")),
            Some(new_t) if new_t != old_t => {
                let widening_pair = (old_t.as_str(), new_t.as_str());
                if matches!(
                    widening_pair,
                    ("int", "float") | ("int_like", "float") | ("bool_like", "string")
                ) {
                    widening.push(format!("{k}: {old_t}->{new_t}"));
                } else {
                    breaking.push(format!("{k}: {old_t}->{new_t}"));
                }
            }
            _ => {}
        }
    }
    if mode == "forward" || mode == "full" {
        for k in to_map.keys() {
            if !from_map.contains_key(k) && mode == "full" {
                breaking.push(format!("{k}: added in full mode"));
            }
        }
    }
    Ok(SchemaCompatResp {
        ok: true,
        operator: "schema_registry_v2_check_compat".to_string(),
        status: "done".to_string(),
        compatible: breaking.is_empty(),
        mode,
        breaking_fields: breaking,
        widening_fields: widening,
    })
}

pub(crate) fn run_schema_registry_suggest_migration_v2(
    state: &AppState,
    req: SchemaMigrationSuggestReq,
) -> Result<SchemaMigrationSuggestResp, String> {
    let compat = run_schema_registry_check_compat_v2(
        state,
        SchemaCompatReq {
            name: req.name.clone(),
            from_version: req.from_version.clone(),
            to_version: req.to_version.clone(),
            mode: Some("backward".to_string()),
        },
    )?;
    let mut steps = Vec::new();
    for b in compat.breaking_fields {
        if b.contains("removed") {
            let f = b.split(':').next().unwrap_or("").to_string();
            steps.push(json!({"action":"add_default","field":f,"default":Value::Null}));
        } else if b.contains("->") {
            let f = b.split(':').next().unwrap_or("").to_string();
            steps.push(json!({"action":"cast","field":f,"strategy":"safe_cast_or_null"}));
        }
    }
    for w in compat.widening_fields {
        let f = w.split(':').next().unwrap_or("").to_string();
        steps.push(json!({"action":"cast","field":f,"strategy":"widen"}));
    }
    Ok(SchemaMigrationSuggestResp {
        ok: true,
        operator: "schema_registry_v2_suggest_migration".to_string(),
        status: "done".to_string(),
        steps,
    })
}
