use super::*;

pub(crate) fn compile_rules_dsl(dsl: &str) -> Result<Value, String> {
    let mut rename = Map::new();
    let mut casts = Map::new();
    let mut filters: Vec<Value> = Vec::new();
    let mut required: Vec<Value> = Vec::new();
    for (idx, raw) in dsl.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("rename ") {
            let parts: Vec<&str> = rest.split("->").map(|x| x.trim()).collect();
            if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                return Err(format!("dsl line {} invalid rename", idx + 1));
            }
            rename.insert(parts[0].to_string(), Value::String(parts[1].to_string()));
            continue;
        }
        if let Some(rest) = line.strip_prefix("cast ") {
            let parts: Vec<&str> = rest.split(':').map(|x| x.trim()).collect();
            if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                return Err(format!("dsl line {} invalid cast", idx + 1));
            }
            casts.insert(parts[0].to_string(), Value::String(parts[1].to_lowercase()));
            continue;
        }
        if let Some(rest) = line.strip_prefix("required ") {
            let f = rest.trim();
            if f.is_empty() {
                return Err(format!("dsl line {} invalid required", idx + 1));
            }
            required.push(Value::String(f.to_string()));
            continue;
        }
        if let Some(rest) = line.strip_prefix("filter ") {
            let expr = rest.trim();
            let candidates = ["<=", ">=", "==", "!=", ">", "<"];
            let mut hit: Option<(&str, usize)> = None;
            for op in candidates {
                if let Some(p) = expr.find(op) {
                    hit = Some((op, p));
                    break;
                }
            }
            let Some((op, pos)) = hit else {
                return Err(format!("dsl line {} invalid filter", idx + 1));
            };
            let left = expr[..pos].trim();
            let right = expr[pos + op.len()..].trim().trim_matches('"');
            if left.is_empty() {
                return Err(format!("dsl line {} invalid filter lhs", idx + 1));
            }
            let mapped = match op {
                ">" => "gt",
                ">=" => "gte",
                "<" => "lt",
                "<=" => "lte",
                "==" => "eq",
                "!=" => "ne",
                _ => "eq",
            };
            let value = if let Ok(n) = right.parse::<f64>() {
                serde_json::Number::from_f64(n)
                    .map(Value::Number)
                    .unwrap_or_else(|| Value::String(right.to_string()))
            } else {
                Value::String(right.to_string())
            };
            filters.push(json!({"field": left, "op": mapped, "value": value}));
            continue;
        }
        return Err(format!("dsl line {} unsupported statement", idx + 1));
    }
    Ok(json!({
        "rename_map": rename,
        "casts": casts,
        "filters": filters,
        "required_fields": required
    }))
}

pub(crate) fn rules_pkg_base_dir() -> PathBuf {
    env::var("AIWF_RULES_PACKAGE_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("bus").join("rules_packages"))
}

pub(crate) fn stream_checkpoint_dir() -> PathBuf {
    env::var("AIWF_STREAM_CHECKPOINT_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("bus").join("stream_checkpoints"))
}

pub(crate) fn checkpoint_path(key: &str) -> Result<PathBuf, String> {
    let k = safe_pkg_token(key)?;
    Ok(stream_checkpoint_dir().join(format!("{k}.json")))
}

pub(crate) fn write_stream_checkpoint(key: &str, chunk_idx: usize) -> Result<(), String> {
    let path = checkpoint_path(key)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create checkpoint dir: {e}"))?;
    }
    let payload =
        json!({"checkpoint_key": key, "last_chunk": chunk_idx, "updated_at": crate::utc_now_iso()});
    fs::write(
        &path,
        serde_json::to_string_pretty(&payload).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("write checkpoint: {e}"))
}

pub(crate) fn read_stream_checkpoint(key: &str) -> Result<Option<usize>, String> {
    let path = checkpoint_path(key)?;
    if !path.exists() {
        return Ok(None);
    }
    let txt = fs::read_to_string(&path).map_err(|e| format!("read checkpoint: {e}"))?;
    let v: Value = serde_json::from_str(&txt).map_err(|e| format!("parse checkpoint: {e}"))?;
    Ok(v.get("last_chunk")
        .and_then(|x| x.as_u64())
        .map(|x| x as usize))
}

pub(crate) fn safe_pkg_token(s: &str) -> Result<String, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty package token".to_string());
    }
    if t.chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.')
    {
        Ok(t.to_string())
    } else {
        Err("package token contains invalid characters".to_string())
    }
}

pub(crate) fn rules_pkg_path(name: &str, version: &str) -> Result<PathBuf, String> {
    let n = safe_pkg_token(name)?;
    let v = safe_pkg_token(version)?;
    Ok(rules_pkg_base_dir().join(format!("{n}__{v}.json")))
}

pub(crate) fn run_rules_package_publish_v1(
    req: RulesPackagePublishReq,
) -> Result<RulesPackageResp, String> {
    let mut rules = req.rules.unwrap_or(Value::Null);
    if rules.is_null() {
        let dsl = req.dsl.unwrap_or_default();
        if dsl.trim().is_empty() {
            return Err("rules or dsl is required".to_string());
        }
        rules = compile_rules_dsl(&dsl)?;
    }
    let path = rules_pkg_path(&req.name, &req.version)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create rules package dir: {e}"))?;
    }
    let rules_text = serde_json::to_string_pretty(&rules).map_err(|e| e.to_string())?;
    let mut h = Sha256::new();
    h.update(rules_text.as_bytes());
    let fingerprint = format!("{:x}", h.finalize());
    let payload = json!({
        "name": req.name,
        "version": req.version,
        "fingerprint": fingerprint,
        "rules": rules,
    });
    fs::write(
        &path,
        serde_json::to_string_pretty(&payload).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("write rules package: {e}"))?;
    Ok(RulesPackageResp {
        ok: true,
        operator: "rules_package_publish_v1".to_string(),
        status: "done".to_string(),
        name: req.name,
        version: req.version,
        rules: payload.get("rules").cloned().unwrap_or_else(|| json!({})),
        fingerprint,
    })
}

pub(crate) fn run_rules_package_get_v1(
    req: RulesPackageGetReq,
) -> Result<RulesPackageResp, String> {
    let path = rules_pkg_path(&req.name, &req.version)?;
    let txt = fs::read_to_string(&path).map_err(|e| format!("read rules package: {e}"))?;
    let v: Value = serde_json::from_str(&txt).map_err(|e| format!("parse rules package: {e}"))?;
    let rules = v.get("rules").cloned().unwrap_or_else(|| json!({}));
    let fingerprint = v
        .get("fingerprint")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    Ok(RulesPackageResp {
        ok: true,
        operator: "rules_package_get_v1".to_string(),
        status: "done".to_string(),
        name: req.name,
        version: req.version,
        rules,
        fingerprint,
    })
}
