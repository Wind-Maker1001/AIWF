use super::*;

pub(super) fn collect_parquet_paths(
    base: &Path,
    recursive: bool,
    out: &mut Vec<PathBuf>,
) -> Result<(), String> {
    let rd = fs::read_dir(base).map_err(|e| format!("read parquet dir: {e}"))?;
    for ent in rd {
        let ent = ent.map_err(|e| format!("read parquet dir entry: {e}"))?;
        let p = ent.path();
        if p.is_dir() && recursive {
            collect_parquet_paths(&p, true, out)?;
        } else if p
            .extension()
            .and_then(|x| x.to_str())
            .map(|x| x.eq_ignore_ascii_case("parquet"))
            .unwrap_or(false)
        {
            out.push(p);
        }
    }
    Ok(())
}
pub(super) fn partition_predicate_match(
    path: &Path,
    field: Option<&str>,
    eqv: Option<&Value>,
) -> bool {
    let Some(field) = field else { return true };
    let Some(eqv) = eqv else { return true };
    let target = value_to_string(eqv);
    for seg in path.components() {
        let s = seg.as_os_str().to_string_lossy();
        if let Some((k, v)) = s.split_once('=')
            && k == field
        {
            return v == target;
        }
    }
    true
}
pub(super) fn schema_file_path(base_path: &str, partitioned: bool) -> PathBuf {
    let p = Path::new(base_path);
    if partitioned || p.is_dir() || !base_path.to_lowercase().ends_with(".parquet") {
        return p.join("_schema.json");
    }
    PathBuf::from(format!("{base_path}.schema.json"))
}
fn json_type_name(v: &Value) -> String {
    if v.is_boolean() {
        "bool".to_string()
    } else if v.is_i64() || v.is_u64() {
        "int".to_string()
    } else if v.is_f64() {
        "float".to_string()
    } else if v.is_null() {
        "null".to_string()
    } else {
        "string".to_string()
    }
}
pub(super) fn infer_rows_schema(rows: &[Value]) -> Map<String, Value> {
    let mut out = Map::new();
    for r in rows {
        let Some(o) = r.as_object() else {
            continue;
        };
        for (k, v) in o {
            let t = json_type_name(v);
            let prev = out.get(k).and_then(|x| x.as_str()).unwrap_or("");
            let merged = if prev.is_empty() || prev == t {
                t
            } else if (prev == "int" && t == "float") || (prev == "float" && t == "int") {
                "float".to_string()
            } else {
                "string".to_string()
            };
            out.insert(k.clone(), json!(merged));
        }
    }
    out
}
pub(super) fn schema_compatible(
    old_s: &Map<String, Value>,
    new_s: &Map<String, Value>,
    mode: &str,
) -> bool {
    let m = mode.trim().to_lowercase();
    if m == "strict" && old_s.len() != new_s.len() {
        return false;
    }
    for (k, ov) in old_s {
        let Some(nv) = new_s.get(k) else {
            return false;
        };
        let o = ov.as_str().unwrap_or("");
        let n = nv.as_str().unwrap_or("");
        if o == n {
            continue;
        }
        if m == "widen" && ((o == "int" && n == "float") || n == "string") {
            continue;
        }
        return false;
    }
    true
}
pub(super) fn apply_schema_columns(rows: &mut [Value], schema: &Map<String, Value>) {
    for r in rows {
        let Some(o) = r.as_object_mut() else {
            continue;
        };
        for k in schema.keys() {
            if !o.contains_key(k) {
                o.insert(k.clone(), Value::Null);
            }
        }
    }
}
