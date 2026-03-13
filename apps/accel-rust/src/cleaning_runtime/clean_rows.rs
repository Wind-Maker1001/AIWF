use super::*;

pub(crate) fn resolve_job_root(input_root: Option<&str>, job_id: &str) -> Result<PathBuf, String> {
    fn is_valid_job_id(s: &str) -> bool {
        let t = s.trim();
        if t.len() < 8 || t.len() > 128 {
            return false;
        }
        t.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    }

    fn normalize_path(p: &Path) -> PathBuf {
        use std::path::Component;
        let mut out = PathBuf::new();
        for c in p.components() {
            match c {
                Component::CurDir => {}
                Component::ParentDir => {
                    let _ = out.pop();
                }
                other => out.push(other.as_os_str()),
            }
        }
        out
    }

    let jid = job_id.trim();
    if !is_valid_job_id(jid) {
        return Err("invalid job_id".to_string());
    }

    let bus = env::var("AIWF_BUS").unwrap_or_else(|_| "R:\\aiwf".to_string());
    let allowed_root = normalize_path(&PathBuf::from(bus).join("jobs"));
    let requested = if let Some(v) = input_root {
        if v.trim().is_empty() {
            allowed_root.join(jid)
        } else {
            PathBuf::from(v)
        }
    } else {
        allowed_root.join(jid)
    };

    let absolute = if requested.is_absolute() {
        requested
    } else {
        std::env::current_dir()
            .map_err(|e| format!("resolve current dir: {e}"))?
            .join(requested)
    };
    let normalized = normalize_path(&absolute);

    let leaf_ok = normalized.file_name().and_then(|n| n.to_str()) == Some(jid);
    let in_scope = normalized.starts_with(&allowed_root);
    if !leaf_ok || !in_scope {
        return Err(format!(
            "job_root must be under '{}' and end with job_id",
            allowed_root.to_string_lossy()
        ));
    }

    Ok(normalized)
}

pub(crate) fn rule_value<'a>(params: &'a Value, key: &str) -> Option<&'a Value> {
    if let Some(rules) = params.get("rules").and_then(|v| v.as_object())
        && let Some(v) = rules.get(key)
    {
        return Some(v);
    }
    params.get(key)
}

pub(crate) fn value_as_bool(v: Option<&Value>, default: bool) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => {
            let l = s.trim().to_lowercase();
            matches!(l.as_str(), "1" | "true" | "yes" | "on")
        }
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) != 0,
        _ => default,
    }
}

pub(crate) fn value_as_i32(v: Option<&Value>, default: i32) -> i32 {
    match v {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(default as i64) as i32,
        Some(Value::String(s)) => s.trim().parse::<i32>().unwrap_or(default),
        _ => default,
    }
}

pub(crate) fn value_as_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => parse_amount(s),
        _ => None,
    }
}

pub(crate) fn parse_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|x| x as i64)),
        Value::String(s) => s.trim().parse::<f64>().ok().map(|x| x as i64),
        _ => None,
    }
}

pub(crate) fn parse_amount(s: &str) -> Option<f64> {
    let mut t = s.trim().replace(',', "");
    if t.starts_with('$') {
        t = t[1..].to_string();
    }
    t.parse::<f64>().ok()
}

pub(crate) fn parse_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => parse_amount(s),
        _ => None,
    }
}

pub(crate) fn round_half_up(v: f64, digits: i32) -> f64 {
    let factor = 10f64.powi(digits.max(0));
    (v * factor).round() / factor
}

pub(crate) fn load_and_clean_rows(params_opt: Option<&Value>) -> Result<Vec<CleanRow>, String> {
    let Some(params) = params_opt else {
        return Err("no input rows provided; params.rows is required".to_string());
    };

    let rows_val = params.get("rows");
    let Some(rows_arr) = rows_val.and_then(|v| v.as_array()) else {
        return Err("params.rows is required".to_string());
    };
    if rows_arr.is_empty() {
        return Err("params.rows is empty".to_string());
    };

    let id_field = rule_value(params, "id_field")
        .and_then(|v| v.as_str())
        .unwrap_or("id")
        .to_string();
    let amount_field = rule_value(params, "amount_field")
        .and_then(|v| v.as_str())
        .unwrap_or("amount")
        .to_string();
    let drop_negative = value_as_bool(rule_value(params, "drop_negative_amount"), false);
    let deduplicate = value_as_bool(rule_value(params, "deduplicate_by_id"), true);
    let dedup_keep = rule_value(params, "deduplicate_keep")
        .and_then(|v| v.as_str())
        .unwrap_or("last")
        .to_lowercase();
    let sort_by_id = value_as_bool(rule_value(params, "sort_by_id"), true);
    let digits = value_as_i32(rule_value(params, "amount_round_digits"), 2).clamp(0, 6);
    let min_amount = value_as_f64(rule_value(params, "min_amount"));
    let max_amount = value_as_f64(rule_value(params, "max_amount"));

    let mut normalized: Vec<(i64, f64)> = Vec::new();
    for r in rows_arr {
        let Some(obj) = r.as_object() else {
            continue;
        };
        let id_val = obj.get(&id_field).and_then(parse_i64);
        let amount_val = obj.get(&amount_field).and_then(parse_f64);
        let (Some(id), Some(amount)) = (id_val, amount_val) else {
            continue;
        };
        if drop_negative && amount < 0.0 {
            continue;
        }
        if let Some(min_v) = min_amount
            && amount < min_v
        {
            continue;
        }
        if let Some(max_v) = max_amount
            && amount > max_v
        {
            continue;
        }
        normalized.push((id, round_half_up(amount, digits)));
    }

    let mut cleaned: Vec<(i64, f64)> = if deduplicate {
        use std::collections::HashMap;
        let mut map: HashMap<i64, f64> = HashMap::new();
        if dedup_keep == "first" {
            for (id, amount) in &normalized {
                map.entry(*id).or_insert(*amount);
            }
        } else {
            for (id, amount) in &normalized {
                map.insert(*id, *amount);
            }
        }
        map.into_iter().collect()
    } else {
        normalized
    };

    if sort_by_id {
        cleaned.sort_by_key(|x| x.0);
    }

    let out = cleaned
        .into_iter()
        .map(|(id, amount)| CleanRow { id, amount })
        .collect::<Vec<_>>();
    if out.is_empty() {
        return Ok(Vec::new());
    }
    Ok(out)
}
