use super::*;

pub(crate) fn normalize_entity(s: &str) -> String {
    s.trim()
        .to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c.is_alphanumeric() {
                c
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn run_entity_linking_v1(req: EntityLinkReq) -> Result<Value, String> {
    let id_field = req.id_field.unwrap_or_else(|| "entity_id".to_string());
    let mut out = Vec::new();
    let mut dict = Map::new();
    for r in req.rows {
        let Some(mut obj) = r.as_object().cloned() else {
            continue;
        };
        let raw = value_to_string_or_null(obj.get(&req.field));
        let norm = normalize_entity(&raw);
        let mut h = Sha256::new();
        h.update(norm.as_bytes());
        let id = format!("{:x}", h.finalize());
        let short = id.chars().take(12).collect::<String>();
        obj.insert(id_field.clone(), Value::String(short.clone()));
        obj.insert("entity_norm".to_string(), Value::String(norm.clone()));
        dict.insert(short, Value::String(norm));
        out.push(Value::Object(obj));
    }
    Ok(json!({
        "ok": true,
        "operator": "entity_linking_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out,
        "dictionary": dict
    }))
}

pub(crate) fn run_table_reconstruct_v1(req: TableReconstructReq) -> Result<Value, String> {
    let lines = if let Some(v) = req.lines {
        v
    } else {
        req.text
            .unwrap_or_default()
            .replace("\r\n", "\n")
            .split('\n')
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    };
    let delim = req.delimiter.unwrap_or_else(|| "\\s{2,}|\\t".to_string());
    let re = Regex::new(&delim).map_err(|e| format!("invalid delimiter regex: {e}"))?;
    let mut rows = Vec::new();
    let mut max_cols = 0usize;
    for line in lines {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let cols = re
            .split(t)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        if cols.is_empty() {
            continue;
        }
        max_cols = max_cols.max(cols.len());
        rows.push(cols);
    }
    if rows.is_empty() {
        return Err("table_reconstruct_v1 empty input".to_string());
    }
    let mut norm = Vec::new();
    for mut r in rows {
        if r.len() < max_cols {
            r.resize(max_cols, "".to_string());
        } else if r.len() > max_cols {
            r.truncate(max_cols);
        }
        norm.push(r);
    }
    let header = norm.first().cloned().unwrap_or_default();
    let body = norm
        .iter()
        .skip(1)
        .map(|r| json!({"cells": r, "col_count": r.len()}))
        .collect::<Vec<_>>();
    Ok(json!({
        "ok": true,
        "operator": "table_reconstruct_v1",
        "status": "done",
        "run_id": req.run_id,
        "header": header,
        "rows": body,
        "stats": {"col_count": max_cols, "row_count": norm.len()}
    }))
}
