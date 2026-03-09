use crate::{
    api_types::{
        AggregatePushdownReq, AggregatePushdownResp, EntityExtractReq, EntityExtractResp,
        NormalizeSchemaReq, NormalizeSchemaResp, RulesPackageGetReq, RulesPackagePublishReq,
        RulesPackageResp, TextPreprocessReq, TextPreprocessResp,
    },
    operators::analytics::parse_agg_specs,
    row_io::{load_sqlite_rows, load_sqlserver_rows},
    transform_support::{
        collapse_ws, validate_sql_identifier, validate_where_clause, value_to_string_or_null,
    },
};
use regex::Regex;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf};

pub(crate) fn run_text_preprocess_v2(req: TextPreprocessReq) -> Result<TextPreprocessResp, String> {
    let mut lines: Vec<String> = req
        .text
        .replace("\r\n", "\n")
        .split('\n')
        .map(|s| s.to_string())
        .collect();
    let remove_refs = req.remove_references.unwrap_or(true);
    let remove_notes = req.remove_notes.unwrap_or(true);
    let normalize_ws = req.normalize_whitespace.unwrap_or(true);
    let mut removed_references_lines = 0usize;
    let mut removed_notes_lines = 0usize;

    if remove_refs {
        let mut cut_idx: Option<usize> = None;
        for (i, line) in lines.iter().enumerate() {
            let t = line.trim().to_lowercase();
            if t == "references" || t == "bibliography" || t == "参考文献" || t == "引用文献"
            {
                cut_idx = Some(i);
                break;
            }
        }
        if let Some(i) = cut_idx {
            removed_references_lines = lines.len().saturating_sub(i);
            lines = lines.into_iter().take(i).collect();
        }
    }

    if remove_notes {
        let mut out: Vec<String> = Vec::new();
        for line in lines {
            let t = line.trim();
            if t.starts_with('[') && t.contains(']') && t.len() < 24 {
                removed_notes_lines += 1;
                continue;
            }
            if t.to_lowercase().starts_with("footnote")
                || t.starts_with("注释")
                || t.starts_with("脚注")
            {
                removed_notes_lines += 1;
                continue;
            }
            out.push(line);
        }
        lines = out;
    }

    if normalize_ws {
        lines = lines
            .into_iter()
            .map(|x| collapse_ws(&x))
            .collect::<Vec<String>>();
    }

    if lines.is_empty() {
        return Err("text_preprocess_v2 produced empty content".to_string());
    }

    let mut markdown = String::new();
    if let Some(title) = req.title {
        let t = title.trim();
        if !t.is_empty() {
            markdown.push_str("# ");
            markdown.push_str(t);
            markdown.push_str("\n\n");
        }
    }
    markdown.push_str(lines.join("\n").trim());
    markdown.push('\n');

    let mut hasher = Sha256::new();
    hasher.update(markdown.as_bytes());
    let sha256 = format!("{:x}", hasher.finalize());
    Ok(TextPreprocessResp {
        ok: true,
        operator: "text_preprocess_v2".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        markdown,
        removed_references_lines,
        removed_notes_lines,
        sha256,
    })
}

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

pub(crate) fn run_normalize_schema_v1(
    req: NormalizeSchemaReq,
) -> Result<NormalizeSchemaResp, String> {
    let mut out = Vec::new();
    let mut filled_defaults = 0usize;
    let schema = req.schema;
    let fields = schema
        .get("fields")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let defaults = schema
        .get("defaults")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    for row in req.rows {
        let Some(obj) = row.as_object() else {
            continue;
        };
        let mut next = obj.clone();
        for f in &fields {
            let Some(field) = f.as_str() else {
                continue;
            };
            if !next.contains_key(field) {
                if let Some(v) = defaults.get(field) {
                    next.insert(field.to_string(), v.clone());
                    filled_defaults += 1;
                } else {
                    next.insert(field.to_string(), Value::Null);
                }
            }
        }
        out.push(Value::Object(next));
    }
    Ok(NormalizeSchemaResp {
        ok: true,
        operator: "normalize_schema_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        rows: out,
        stats: json!({"filled_defaults": filled_defaults}),
    })
}

pub(crate) fn run_entity_extract_v1(req: EntityExtractReq) -> Result<EntityExtractResp, String> {
    let email_re =
        Regex::new(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}").map_err(|e| e.to_string())?;
    let url_re = Regex::new(r"https?://[^\s)]+").map_err(|e| e.to_string())?;
    let num_re = Regex::new(r"\b\d+(?:\.\d+)?\b").map_err(|e| e.to_string())?;
    let mut text = String::new();
    if let Some(t) = req.text {
        text.push_str(&t);
        text.push('\n');
    }
    if let Some(rows) = req.rows {
        let field = req.text_field.unwrap_or_else(|| "text".to_string());
        for r in rows {
            if let Some(obj) = r.as_object() {
                text.push_str(&value_to_string_or_null(obj.get(&field)));
                text.push('\n');
            }
        }
    }
    let emails: Vec<Value> = email_re
        .find_iter(&text)
        .map(|m| Value::String(m.as_str().to_string()))
        .collect();
    let urls: Vec<Value> = url_re
        .find_iter(&text)
        .map(|m| Value::String(m.as_str().to_string()))
        .collect();
    let nums: Vec<Value> = num_re
        .find_iter(&text)
        .take(2000)
        .map(|m| Value::String(m.as_str().to_string()))
        .collect();
    Ok(EntityExtractResp {
        ok: true,
        operator: "entity_extract_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        entities: json!({"emails": emails, "urls": urls, "numbers": nums}),
    })
}

pub(crate) fn run_aggregate_pushdown_v1(
    req: AggregatePushdownReq,
) -> Result<AggregatePushdownResp, String> {
    if req.group_by.is_empty() {
        return Err("group_by is empty".to_string());
    }
    let from = req.from.as_deref().unwrap_or("data").trim().to_string();
    if from.is_empty() {
        return Err("from is empty".to_string());
    }
    let from = validate_sql_identifier(&from)?;
    let group_by = req
        .group_by
        .iter()
        .map(|g| validate_sql_identifier(g))
        .collect::<Result<Vec<_>, String>>()?;
    let specs = parse_agg_specs(&req.aggregates)?;
    let select_group = group_by.join(", ");
    let select_aggs = specs
        .iter()
        .map(|s| match s.op.as_str() {
            "count" => Ok(format!(
                "COUNT(1) AS {}",
                validate_sql_identifier(&s.as_name)?
            )),
            "sum" => Ok(format!(
                "SUM({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            "avg" => Ok(format!(
                "AVG({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            "min" => Ok(format!(
                "MIN({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            "max" => Ok(format!(
                "MAX({}) AS {}",
                validate_sql_identifier(&s.field.clone().unwrap_or_default())?,
                validate_sql_identifier(&s.as_name)?
            )),
            _ => Err("unsupported aggregate op".to_string()),
        })
        .collect::<Result<Vec<_>, String>>()?
        .join(", ");
    let where_sql = req
        .where_sql
        .as_deref()
        .map(validate_where_clause)
        .transpose()?
        .map(|w| format!(" WHERE {w}"))
        .unwrap_or_default();
    let limit = req.limit.unwrap_or(10000).max(1);
    let sql = format!(
        "SELECT {select_group}, {select_aggs} FROM {from}{where_sql} GROUP BY {select_group}"
    );
    let rows = match req.source_type.to_lowercase().as_str() {
        "sqlite" => load_sqlite_rows(&req.source, &sql, limit)?,
        "sqlserver" => load_sqlserver_rows(&req.source, &sql, limit)?,
        _ => return Err("source_type must be sqlite or sqlserver".to_string()),
    };
    Ok(AggregatePushdownResp {
        ok: true,
        operator: "aggregate_pushdown_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        sql,
        stats: json!({"rows": rows.len(), "limit": limit, "source_type": req.source_type}),
        rows,
    })
}
