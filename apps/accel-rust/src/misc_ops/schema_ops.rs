use super::*;

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
