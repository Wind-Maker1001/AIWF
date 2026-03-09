use super::*;

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
