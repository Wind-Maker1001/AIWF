use super::*;

fn computed_field_ref_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\$([A-Za-z0-9_]+)").expect("valid computed field regex"))
}

pub(crate) fn run_lineage_v2(req: LineageV2Req) -> Result<Value, String> {
    let mut edges = Vec::<Value>::new();
    if let Some(rules) = req.rules.as_ref()
        && let Some(m) = rules.get("computed_fields").and_then(|v| v.as_object())
    {
        let re = computed_field_ref_regex();
        for (target, expr) in m {
            let mut deps = HashSet::new();
            let s = value_to_string(expr);
            for c in re.captures_iter(&s) {
                if let Some(f) = c.get(1) {
                    deps.insert(f.as_str().to_string());
                }
            }
            for d in deps {
                edges.push(json!({"from": d, "to": target, "kind":"computed_fields"}));
            }
        }
    }
    if let Some(specs) = req.computed_fields_v3.as_ref() {
        for sp in specs {
            let Some(o) = sp.as_object() else { continue };
            let target = o
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let expr = o.get("expr").cloned().unwrap_or(Value::Null);
            let mut deps = HashSet::new();
            collect_expr_lineage(&expr, &mut deps);
            for d in deps {
                edges.push(json!({"from": d, "to": target, "kind":"computed_fields_v3"}));
            }
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "lineage_v2",
        "status": "done",
        "run_id": req.run_id,
        "edges": edges
    }))
}

pub(crate) fn run_lineage_v3(req: LineageV3Req) -> Result<Value, String> {
    let mut out = run_lineage_v2(LineageV2Req {
        run_id: req.run_id.clone(),
        rules: req.rules,
        computed_fields_v3: req.computed_fields_v3,
    })?;
    if let Some(m) = out.as_object_mut() {
        if let Some(rows) = req.rows.as_ref()
            && let Some(obj) = rows.first().and_then(|v| v.as_object())
        {
            m.insert(
                "source_columns".to_string(),
                Value::Array(obj.keys().map(|k| Value::String(k.clone())).collect()),
            );
        }
        let edges = req
            .workflow_steps
            .unwrap_or_default()
            .into_iter()
            .filter_map(|s| s.as_object().cloned())
            .flat_map(|o| {
                let to = o
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let deps = o
                    .get("depends_on")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                deps.into_iter().filter_map(move |d| {
                    d.as_str()
                        .map(|from| json!({"from": from, "to": to, "type": "step_dep"}))
                })
            })
            .collect::<Vec<_>>();
        m.insert("step_lineage".to_string(), json!(edges));
        m.insert("operator".to_string(), json!("lineage_v3"));
    }
    Ok(out)
}

pub(super) fn summarize_value(v: &Value) -> Value {
    match v {
        Value::Array(a) => json!({"type":"array","len":a.len()}),
        Value::Object(m) => {
            let keys = m.keys().take(12).cloned().collect::<Vec<_>>();
            json!({"type":"object","keys":keys,"size":m.len()})
        }
        Value::String(s) => json!({"type":"string","len":s.chars().count()}),
        Value::Number(n) => json!({"type":"number","value":n}),
        Value::Bool(b) => json!({"type":"bool","value":b}),
        Value::Null => json!({"type":"null"}),
    }
}
