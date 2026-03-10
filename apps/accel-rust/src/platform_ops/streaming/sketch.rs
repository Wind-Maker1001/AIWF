use super::*;

pub(crate) fn run_sketch_v1(req: SketchV1Req) -> Result<Value, String> {
    maybe_inject_fault("sketch_v1")?;
    let op = req.op.trim().to_lowercase();
    let kind = req
        .kind
        .clone()
        .unwrap_or_else(|| "hll".to_string())
        .to_lowercase();
    let field = req.field.clone().unwrap_or_else(|| "value".to_string());
    let mut state = req.state.unwrap_or_else(|| json!({}));
    let state_obj = state
        .as_object_mut()
        .ok_or_else(|| "sketch_v1 state must be object".to_string())?;
    if op == "update" || op == "create" {
        let rows = req.rows.unwrap_or_default();
        if kind == "hll" {
            let mut set = state_obj
                .get("set")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<HashSet<_>>();
            for r in &rows {
                if let Some(o) = r.as_object() {
                    set.insert(value_to_string_or_null(o.get(&field)));
                }
            }
            state_obj.insert("kind".to_string(), json!("hll"));
            state_obj.insert(
                "set".to_string(),
                Value::Array(set.into_iter().map(Value::String).collect()),
            );
        } else if kind == "tdigest" {
            let mut vals = state_obj
                .get("values")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| value_to_f64(&v))
                .collect::<Vec<_>>();
            for r in &rows {
                if let Some(o) = r.as_object()
                    && let Some(v) = o.get(&field).and_then(value_to_f64)
                {
                    vals.push(v);
                }
            }
            state_obj.insert("kind".to_string(), json!("tdigest"));
            state_obj.insert(
                "values".to_string(),
                Value::Array(vals.into_iter().map(Value::from).collect()),
            );
        } else if kind == "topk" {
            let mut freq = state_obj
                .get("freq")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            for r in &rows {
                if let Some(o) = r.as_object() {
                    let k = value_to_string_or_null(o.get(&field));
                    let n = freq.get(&k).and_then(|v| v.as_u64()).unwrap_or(0) + 1;
                    freq.insert(k, json!(n));
                }
            }
            state_obj.insert("kind".to_string(), json!("topk"));
            state_obj.insert("freq".to_string(), Value::Object(freq));
            state_obj.insert(
                "topk_n".to_string(),
                json!(req.topk_n.unwrap_or(5).clamp(1, 100)),
            );
        } else {
            return Err(format!("sketch_v1 unsupported kind: {kind}"));
        }
    } else if op == "merge" {
        let rhs = req.merge_state.unwrap_or_else(|| json!({}));
        if kind == "hll" {
            let mut set = state_obj
                .get("set")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<HashSet<_>>();
            for x in rhs
                .get("set")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
            {
                if let Some(s) = x.as_str() {
                    set.insert(s.to_string());
                }
            }
            state_obj.insert(
                "set".to_string(),
                Value::Array(set.into_iter().map(Value::String).collect()),
            );
            state_obj.insert("kind".to_string(), json!("hll"));
        } else if kind == "tdigest" {
            let mut vals = state_obj
                .get("values")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            vals.extend(
                rhs.get("values")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default(),
            );
            state_obj.insert("values".to_string(), Value::Array(vals));
            state_obj.insert("kind".to_string(), json!("tdigest"));
        } else if kind == "topk" {
            let mut freq = state_obj
                .get("freq")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            for (k, v) in rhs
                .get("freq")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default()
            {
                let n =
                    freq.get(&k).and_then(|x| x.as_u64()).unwrap_or(0) + v.as_u64().unwrap_or(0);
                freq.insert(k, json!(n));
            }
            state_obj.insert("freq".to_string(), Value::Object(freq));
            state_obj.insert("kind".to_string(), json!("topk"));
        }
    }
    let estimate = if kind == "hll" {
        state_obj
            .get("set")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0) as f64
    } else if kind == "tdigest" {
        let vals = state_obj
            .get("values")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| value_to_f64(&v))
            .collect::<Vec<_>>();
        approx_percentile(vals, 0.5, 2000).unwrap_or(0.0)
    } else {
        let mut items = state_obj
            .get("freq")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.as_u64().unwrap_or(0)))
            .collect::<Vec<_>>();
        items.sort_by(|a, b| b.1.cmp(&a.1));
        let n = state_obj
            .get("topk_n")
            .and_then(|v| v.as_u64())
            .unwrap_or(5) as usize;
        state_obj.insert(
            "topk".to_string(),
            Value::Array(
                items
                    .into_iter()
                    .take(n)
                    .map(|(k, c)| json!({"value": k, "count": c}))
                    .collect(),
            ),
        );
        n as f64
    };
    Ok(
        json!({"ok": true, "operator":"sketch_v1", "status":"done", "run_id": req.run_id, "kind": kind, "state": state, "estimate": estimate}),
    )
}
