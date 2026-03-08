use crate::*;

pub(crate) fn run_stream_state_save_v1(req: StreamStateSaveReq) -> Result<Value, String> {
    let mut store = load_kv_store(&stream_state_store_path());
    store.insert(
        req.stream_key.clone(),
        json!({
            "state": req.state,
            "offset": req.offset.unwrap_or(0),
            "updated_at": utc_now_iso()
        }),
    );
    save_kv_store(&stream_state_store_path(), &store)?;
    Ok(
        json!({"ok": true, "operator": "stream_state_v1_save", "status": "done", "run_id": req.run_id, "stream_key": req.stream_key}),
    )
}

pub(crate) fn run_stream_state_load_v1(req: StreamStateLoadReq) -> Result<Value, String> {
    let store = load_kv_store(&stream_state_store_path());
    let v = store.get(&req.stream_key).cloned().unwrap_or(Value::Null);
    Ok(
        json!({"ok": true, "operator": "stream_state_v1_load", "status": "done", "run_id": req.run_id, "stream_key": req.stream_key, "value": v}),
    )
}

pub(crate) fn run_query_lang_v1(req: QueryLangReq) -> Result<Value, String> {
    let q = req.query.trim();
    if q.is_empty() {
        return Err("query_lang_v1 query is empty".to_string());
    }
    let mut rows = req.rows;
    if let Some(rest) = q.strip_prefix("where ") {
        let cond = rest.trim();
        let parts = ["==", "!=", ">=", "<=", ">", "<"]
            .iter()
            .find_map(|op| cond.find(op).map(|p| (*op, p)));
        if let Some((op, pos)) = parts {
            let field = cond[..pos].trim();
            let rhs = cond[pos + op.len()..].trim().trim_matches('"');
            rows.retain(|r| {
                let o = r.as_object();
                let lv = o
                    .map(|m| value_to_string_or_null(m.get(field)))
                    .unwrap_or_default();
                let lnum = lv.parse::<f64>().ok();
                let rnum = rhs.parse::<f64>().ok();
                match op {
                    "==" => lv == rhs,
                    "!=" => lv != rhs,
                    ">" => lnum.zip(rnum).map(|(a, b)| a > b).unwrap_or(false),
                    "<" => lnum.zip(rnum).map(|(a, b)| a < b).unwrap_or(false),
                    ">=" => lnum.zip(rnum).map(|(a, b)| a >= b).unwrap_or(false),
                    "<=" => lnum.zip(rnum).map(|(a, b)| a <= b).unwrap_or(false),
                    _ => false,
                }
            });
        }
    } else if let Some(rest) = q.strip_prefix("select ") {
        let fields = rest
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        if !fields.is_empty() {
            rows = rows
                .into_iter()
                .filter_map(|r| {
                    let o = r.as_object()?;
                    let mut m = Map::new();
                    for f in &fields {
                        if let Some(v) = o.get(f) {
                            m.insert(f.clone(), v.clone());
                        }
                    }
                    Some(Value::Object(m))
                })
                .collect();
        }
    } else if let Some(rest) = q.strip_prefix("limit ") {
        let n = rest.trim().parse::<usize>().unwrap_or(100);
        rows.truncate(n);
    }
    Ok(
        json!({"ok": true, "operator": "query_lang_v1", "status": "done", "run_id": req.run_id, "rows": rows}),
    )
}

pub(crate) fn parse_filter_eq_map(v: &Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(obj) = v.as_object() {
        for (k, vv) in obj {
            out.insert(k.clone(), value_to_string(vv));
        }
    }
    out
}

pub(crate) fn run_columnar_eval_v1(req: ColumnarEvalV1Req) -> Result<Value, String> {
    maybe_inject_fault("columnar_eval_v1")?;
    let begin = Instant::now();
    let rows_in = req.rows.len();
    let select_fields = req.select_fields.unwrap_or_default();
    let filter_eq = req
        .filter_eq
        .as_ref()
        .map(parse_filter_eq_map)
        .unwrap_or_default();
    let limit = req.limit.unwrap_or(10000).max(1);

    let mut cols_set = HashSet::<String>::new();
    for r in &req.rows {
        if let Some(o) = r.as_object() {
            for k in o.keys() {
                cols_set.insert(k.clone());
            }
        }
    }
    let mut columns = cols_set.into_iter().collect::<Vec<_>>();
    columns.sort();
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(c, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for c in &columns {
        let vals = req
            .rows
            .iter()
            .map(|r| r.as_object().and_then(|o| o.get(c)).map(value_to_string))
            .collect::<Vec<_>>();
        arrays.push(Arc::new(StringArray::from(vals)) as ArrayRef);
    }
    let batch = RecordBatch::try_new(schema, arrays).map_err(|e| format!("columnar batch: {e}"))?;

    let mut picked_idx = Vec::<u32>::new();
    for i in 0..batch.num_rows() {
        let mut ok = true;
        for (f, exp) in &filter_eq {
            if let Some(ci) = columns.iter().position(|c| c == f)
                && let Some(a) = batch.column(ci).as_any().downcast_ref::<StringArray>()
            {
                let cur = if a.is_null(i) {
                    "".to_string()
                } else {
                    a.value(i).to_string()
                };
                if cur != *exp {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            picked_idx.push(i as u32);
        }
        if picked_idx.len() >= limit {
            break;
        }
    }
    let idx = UInt32Array::from(picked_idx);
    let mut out_rows = Vec::new();
    for row_pos in 0..idx.len() {
        let src = idx.value(row_pos) as usize;
        let mut obj = Map::new();
        for (ci, c) in columns.iter().enumerate() {
            if !select_fields.is_empty() && !select_fields.iter().any(|x| x == c) {
                continue;
            }
            if let Some(a) = batch.column(ci).as_any().downcast_ref::<StringArray>() {
                if a.is_null(src) {
                    obj.insert(c.clone(), Value::Null);
                } else {
                    obj.insert(c.clone(), json!(a.value(src)));
                }
            }
        }
        out_rows.push(Value::Object(obj));
    }
    let duration = begin.elapsed().as_millis();
    let _ = run_runtime_stats_v1(RuntimeStatsV1Req {
        run_id: req.run_id.clone(),
        op: "record".to_string(),
        operator: Some("columnar_eval_v1".to_string()),
        ok: Some(true),
        error_code: None,
        duration_ms: Some(duration),
        rows_in: Some(rows_in),
        rows_out: Some(out_rows.len()),
    });
    Ok(json!({
        "ok": true,
        "operator": "columnar_eval_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out_rows,
        "stats": {"rows_in": rows_in, "rows_out": out_rows.len(), "duration_ms": duration}
    }))
}
