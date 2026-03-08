use crate::*;

pub(crate) fn parse_event_ts_ms(v: Option<&Value>) -> Option<i64> {
    let s = value_to_string_or_null(v);
    if s.is_empty() {
        return None;
    }
    if let Ok(n) = s.parse::<i64>() {
        return Some(n);
    }
    Some(parse_time_order_key(&s))
}

pub(crate) fn run_stream_window_v1(req: StreamWindowV1Req) -> Result<Value, String> {
    maybe_inject_fault("stream_window_v1")?;
    if req.window_ms == 0 {
        return Err("stream_window_v1 window_ms must be > 0".to_string());
    }
    let watermark_ms = req.watermark_ms.unwrap_or(req.window_ms);
    let group_by = req.group_by.unwrap_or_default();
    let trigger = req.trigger.unwrap_or_else(|| "on_watermark".to_string());
    let value_field = req.value_field.unwrap_or_else(|| "value".to_string());
    let now_ms = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()) as i64;
    let mut buckets = HashMap::<String, (u64, f64, u64, i64)>::new();
    let mut dropped_late = 0usize;
    for r in &req.rows {
        let Some(o) = r.as_object() else {
            continue;
        };
        let Some(ts) = parse_event_ts_ms(o.get(&req.event_time_field)) else {
            continue;
        };
        if now_ms.saturating_sub(ts) > watermark_ms as i64 {
            dropped_late += 1;
            continue;
        }
        let start = (ts / req.window_ms as i64) * req.window_ms as i64;
        let mut parts = vec![start.to_string()];
        for g in &group_by {
            parts.push(value_to_string_or_null(o.get(g)));
        }
        let key = parts.join("|");
        let e = buckets.entry(key).or_insert((0, 0.0, 0, start));
        e.0 += 1;
        if let Some(v) = o.get(&value_field).and_then(value_to_f64) {
            e.1 += v;
            e.2 += 1;
        }
    }
    let mut rows = Vec::new();
    for (k, (cnt, sum, sum_n, start)) in buckets {
        let mut obj = Map::new();
        obj.insert("window_start_ms".to_string(), json!(start));
        obj.insert(
            "window_end_ms".to_string(),
            json!(start + req.window_ms as i64),
        );
        obj.insert("count".to_string(), json!(cnt));
        obj.insert("sum".to_string(), json!(sum));
        obj.insert(
            "avg".to_string(),
            if sum_n == 0 {
                Value::Null
            } else {
                json!(sum / sum_n as f64)
            },
        );
        let parts = k.split('|').collect::<Vec<_>>();
        for (i, g) in group_by.iter().enumerate() {
            if let Some(v) = parts.get(i + 1) {
                obj.insert(g.clone(), json!(*v));
            }
        }
        rows.push(Value::Object(obj));
    }
    rows.sort_by(|a, b| {
        let av = a.get("window_start_ms").and_then(value_to_i64).unwrap_or(0);
        let bv = b.get("window_start_ms").and_then(value_to_i64).unwrap_or(0);
        av.cmp(&bv)
    });
    let _ = run_stream_state_v2(StreamStateV2Req {
        run_id: req.run_id.clone(),
        op: "checkpoint".to_string(),
        stream_key: format!("stream_window_v1:{}", req.stream_key),
        state: Some(json!({"rows_out": rows.len(), "trigger": trigger})),
        offset: Some(req.rows.len() as u64),
        checkpoint_version: None,
        expected_version: None,
        backend: None,
        db_path: None,
        event_ts_ms: None,
        max_late_ms: None,
    });
    Ok(json!({
        "ok": true,
        "operator": "stream_window_v1",
        "status": "done",
        "run_id": req.run_id,
        "trigger": trigger,
        "rows": rows,
        "stats": {"input_rows": req.rows.len(), "output_rows": rows.len(), "dropped_late": dropped_late, "window_ms": req.window_ms, "watermark_ms": watermark_ms}
    }))
}

pub(crate) fn run_stream_window_v2(req: StreamWindowV2Req) -> Result<Value, String> {
    maybe_inject_fault("stream_window_v2")?;
    if req.window_ms == 0 {
        return Err("stream_window_v2 window_ms must be > 0".to_string());
    }
    let window_type = req
        .window_type
        .unwrap_or_else(|| "tumbling".to_string())
        .to_lowercase();
    let slide_ms = req.slide_ms.unwrap_or(req.window_ms).max(1);
    let session_gap_ms = req.session_gap_ms.unwrap_or(req.window_ms).max(1);
    let allowed_lateness_ms = req.allowed_lateness_ms.unwrap_or(req.window_ms);
    let watermark_ms = req.watermark_ms.unwrap_or(allowed_lateness_ms);
    let group_by = req.group_by.unwrap_or_default();
    let value_field = req.value_field.unwrap_or_else(|| "value".to_string());
    let trigger = req.trigger.unwrap_or_else(|| "on_watermark".to_string());
    let now_ms = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()) as i64;

    let mut late_rows = Vec::<Value>::new();
    let mut ontime = Vec::<(i64, Map<String, Value>)>::new();
    for r in &req.rows {
        let Some(o) = r.as_object().cloned() else {
            continue;
        };
        let Some(ts) = parse_event_ts_ms(o.get(&req.event_time_field)) else {
            continue;
        };
        if now_ms.saturating_sub(ts) > watermark_ms as i64 {
            late_rows.push(Value::Object(o));
        } else {
            ontime.push((ts, o));
        }
    }

    let mut buckets = HashMap::<String, (u64, f64, u64, i64, i64)>::new();
    let key_of = |o: &Map<String, Value>| -> String {
        group_by
            .iter()
            .map(|g| value_to_string_or_null(o.get(g)))
            .collect::<Vec<_>>()
            .join("|")
    };

    if window_type == "session" {
        let mut grouped = HashMap::<String, Vec<(i64, Map<String, Value>)>>::new();
        for (ts, o) in ontime {
            grouped.entry(key_of(&o)).or_default().push((ts, o));
        }
        for (gk, mut items) in grouped {
            items.sort_by_key(|x| x.0);
            let mut start = 0i64;
            let mut end = 0i64;
            let mut cnt = 0u64;
            let mut sum = 0.0f64;
            let mut sum_n = 0u64;
            for (i, (ts, o)) in items.iter().enumerate() {
                if i == 0 {
                    start = *ts;
                    end = *ts;
                }
                if *ts - end > session_gap_ms as i64 {
                    let k = format!("{start}|{gk}");
                    buckets.insert(k, (cnt, sum, sum_n, start, end + 1));
                    start = *ts;
                    cnt = 0;
                    sum = 0.0;
                    sum_n = 0;
                }
                end = *ts;
                cnt += 1;
                if let Some(v) = o.get(&value_field).and_then(value_to_f64) {
                    sum += v;
                    sum_n += 1;
                }
            }
            if cnt > 0 {
                let k = format!("{start}|{gk}");
                buckets.insert(k, (cnt, sum, sum_n, start, end + 1));
            }
        }
    } else if window_type == "sliding" {
        let overlap = (req.window_ms / slide_ms).max(1);
        for (ts, o) in ontime {
            let gk = key_of(&o);
            let base = (ts / slide_ms as i64) * slide_ms as i64;
            for j in 0..=overlap {
                let start = base - (j as i64 * slide_ms as i64);
                if ts < start || ts >= start + req.window_ms as i64 {
                    continue;
                }
                let end = start + req.window_ms as i64;
                let k = format!("{start}|{gk}");
                let e = buckets.entry(k).or_insert((0, 0.0, 0, start, end));
                e.0 += 1;
                if let Some(v) = o.get(&value_field).and_then(value_to_f64) {
                    e.1 += v;
                    e.2 += 1;
                }
            }
        }
    } else {
        for (ts, o) in ontime {
            let gk = key_of(&o);
            let start = (ts / req.window_ms as i64) * req.window_ms as i64;
            let end = start + req.window_ms as i64;
            let k = format!("{start}|{gk}");
            let e = buckets.entry(k).or_insert((0, 0.0, 0, start, end));
            e.0 += 1;
            if let Some(v) = o.get(&value_field).and_then(value_to_f64) {
                e.1 += v;
                e.2 += 1;
            }
        }
    }

    let mut rows = Vec::new();
    for (k, (cnt, sum, sum_n, start, end)) in buckets {
        let mut obj = Map::new();
        obj.insert("window_start_ms".to_string(), json!(start));
        obj.insert("window_end_ms".to_string(), json!(end));
        obj.insert("count".to_string(), json!(cnt));
        obj.insert("sum".to_string(), json!(sum));
        obj.insert(
            "avg".to_string(),
            if sum_n == 0 {
                Value::Null
            } else {
                json!(sum / sum_n as f64)
            },
        );
        let parts = k.split('|').collect::<Vec<_>>();
        for (i, g) in group_by.iter().enumerate() {
            if let Some(v) = parts.get(i + 1) {
                obj.insert(g.clone(), json!(*v));
            }
        }
        rows.push(Value::Object(obj));
    }
    rows.sort_by(|a, b| {
        let av = a.get("window_start_ms").and_then(value_to_i64).unwrap_or(0);
        let bv = b.get("window_start_ms").and_then(value_to_i64).unwrap_or(0);
        av.cmp(&bv)
    });

    let _ = run_stream_state_v2(StreamStateV2Req {
        run_id: req.run_id.clone(),
        op: "checkpoint".to_string(),
        stream_key: format!("stream_window_v2:{}", req.stream_key),
        state: Some(json!({"window_type": window_type, "rows_out": rows.len()})),
        offset: Some(req.rows.len() as u64),
        checkpoint_version: None,
        expected_version: None,
        backend: None,
        db_path: None,
        event_ts_ms: None,
        max_late_ms: None,
    });

    Ok(json!({
        "ok": true,
        "operator": "stream_window_v2",
        "status": "done",
        "run_id": req.run_id,
        "window_type": window_type,
        "trigger": trigger,
        "rows": rows,
        "late_rows": if req.emit_late_side.unwrap_or(false) { Value::Array(late_rows.clone()) } else { Value::Array(vec![]) },
        "stats": {
            "input_rows": req.rows.len(),
            "output_rows": rows.len(),
            "late_rows": late_rows.len(),
            "window_ms": req.window_ms,
            "slide_ms": slide_ms,
            "session_gap_ms": session_gap_ms,
            "allowed_lateness_ms": allowed_lateness_ms
        }
    }))
}

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
