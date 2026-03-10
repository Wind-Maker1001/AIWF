use super::helpers::parse_percentile_op;
use super::*;

pub(crate) fn run_aggregate_rows_v1(req: AggregateRowsReq) -> Result<AggregateRowsResp, String> {
    let specs = parse_agg_specs(&req.aggregates)?;
    let mut buckets: HashMap<String, AggBucket> = HashMap::new();
    for row in &req.rows {
        let Some(obj) = row.as_object() else {
            continue;
        };
        let mut group_vals = Map::new();
        let mut key_parts = Vec::new();
        for g in &req.group_by {
            let v = obj.get(g).cloned().unwrap_or(Value::Null);
            key_parts.push(value_to_string_or_null(Some(&v)));
            group_vals.insert(g.clone(), v);
        }
        let key = key_parts.join("\u{1f}");
        let b = buckets.entry(key).or_insert_with(|| AggBucket {
            group_vals: group_vals.clone(),
            ..Default::default()
        });
        b.count += 1;
        for sp in &specs {
            let Some(f) = sp.field.as_ref() else {
                continue;
            };
            let n = obj.get(f).and_then(value_to_f64);
            if let Some(v) = n {
                *b.sums.entry(sp.as_name.clone()).or_insert(0.0) += v;
                b.min
                    .entry(sp.as_name.clone())
                    .and_modify(|cur| {
                        if v < *cur {
                            *cur = v;
                        }
                    })
                    .or_insert(v);
                b.max
                    .entry(sp.as_name.clone())
                    .and_modify(|cur| {
                        if v > *cur {
                            *cur = v;
                        }
                    })
                    .or_insert(v);
            }
        }
    }
    let mut out = Vec::new();
    for (_, b) in buckets {
        let mut obj = b.group_vals;
        for sp in &specs {
            match sp.op.as_str() {
                "count" => {
                    obj.insert(sp.as_name.clone(), json!(b.count));
                }
                "sum" => {
                    obj.insert(
                        sp.as_name.clone(),
                        json!(b.sums.get(&sp.as_name).copied().unwrap_or(0.0)),
                    );
                }
                "avg" => {
                    let sum = b.sums.get(&sp.as_name).copied().unwrap_or(0.0);
                    let denom = b.count.max(1) as f64;
                    obj.insert(sp.as_name.clone(), json!(sum / denom));
                }
                "min" => {
                    obj.insert(
                        sp.as_name.clone(),
                        b.min
                            .get(&sp.as_name)
                            .copied()
                            .map(Value::from)
                            .unwrap_or(Value::Null),
                    );
                }
                "max" => {
                    obj.insert(
                        sp.as_name.clone(),
                        b.max
                            .get(&sp.as_name)
                            .copied()
                            .map(Value::from)
                            .unwrap_or(Value::Null),
                    );
                }
                _ => {}
            }
        }
        out.push(Value::Object(obj));
    }
    let first_group = req.group_by.first().cloned().unwrap_or_default();
    out.sort_by(|a, b| {
        let av = a
            .as_object()
            .and_then(|m| m.get(&first_group))
            .map(|v| value_to_string_or_null(Some(v)))
            .unwrap_or_default();
        let bv = b
            .as_object()
            .and_then(|m| m.get(&first_group))
            .map(|v| value_to_string_or_null(Some(v)))
            .unwrap_or_default();
        av.cmp(&bv)
    });
    Ok(AggregateRowsResp {
        ok: true,
        operator: "aggregate_rows_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        stats: json!({"input_rows": req.rows.len(), "output_rows": out.len(), "groups": out.len()}),
        rows: out,
    })
}

pub(crate) fn run_aggregate_rows_v2(req: AggregateRowsV2Req) -> Result<AggregateRowsResp, String> {
    let specs = parse_agg_specs(&req.aggregates)?;
    let mut buckets: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    let mut group_cache: HashMap<String, Map<String, Value>> = HashMap::new();
    for row in req.rows {
        let Some(obj) = row.as_object() else {
            continue;
        };
        let mut group_vals = Map::new();
        let mut key_parts = Vec::new();
        for g in &req.group_by {
            let v = obj.get(g).cloned().unwrap_or(Value::Null);
            key_parts.push(value_to_string_or_null(Some(&v)));
            group_vals.insert(g.clone(), v);
        }
        let key = key_parts.join("\u{1f}");
        group_cache.entry(key.clone()).or_insert(group_vals);
        buckets.entry(key).or_default().push(obj.clone());
    }
    let mut out: Vec<Value> = Vec::new();
    for (k, rows) in buckets {
        let mut obj = group_cache.get(&k).cloned().unwrap_or_default();
        for sp in &specs {
            let op = sp.op.to_lowercase();
            if op == "count" {
                obj.insert(sp.as_name.clone(), json!(rows.len()));
                continue;
            }
            if op == "count_distinct" {
                let mut set = HashSet::new();
                if let Some(f) = sp.field.as_ref() {
                    for r in &rows {
                        set.insert(value_to_string_or_null(r.get(f)));
                    }
                }
                obj.insert(sp.as_name.clone(), json!(set.len()));
                continue;
            }
            let Some(field) = sp.field.as_ref() else {
                continue;
            };
            let vals = rows
                .iter()
                .filter_map(|r| r.get(field).and_then(value_to_f64))
                .collect::<Vec<_>>();
            match op.as_str() {
                "sum" => {
                    obj.insert(sp.as_name.clone(), json!(vals.iter().sum::<f64>()));
                }
                "avg" => {
                    let v = if vals.is_empty() {
                        Value::Null
                    } else {
                        json!(vals.iter().sum::<f64>() / vals.len() as f64)
                    };
                    obj.insert(sp.as_name.clone(), v);
                }
                "min" => {
                    let v = vals
                        .iter()
                        .cloned()
                        .reduce(f64::min)
                        .map(Value::from)
                        .unwrap_or(Value::Null);
                    obj.insert(sp.as_name.clone(), v);
                }
                "max" => {
                    let v = vals
                        .iter()
                        .cloned()
                        .reduce(f64::max)
                        .map(Value::from)
                        .unwrap_or(Value::Null);
                    obj.insert(sp.as_name.clone(), v);
                }
                "stddev" => {
                    if vals.len() < 2 {
                        obj.insert(sp.as_name.clone(), Value::Null);
                    } else {
                        let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                        let var = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                            / vals.len() as f64;
                        obj.insert(sp.as_name.clone(), json!(var.sqrt()));
                    }
                }
                _ => {
                    if let Some(p) = parse_percentile_op(&op) {
                        if vals.is_empty() {
                            obj.insert(sp.as_name.clone(), Value::Null);
                        } else {
                            let mut sorted = vals.clone();
                            sorted.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let pos = ((sorted.len() - 1) as f64 * p).round() as usize;
                            obj.insert(sp.as_name.clone(), json!(sorted[pos]));
                        }
                    }
                }
            }
        }
        out.push(Value::Object(obj));
    }
    Ok(AggregateRowsResp {
        ok: true,
        operator: "aggregate_rows_v2".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        stats: json!({"groups": out.len(), "output_rows": out.len()}),
        rows: out,
    })
}
