use super::*;

pub(crate) fn parse_percentile_op(op: &str) -> Option<f64> {
    let t = op.trim().to_lowercase();
    if t == "percentile_p50" || t == "p50" || t == "median" {
        return Some(0.5);
    }
    if let Some(rest) = t.strip_prefix("percentile_")
        && let Ok(v) = rest.parse::<f64>()
    {
        return Some((v / 100.0).clamp(0.0, 1.0));
    }
    None
}

pub(crate) fn approx_percentile(mut vals: Vec<f64>, p: f64, sample_size: usize) -> Option<f64> {
    if vals.is_empty() {
        return None;
    }
    let s = sample_size.max(1);
    if vals.len() > s {
        let step = (vals.len() as f64 / s as f64).ceil() as usize;
        vals = vals
            .into_iter()
            .enumerate()
            .filter_map(|(i, v)| if i % step == 0 { Some(v) } else { None })
            .collect();
    }
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pos = ((vals.len() - 1) as f64 * p.clamp(0.0, 1.0)).round() as usize;
    vals.get(pos).copied()
}

fn parse_topk_n(op: &str) -> Option<usize> {
    let t = op.trim().to_lowercase();
    if t == "topk" {
        return Some(5);
    }
    t.strip_prefix("topk_")
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.clamp(1, 100))
}

fn parse_tdigest_percentile(op: &str) -> Option<f64> {
    let t = op.trim().to_lowercase();
    if t == "tdigest_p50" {
        return Some(0.5);
    }
    if t == "tdigest_p90" {
        return Some(0.9);
    }
    if t == "tdigest_p95" {
        return Some(0.95);
    }
    if let Some(rest) = t.strip_prefix("tdigest_p")
        && let Ok(v) = rest.parse::<f64>()
    {
        return Some((v / 100.0).clamp(0.0, 1.0));
    }
    None
}

pub(crate) fn aggregate_group_v3(
    group_vals: &Map<String, Value>,
    rows: &[Map<String, Value>],
    specs: &[Value],
    sample_size: usize,
) -> Value {
    let mut obj = group_vals.clone();
    for spec in specs {
        let Some(sp) = spec.as_object() else { continue };
        let op = sp
            .get("op")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_lowercase();
        let field = sp.get("field").and_then(|v| v.as_str()).unwrap_or("");
        let as_name = sp
            .get("as")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{field}_{op}"));
        if op == "count" {
            obj.insert(as_name, json!(rows.len()));
            continue;
        }
        if op == "approx_count_distinct" || op == "approx_count_distinct_hll" || op == "hll_count" {
            let mut set = HashSet::new();
            for r in rows {
                set.insert(value_to_string_or_null(r.get(field)));
            }
            obj.insert(as_name, json!(set.len()));
            continue;
        }
        if let Some(topk_n) = parse_topk_n(&op) {
            let mut freq = HashMap::<String, usize>::new();
            for r in rows {
                let k = value_to_string_or_null(r.get(field));
                *freq.entry(k).or_insert(0) += 1;
            }
            let mut items = freq
                .into_iter()
                .map(|(value, count)| json!({"value": value, "count": count}))
                .collect::<Vec<_>>();
            items.sort_by(|a, b| {
                let ac = a.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                let bc = b.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                bc.cmp(&ac)
            });
            items.truncate(topk_n);
            obj.insert(as_name, Value::Array(items));
            continue;
        }
        let vals = rows
            .iter()
            .filter_map(|r| r.get(field).and_then(value_to_f64))
            .collect::<Vec<_>>();
        match op.as_str() {
            "sum" => {
                obj.insert(as_name, json!(vals.iter().sum::<f64>()));
            }
            "avg" => {
                let v = if vals.is_empty() {
                    Value::Null
                } else {
                    json!(vals.iter().sum::<f64>() / vals.len() as f64)
                };
                obj.insert(as_name, v);
            }
            "min" => {
                obj.insert(
                    as_name,
                    vals.iter()
                        .cloned()
                        .reduce(f64::min)
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                );
            }
            "max" => {
                obj.insert(
                    as_name,
                    vals.iter()
                        .cloned()
                        .reduce(f64::max)
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                );
            }
            "stddev" => {
                if vals.len() < 2 {
                    obj.insert(as_name, Value::Null);
                } else {
                    let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                    let var =
                        vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64;
                    obj.insert(as_name, json!(var.sqrt()));
                }
            }
            "percentile_p50" | "median" | "approx_percentile_p50" => {
                obj.insert(
                    as_name,
                    approx_percentile(vals, 0.5, sample_size)
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                );
            }
            _ => {
                if let Some(p) = parse_percentile_op(&op) {
                    obj.insert(
                        as_name,
                        approx_percentile(vals, p, sample_size)
                            .map(Value::from)
                            .unwrap_or(Value::Null),
                    );
                } else if let Some(p) = parse_tdigest_percentile(&op) {
                    obj.insert(
                        as_name,
                        approx_percentile(vals, p, sample_size)
                            .map(Value::from)
                            .unwrap_or(Value::Null),
                    );
                }
            }
        }
    }
    Value::Object(obj)
}

pub(crate) fn compute_aggregate(
    rows: &[Map<String, Value>],
    aggregate_rule: Option<&Value>,
) -> Option<Value> {
    let rule = aggregate_rule.and_then(|v| v.as_object())?;
    let group_by = rule
        .get("group_by")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect::<Vec<String>>()
        })
        .unwrap_or_default();
    let metrics = rule
        .get("metrics")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_else(|| vec![json!({"field": "amount", "op": "sum", "as": "sum_amount"})]);
    if rows.is_empty() {
        return Some(json!({"rows": [], "group_by": group_by}));
    }

    let mut groups: HashMap<String, Vec<&Map<String, Value>>> = HashMap::new();
    for r in rows {
        let key = if group_by.is_empty() {
            "__all__".to_string()
        } else {
            group_by
                .iter()
                .map(|f| value_to_string_or_null(r.get(f)))
                .collect::<Vec<String>>()
                .join("|")
        };
        groups.entry(key).or_default().push(r);
    }

    let mut out: Vec<Value> = Vec::new();
    for (_k, rs) in groups {
        let mut row = Map::<String, Value>::new();
        if let Some(first) = rs.first() {
            for f in &group_by {
                row.insert(f.clone(), first.get(f).cloned().unwrap_or(Value::Null));
            }
        }
        for m in &metrics {
            let Some(obj) = m.as_object() else { continue };
            let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
            let op = obj.get("op").and_then(|v| v.as_str()).unwrap_or("count");
            let as_name = obj
                .get("as")
                .and_then(|v| v.as_str())
                .unwrap_or(op)
                .to_string();
            match op {
                "count" => {
                    row.insert(as_name, Value::Number((rs.len() as u64).into()));
                }
                "sum" | "avg" | "min" | "max" => {
                    let nums: Vec<f64> = rs
                        .iter()
                        .filter_map(|r| r.get(field).and_then(value_to_f64))
                        .collect();
                    if nums.is_empty() {
                        row.insert(as_name, Value::Null);
                    } else {
                        let val = match op {
                            "sum" => nums.iter().sum::<f64>(),
                            "avg" => nums.iter().sum::<f64>() / nums.len() as f64,
                            "min" => nums.iter().fold(f64::INFINITY, |a, b| a.min(*b)),
                            _ => nums.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b)),
                        };
                        row.insert(
                            as_name,
                            serde_json::Number::from_f64(val)
                                .map(Value::Number)
                                .unwrap_or(Value::Null),
                        );
                    }
                }
                _ => {}
            }
        }
        out.push(Value::Object(row));
    }
    Some(json!({"rows": out, "group_by": group_by, "metrics": metrics}))
}
