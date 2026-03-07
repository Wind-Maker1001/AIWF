use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};

#[derive(Deserialize)]
pub(crate) struct AggregateRowsReq {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct AggregateRowsV2Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct AggregateRowsV3Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<Value>,
    pub approx_sample_size: Option<usize>,
}

#[derive(Serialize)]
pub(crate) struct AggregateRowsResp {
    pub ok: bool,
    pub operator: String,
    pub status: String,
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub stats: Value,
}

#[derive(Deserialize)]
pub(crate) struct AggregateRowsV4Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<Value>,
    pub approx_sample_size: Option<usize>,
    pub verify_exact: Option<bool>,
    pub parallel_workers: Option<usize>,
}

#[derive(Clone)]
pub(crate) struct AggSpec {
    pub op: String,
    pub field: Option<String>,
    pub as_name: String,
}

#[derive(Default, Clone)]
pub(crate) struct AggBucket {
    pub group_vals: Map<String, Value>,
    pub count: u64,
    pub sums: HashMap<String, f64>,
    pub min: HashMap<String, f64>,
    pub max: HashMap<String, f64>,
}

pub(crate) fn parse_agg_specs(specs: &[Value]) -> Result<Vec<AggSpec>, String> {
    let mut out = Vec::new();
    for s in specs {
        let Some(obj) = s.as_object() else {
            return Err("aggregate spec must be object".to_string());
        };
        let op = obj
            .get("op")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_lowercase();
        if op.is_empty() {
            return Err("aggregate spec missing op".to_string());
        }
        let field = obj
            .get("field")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let as_name = obj
            .get("as")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| match &field {
                Some(f) => format!("{f}_{op}"),
                None => format!("_{op}"),
            });
        match op.as_str() {
            "count" => {}
            "sum" | "avg" | "min" | "max" | "count_distinct" | "stddev" => {
                if field.is_none() {
                    return Err(format!("aggregate op {op} requires field"));
                }
            }
            _ => {
                if parse_percentile_op(&op).is_none() {
                    return Err(format!("unsupported aggregate op: {op}"));
                }
                if field.is_none() {
                    return Err(format!("aggregate op {op} requires field"));
                }
            }
        }
        out.push(AggSpec { op, field, as_name });
    }
    if out.is_empty() {
        return Err("aggregates is empty".to_string());
    }
    Ok(out)
}

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

pub(crate) fn parse_percentile_op(op: &str) -> Option<f64> {
    let t = op.trim().to_lowercase();
    if t == "percentile_p50" || t == "p50" || t == "median" {
        return Some(0.5);
    }
    if let Some(rest) = t.strip_prefix("percentile_") {
        if let Ok(v) = rest.parse::<f64>() {
            return Some((v / 100.0).clamp(0.0, 1.0));
        }
    }
    None
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

pub(crate) fn approx_percentile(
    mut vals: Vec<f64>,
    p: f64,
    sample_size: usize,
) -> Option<f64> {
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

pub(crate) fn run_aggregate_rows_v3(req: AggregateRowsV3Req) -> Result<AggregateRowsResp, String> {
    let sample_size = req.approx_sample_size.unwrap_or(1024).clamp(64, 1_000_000);
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
    let mut out = Vec::new();
    for (k, rows) in buckets {
        let group_vals = group_cache.get(&k).cloned().unwrap_or_default();
        out.push(aggregate_group_v3(
            &group_vals,
            &rows,
            &req.aggregates,
            sample_size,
        ));
    }
    Ok(AggregateRowsResp {
        ok: true,
        operator: "aggregate_rows_v3".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        stats: json!({"groups": out.len(), "output_rows": out.len(), "approx_sample_size": sample_size}),
        rows: out,
    })
}

pub(crate) fn run_aggregate_rows_v4(req: AggregateRowsV4Req) -> Result<AggregateRowsResp, String> {
    let workers = req.parallel_workers.unwrap_or(1).clamp(1, 16);
    let mut out = if workers == 1 {
        run_aggregate_rows_v3(AggregateRowsV3Req {
            run_id: req.run_id.clone(),
            rows: req.rows.clone(),
            group_by: req.group_by.clone(),
            aggregates: req.aggregates.clone(),
            approx_sample_size: req.approx_sample_size,
        })?
    } else {
        let sample_size = req.approx_sample_size.unwrap_or(1024).clamp(64, 1_000_000);
        let mut buckets: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
        let mut group_cache: HashMap<String, Map<String, Value>> = HashMap::new();
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
            group_cache.entry(key.clone()).or_insert(group_vals);
            buckets.entry(key).or_default().push(obj.clone());
        }
        let mut groups = buckets
            .into_iter()
            .map(|(k, rows)| (group_cache.get(&k).cloned().unwrap_or_default(), rows))
            .collect::<Vec<_>>();
        let chunk = (groups.len() / workers).max(1);
        let mut joins = Vec::new();
        while !groups.is_empty() {
            let take = groups.drain(0..groups.len().min(chunk)).collect::<Vec<_>>();
            let specs = req.aggregates.clone();
            joins.push(std::thread::spawn(move || {
                take.into_iter()
                    .map(|(group_vals, rows)| {
                        aggregate_group_v3(&group_vals, &rows, &specs, sample_size)
                    })
                    .collect::<Vec<_>>()
            }));
        }
        let mut rows = Vec::new();
        for h in joins {
            let part = h
                .join()
                .map_err(|_| "aggregate_rows_v4 parallel worker panicked".to_string())?;
            rows.extend(part);
        }
        AggregateRowsResp {
            ok: true,
            operator: "aggregate_rows_v3".to_string(),
            status: "done".to_string(),
            run_id: req.run_id.clone(),
            stats: json!({"groups": rows.len(), "output_rows": rows.len(), "approx_sample_size": sample_size, "parallel_workers": workers}),
            rows,
        }
    };
    out.operator = "aggregate_rows_v4".to_string();
    if let Some(stats) = out.stats.as_object_mut() {
        stats.insert("parallel_workers".to_string(), json!(workers));
    }
    if req.verify_exact.unwrap_or(false) {
        let exact = run_aggregate_rows_v2(AggregateRowsV2Req {
            run_id: req.run_id.clone(),
            rows: req.rows,
            group_by: req.group_by,
            aggregates: req.aggregates,
        })?;
        if let Some(stats) = out.stats.as_object_mut() {
            stats.insert("exact_verify".to_string(), json!(true));
            stats.insert("exact_rows".to_string(), json!(exact.rows.len()));
            stats.insert("approx_rows".to_string(), json!(out.rows.len()));
            stats.insert(
                "row_count_delta".to_string(),
                json!((out.rows.len() as i64 - exact.rows.len() as i64).abs()),
            );
        }
    }
    Ok(out)
}

pub(crate) fn compute_aggregate(
    rows: &[Map<String, Value>],
    aggregate_rule: Option<&Value>,
) -> Option<Value> {
    let Some(rule) = aggregate_rule.and_then(|v| v.as_object()) else {
        return None;
    };
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
