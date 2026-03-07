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

#[derive(Deserialize)]
pub(crate) struct QualityCheckReq {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub rules: Value,
}

#[derive(Deserialize)]
pub(crate) struct QualityCheckV2Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub rules: Value,
}

#[derive(Deserialize)]
pub(crate) struct QualityCheckV3Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub rules: Value,
}

#[derive(Deserialize)]
pub(crate) struct QualityCheckV4Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub rules: Value,
    pub rules_dsl: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct QualityCheckResp {
    pub ok: bool,
    pub operator: String,
    pub status: String,
    pub run_id: Option<String>,
    pub passed: bool,
    pub report: Value,
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

pub(crate) fn run_quality_check_v1(req: QualityCheckReq) -> Result<QualityCheckResp, String> {
    let rules = req.rules.as_object().cloned().unwrap_or_default();
    let unique_fields = rules
        .get("unique_fields")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    let required_fields = rules
        .get("required_fields")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    let max_null_ratio = rules
        .get("max_null_ratio")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0)
        .clamp(0.0, 1.0);
    let mut passed = true;
    let mut violations = Vec::new();
    let mut duplicate_count = 0usize;
    if !unique_fields.is_empty() {
        let mut seen = std::collections::HashSet::new();
        for r in &req.rows {
            let Some(obj) = r.as_object() else {
                continue;
            };
            let key = unique_fields
                .iter()
                .map(|f| value_to_string_or_null(obj.get(f)))
                .collect::<Vec<_>>()
                .join("|");
            if !seen.insert(key) {
                duplicate_count += 1;
            }
        }
        if duplicate_count > 0 {
            passed = false;
            violations.push(json!({"rule":"unique_fields","duplicates": duplicate_count}));
        }
    }
    let mut null_violations = Vec::new();
    if !required_fields.is_empty() {
        for f in &required_fields {
            let mut nulls = 0usize;
            for r in &req.rows {
                let miss = r
                    .as_object()
                    .and_then(|o| o.get(f))
                    .map(|v| v.is_null() || value_to_string_or_null(Some(v)).trim().is_empty())
                    .unwrap_or(true);
                if miss {
                    nulls += 1;
                }
            }
            let ratio = if req.rows.is_empty() {
                0.0
            } else {
                nulls as f64 / req.rows.len() as f64
            };
            if ratio > max_null_ratio {
                passed = false;
                null_violations
                    .push(json!({"field":f, "null_ratio":ratio, "max_null_ratio":max_null_ratio}));
            }
        }
    }
    if !null_violations.is_empty() {
        violations.push(json!({"rule":"required_fields", "details": null_violations}));
    }
    let mut outlier_report = Vec::new();
    if let Some(oz) = rules.get("outlier_zscore").and_then(|v| v.as_object()) {
        let field = oz.get("field").and_then(|v| v.as_str()).unwrap_or("");
        let max_z = oz
            .get("max_z")
            .and_then(|v| v.as_f64())
            .unwrap_or(4.0)
            .abs();
        if !field.is_empty() {
            let vals = req
                .rows
                .iter()
                .filter_map(|r| {
                    r.as_object()
                        .and_then(|o| o.get(field))
                        .and_then(value_to_f64)
                })
                .collect::<Vec<_>>();
            if vals.len() >= 3 {
                let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                let var = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64;
                let std = var.sqrt();
                if std > 0.0 {
                    let outliers = vals
                        .iter()
                        .filter(|v| ((*v - mean).abs() / std) > max_z)
                        .count();
                    if outliers > 0 {
                        passed = false;
                        outlier_report
                            .push(json!({"field":field,"outliers":outliers,"max_z":max_z}));
                    }
                }
            }
        }
    }
    if !outlier_report.is_empty() {
        violations.push(json!({"rule":"outlier_zscore", "details": outlier_report}));
    }
    Ok(QualityCheckResp {
        ok: true,
        operator: "quality_check_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        passed,
        report: json!({
            "rows": req.rows.len(),
            "violations": violations,
            "rule_count": rules.len()
        }),
    })
}

pub(crate) fn run_quality_check_v2(req: QualityCheckV2Req) -> Result<QualityCheckResp, String> {
    let base = run_quality_check_v1(QualityCheckReq {
        run_id: req.run_id.clone(),
        rows: req.rows.clone(),
        rules: req.rules.clone(),
    })?;
    let rules = req.rules.as_object().cloned().unwrap_or_default();
    let mut violations = base
        .report
        .get("violations")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut passed = base.passed;

    if let Some(ranges) = rules.get("range_checks").and_then(|v| v.as_array()) {
        let mut details = Vec::new();
        for rc in ranges {
            let Some(o) = rc.as_object() else { continue };
            let field = o.get("field").and_then(|v| v.as_str()).unwrap_or("");
            if field.is_empty() {
                continue;
            }
            let min_v = o.get("min").and_then(value_to_f64);
            let max_v = o.get("max").and_then(value_to_f64);
            let mut bad = 0usize;
            for r in &req.rows {
                let Some(obj) = r.as_object() else { continue };
                let Some(v) = obj.get(field).and_then(value_to_f64) else {
                    continue;
                };
                if let Some(mn) = min_v
                    && v < mn
                {
                    bad += 1;
                    continue;
                }
                if let Some(mx) = max_v
                    && v > mx
                {
                    bad += 1;
                    continue;
                }
            }
            if bad > 0 {
                passed = false;
                details.push(json!({"field":field,"violations":bad,"min":min_v,"max":max_v}));
            }
        }
        if !details.is_empty() {
            violations.push(json!({"rule":"range_checks","details":details}));
        }
    }

    if let Some(deps) = rules.get("dependency_checks").and_then(|v| v.as_array()) {
        let mut details = Vec::new();
        for d in deps {
            let Some(o) = d.as_object() else { continue };
            let if_field = o.get("if_field").and_then(|v| v.as_str()).unwrap_or("");
            let if_equals = o.get("if_equals").map(value_to_string);
            let then_required = o
                .get("then_required")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if if_field.is_empty() || then_required.is_empty() {
                continue;
            }
            let mut bad = 0usize;
            for r in &req.rows {
                let Some(obj) = r.as_object() else { continue };
                let cond = match if_equals.as_ref() {
                    Some(exp) => value_to_string_or_null(obj.get(if_field)) == *exp,
                    None => !is_missing(obj.get(if_field)),
                };
                if cond && is_missing(obj.get(then_required)) {
                    bad += 1;
                }
            }
            if bad > 0 {
                passed = false;
                details.push(
                    json!({"if_field":if_field,"then_required":then_required,"violations":bad}),
                );
            }
        }
        if !details.is_empty() {
            violations.push(json!({"rule":"dependency_checks","details":details}));
        }
    }

    if let Some(drift) = rules.get("drift_check").and_then(|v| v.as_object()) {
        let field = drift.get("field").and_then(|v| v.as_str()).unwrap_or("");
        let baseline_mean = drift
            .get("baseline_mean")
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        let max_mean_delta = drift
            .get("max_mean_delta")
            .and_then(value_to_f64)
            .unwrap_or(f64::INFINITY);
        if !field.is_empty() && max_mean_delta.is_finite() {
            let vals = req
                .rows
                .iter()
                .filter_map(|r| {
                    r.as_object()
                        .and_then(|o| o.get(field))
                        .and_then(value_to_f64)
                })
                .collect::<Vec<_>>();
            if !vals.is_empty() {
                let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                let delta = (mean - baseline_mean).abs();
                if delta > max_mean_delta {
                    passed = false;
                    violations.push(json!({"rule":"drift_check","field":field,"mean":mean,"baseline_mean":baseline_mean,"delta":delta,"max_mean_delta":max_mean_delta}));
                }
            }
        }
    }

    let score = if violations.is_empty() {
        100.0
    } else {
        (100.0 - (violations.len() as f64 * 10.0)).max(0.0)
    };
    Ok(QualityCheckResp {
        ok: true,
        operator: "quality_check_v2".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        passed,
        report: json!({
            "rows": req.rows.len(),
            "violations": violations,
            "rule_count": rules.len(),
            "quality_score": score
        }),
    })
}

fn iqr_bounds(vals: &[f64]) -> Option<(f64, f64)> {
    if vals.len() < 4 {
        return None;
    }
    let mut v = vals.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let q = |p: f64| -> f64 {
        let idx = ((v.len() - 1) as f64 * p).round() as usize;
        v[idx.min(v.len() - 1)]
    };
    let q1 = q(0.25);
    let q3 = q(0.75);
    let iqr = q3 - q1;
    Some((q1 - 1.5 * iqr, q3 + 1.5 * iqr))
}

fn calc_psi(expected: &[f64], actual: &[f64], bins: usize) -> Option<f64> {
    if expected.is_empty() || actual.is_empty() || bins < 2 {
        return None;
    }
    let min_v = expected
        .iter()
        .chain(actual.iter())
        .fold(f64::INFINITY, |a, b| a.min(*b));
    let max_v = expected
        .iter()
        .chain(actual.iter())
        .fold(f64::NEG_INFINITY, |a, b| a.max(*b));
    if !min_v.is_finite() || !max_v.is_finite() || (max_v - min_v).abs() < f64::EPSILON {
        return Some(0.0);
    }
    let width = (max_v - min_v) / bins as f64;
    let mut e_cnt = vec![0f64; bins];
    let mut a_cnt = vec![0f64; bins];
    for v in expected {
        let idx = (((*v - min_v) / width).floor() as isize).clamp(0, bins as isize - 1) as usize;
        e_cnt[idx] += 1.0;
    }
    for v in actual {
        let idx = (((*v - min_v) / width).floor() as isize).clamp(0, bins as isize - 1) as usize;
        a_cnt[idx] += 1.0;
    }
    let e_total = expected.len() as f64;
    let a_total = actual.len() as f64;
    let eps = 1e-6;
    let psi = (0..bins)
        .map(|i| {
            let e = (e_cnt[i] / e_total).max(eps);
            let a = (a_cnt[i] / a_total).max(eps);
            (a - e) * (a / e).ln()
        })
        .sum::<f64>();
    Some(psi)
}

fn parse_quality_rules_dsl(dsl: &str) -> Value {
    let mut required_fields: Vec<Value> = Vec::new();
    let mut unique_fields: Vec<Value> = Vec::new();
    let mut range_checks: Vec<Value> = Vec::new();
    let mut anomaly_iqr: Vec<Value> = Vec::new();
    for line in dsl
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let lower = line.to_lowercase();
        if let Some(rest) = lower.strip_prefix("required:") {
            for f in rest.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                required_fields.push(json!(f));
            }
            continue;
        }
        if let Some(rest) = lower.strip_prefix("unique:") {
            for f in rest.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                unique_fields.push(json!(f));
            }
            continue;
        }
        if let Some(rest) = lower.strip_prefix("range:") {
            let mut field = String::new();
            let mut min_v: Option<f64> = None;
            let mut max_v: Option<f64> = None;
            if let Some((f, expr)) = rest.split_once(' ') {
                field = f.trim().to_string();
                if let Some((_, rhs)) = expr.split_once(">=") {
                    let part = rhs.trim();
                    if let Some((n, rem)) = part.split_once("<=") {
                        min_v = n.trim().parse::<f64>().ok();
                        max_v = rem.trim().parse::<f64>().ok();
                    } else {
                        min_v = part.parse::<f64>().ok();
                    }
                }
            }
            if !field.is_empty() {
                range_checks.push(json!({"field": field, "min": min_v, "max": max_v}));
            }
            continue;
        }
        if let Some(rest) = lower.strip_prefix("anomaly_iqr:") {
            let mut field = String::new();
            let mut max_ratio = 0.10f64;
            for kv in rest.split(',') {
                let Some((k, v)) = kv.trim().split_once('=') else {
                    continue;
                };
                let key = k.trim();
                let val = v.trim();
                if key == "field" {
                    field = val.to_string();
                } else if key == "max_ratio" {
                    max_ratio = val.parse::<f64>().ok().unwrap_or(0.10).clamp(0.0, 1.0);
                }
            }
            if !field.is_empty() {
                anomaly_iqr.push(json!({"field": field, "max_ratio": max_ratio}));
            }
        }
    }
    json!({
        "required_fields": required_fields,
        "unique_fields": unique_fields,
        "range_checks": range_checks,
        "anomaly_iqr": anomaly_iqr
    })
}

pub(crate) fn run_quality_check_v3(req: QualityCheckV3Req) -> Result<QualityCheckResp, String> {
    let base = run_quality_check_v2(QualityCheckV2Req {
        run_id: req.run_id.clone(),
        rows: req.rows.clone(),
        rules: req.rules.clone(),
    })?;
    let rules = req.rules.as_object().cloned().unwrap_or_default();
    let mut violations = base
        .report
        .get("violations")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut passed = base.passed;
    if let Some(a) = rules.get("anomaly_iqr").and_then(|v| v.as_array()) {
        let mut details = Vec::new();
        for x in a {
            let Some(o) = x.as_object() else { continue };
            let field = o.get("field").and_then(|v| v.as_str()).unwrap_or("");
            if field.is_empty() {
                continue;
            }
            let vals = req
                .rows
                .iter()
                .filter_map(|r| {
                    r.as_object()
                        .and_then(|m| m.get(field))
                        .and_then(value_to_f64)
                })
                .collect::<Vec<_>>();
            if let Some((lo, hi)) = iqr_bounds(&vals) {
                let outliers = vals.iter().filter(|v| **v < lo || **v > hi).count();
                if outliers > 0 {
                    passed = false;
                    details.push(
                        json!({"field": field, "outliers": outliers, "lower": lo, "upper": hi}),
                    );
                }
            }
        }
        if !details.is_empty() {
            violations.push(json!({"rule":"anomaly_iqr","details":details}));
        }
    }
    if let Some(d) = rules.get("drift_psi").and_then(|v| v.as_object()) {
        let field = d.get("field").and_then(|v| v.as_str()).unwrap_or("");
        let threshold = d.get("max_psi").and_then(value_to_f64).unwrap_or(0.25);
        let expected = d
            .get("expected")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(value_to_f64).collect::<Vec<_>>())
            .unwrap_or_default();
        if !field.is_empty() && !expected.is_empty() {
            let actual = req
                .rows
                .iter()
                .filter_map(|r| {
                    r.as_object()
                        .and_then(|m| m.get(field))
                        .and_then(value_to_f64)
                })
                .collect::<Vec<_>>();
            if let Some(psi) = calc_psi(&expected, &actual, 10)
                && psi > threshold
            {
                passed = false;
                violations
                    .push(json!({"rule":"drift_psi","field":field,"psi":psi,"max_psi":threshold}));
            }
        }
    }
    let score = if violations.is_empty() {
        100.0
    } else {
        (100.0 - (violations.len() as f64 * 8.0)).max(0.0)
    };
    Ok(QualityCheckResp {
        ok: true,
        operator: "quality_check_v3".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        passed,
        report: json!({
            "rows": req.rows.len(),
            "violations": violations,
            "rule_count": rules.len(),
            "quality_score": score
        }),
    })
}

pub(crate) fn run_quality_check_v4(req: QualityCheckV4Req) -> Result<QualityCheckResp, String> {
    let mut rules = req.rules.clone();
    if let Some(dsl) = req.rules_dsl.as_ref().filter(|s| !s.trim().is_empty()) {
        let dsl_rules = parse_quality_rules_dsl(dsl);
        if let (Some(dst), Some(src)) = (rules.as_object_mut(), dsl_rules.as_object()) {
            for (k, v) in src {
                dst.insert(k.clone(), v.clone());
            }
        }
    }
    let rows = req.rows.clone();
    let mut out = run_quality_check_v3(QualityCheckV3Req {
        run_id: req.run_id.clone(),
        rows: req.rows,
        rules: rules.clone(),
    })?;
    out.operator = "quality_check_v4".to_string();
    let iqr_list = rules
        .get("anomaly_iqr")
        .and_then(|v| {
            if let Some(a) = v.as_array() {
                Some(a.clone())
            } else {
                v.as_object().map(|o| vec![Value::Object(o.clone())])
            }
        })
        .unwrap_or_default();
    let mut iqr_report = Vec::new();
    for iqr in iqr_list {
        let iqr_cfg = iqr.as_object().cloned().unwrap_or_default();
        let field = iqr_cfg
            .get("field")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                rows.iter().find_map(|r| {
                    let o = r.as_object()?;
                    o.iter()
                        .find(|(_, v)| value_to_f64(v).is_some())
                        .map(|(k, _)| k.clone())
                })
            })
            .unwrap_or_default();
        if field.is_empty() {
            continue;
        }
        let mut vals = rows
            .iter()
            .filter_map(|r| r.as_object())
            .filter_map(|o| o.get(&field).and_then(value_to_f64))
            .filter(|v| v.is_finite())
            .collect::<Vec<_>>();
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if vals.len() < 4 {
            continue;
        }
        let q1 = approx_percentile(vals.clone(), 0.25, vals.len()).unwrap_or(0.0);
        let q3 = approx_percentile(vals.clone(), 0.75, vals.len()).unwrap_or(0.0);
        let iqr = (q3 - q1).max(0.0);
        let lower = q1 - 1.5 * iqr;
        let upper = q3 + 1.5 * iqr;
        let outliers = vals.iter().filter(|v| **v < lower || **v > upper).count();
        let ratio = outliers as f64 / vals.len() as f64;
        let max_ratio = iqr_cfg
            .get("max_ratio")
            .and_then(value_to_f64)
            .unwrap_or(0.10);
        if ratio > max_ratio {
            out.passed = false;
        }
        iqr_report.push(json!({
            "field": field,
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower": lower,
            "upper": upper,
            "outliers": outliers,
            "ratio": ratio,
            "max_ratio": max_ratio
        }));
    }
    if !iqr_report.is_empty()
        && let Some(report) = out.report.as_object_mut()
    {
        report.insert("anomaly_iqr".to_string(), Value::Array(iqr_report));
    }
    Ok(out)
}
