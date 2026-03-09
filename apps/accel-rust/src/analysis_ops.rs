use crate::{
    api_types::{
        ChartDataPrepReq, ConstraintSolverReq, DiffAuditReq, EntityLinkReq, FeatureStoreGetReq,
        FeatureStoreUpsertReq, RuleSimulatorReq, StatsReq, TableReconstructReq, TimeSeriesReq,
    },
    operators::transform::{TransformRowsReq, run_transform_rows_v2},
    transform_support::{value_to_f64, value_to_string_or_null},
};
use accel_rust::metrics::{acquire_file_lock, release_file_lock};
use chrono::{NaiveDate, NaiveDateTime};
use regex::Regex;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use statrs::distribution::{ContinuousCDF, StudentsT};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::{Path, PathBuf},
};

pub(crate) fn feature_store_path() -> PathBuf {
    env::var("AIWF_FEATURE_STORE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".").join("tmp").join("feature_store.json"))
}

pub(crate) fn load_feature_store() -> HashMap<String, Value> {
    let p = feature_store_path();
    if let Ok(lock) = acquire_file_lock(&p) {
        let out = (|| {
            let Ok(txt) = fs::read_to_string(&p) else {
                return HashMap::new();
            };
            serde_json::from_str::<HashMap<String, Value>>(&txt).unwrap_or_default()
        })();
        release_file_lock(&lock);
        return out;
    }
    let Ok(txt) = fs::read_to_string(&p) else {
        return HashMap::new();
    };
    serde_json::from_str::<HashMap<String, Value>>(&txt).unwrap_or_default()
}

pub(crate) fn save_feature_store(store: &HashMap<String, Value>) -> Result<(), String> {
    let p = feature_store_path();
    let lock = acquire_file_lock(&p)?;
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create feature store dir: {e}"))?;
    }
    let s =
        serde_json::to_string_pretty(store).map_err(|e| format!("serialize feature store: {e}"))?;
    let out = fs::write(&p, s).map_err(|e| format!("write feature store: {e}"));
    release_file_lock(&lock);
    out
}

pub(crate) fn run_time_series_v1(req: TimeSeriesReq) -> Result<Value, String> {
    let window = req.window.unwrap_or(3).max(1);
    let groups = req.group_by.unwrap_or_default();
    let mut grouped: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for r in req.rows {
        let Some(obj) = r.as_object() else { continue };
        let k = if groups.is_empty() {
            "__all__".to_string()
        } else {
            groups
                .iter()
                .map(|g| value_to_string_or_null(obj.get(g)))
                .collect::<Vec<_>>()
                .join("|")
        };
        grouped.entry(k).or_default().push(obj.clone());
    }
    let mut out = Vec::new();
    for (_k, mut rows) in grouped {
        rows.sort_by(|a, b| {
            let av = value_to_string_or_null(a.get(&req.time_field));
            let bv = value_to_string_or_null(b.get(&req.time_field));
            parse_time_order_key(&av).cmp(&parse_time_order_key(&bv))
        });
        for i in 0..rows.len() {
            let mut row = rows[i].clone();
            let cur = row
                .get(&req.value_field)
                .and_then(value_to_f64)
                .unwrap_or(0.0);
            let start = i.saturating_sub(window - 1);
            let win = rows[start..=i]
                .iter()
                .filter_map(|r| r.get(&req.value_field).and_then(value_to_f64))
                .collect::<Vec<_>>();
            let ma = if win.is_empty() {
                Value::Null
            } else {
                json!(win.iter().sum::<f64>() / win.len() as f64)
            };
            let mom = if i >= 1 {
                let prev = rows[i - 1]
                    .get(&req.value_field)
                    .and_then(value_to_f64)
                    .unwrap_or(0.0);
                json!(cur - prev)
            } else {
                Value::Null
            };
            let yoy = if i >= 12 {
                let prev = rows[i - 12]
                    .get(&req.value_field)
                    .and_then(value_to_f64)
                    .unwrap_or(0.0);
                if prev.abs() < f64::EPSILON {
                    Value::Null
                } else {
                    json!((cur - prev) / prev)
                }
            } else {
                Value::Null
            };
            row.insert("ts_moving_avg".to_string(), ma);
            row.insert("ts_mom".to_string(), mom);
            row.insert("ts_yoy".to_string(), yoy);
            out.push(Value::Object(row));
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "time_series_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out,
        "stats": {"window": window}
    }))
}

pub(crate) fn parse_time_order_key(s: &str) -> i64 {
    let t = s.trim();
    if let Ok(v) = t.parse::<i64>() {
        return v;
    }
    let fmts_dt = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
    ];
    for f in fmts_dt {
        if let Ok(dt) = NaiveDateTime::parse_from_str(t, f) {
            return dt.and_utc().timestamp();
        }
    }
    let fmts_d = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m", "%Y/%m", "%Y%m"];
    for f in fmts_d {
        let parsed = if f == "%Y-%m" {
            NaiveDate::parse_from_str(&format!("{t}-01"), "%Y-%m-%d")
        } else if f == "%Y/%m" {
            NaiveDate::parse_from_str(&format!("{t}/01"), "%Y/%m/%d")
        } else if f == "%Y%m" {
            NaiveDate::parse_from_str(&format!("{t}01"), "%Y%m%d")
        } else {
            NaiveDate::parse_from_str(t, f)
        };
        if let Ok(d) = parsed {
            return d
                .and_hms_opt(0, 0, 0)
                .map(|dt| dt.and_utc().timestamp())
                .unwrap_or(i64::MAX - 1);
        }
    }
    i64::MAX
}

pub(crate) fn run_stats_v1(req: StatsReq) -> Result<Value, String> {
    let pairs = req
        .rows
        .iter()
        .filter_map(|r| {
            let o = r.as_object()?;
            let x = o.get(&req.x_field).and_then(value_to_f64)?;
            let y = o.get(&req.y_field).and_then(value_to_f64)?;
            Some((x, y))
        })
        .collect::<Vec<_>>();
    if pairs.len() < 2 {
        return Err("stats_v1 requires at least 2 numeric pairs".to_string());
    }
    let n = pairs.len() as f64;
    let sum_x = pairs.iter().map(|(x, _)| *x).sum::<f64>();
    let sum_y = pairs.iter().map(|(_, y)| *y).sum::<f64>();
    let mean_x = sum_x / n;
    let mean_y = sum_y / n;
    let sxy = pairs
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>();
    let sxx = pairs.iter().map(|(x, _)| (x - mean_x).powi(2)).sum::<f64>();
    let syy = pairs.iter().map(|(_, y)| (y - mean_y).powi(2)).sum::<f64>();
    let corr = if sxx <= 0.0 || syy <= 0.0 {
        0.0
    } else {
        sxy / (sxx.sqrt() * syy.sqrt())
    };
    let slope = if sxx <= 0.0 { 0.0 } else { sxy / sxx };
    let intercept = mean_y - slope * mean_x;
    let mut residual_ss = 0.0;
    for (x, y) in &pairs {
        let pred = intercept + slope * *x;
        residual_ss += (*y - pred).powi(2);
    }
    let dof = (pairs.len() as f64 - 2.0).max(1.0);
    let stderr = if sxx <= 0.0 {
        f64::INFINITY
    } else {
        (residual_ss / dof / sxx).sqrt()
    };
    let t = if !stderr.is_finite() || stderr <= 0.0 {
        0.0
    } else {
        slope / stderr
    };
    let p = p_value_from_t(t.abs(), dof);
    let tcrit = if let Ok(dist) = StudentsT::new(0.0, 1.0, dof) {
        dist.inverse_cdf(0.975)
    } else {
        1.96
    };
    let ci_low = slope - tcrit * stderr;
    let ci_high = slope + tcrit * stderr;
    let robust_median_y = {
        let mut ys = pairs.iter().map(|(_, y)| *y).collect::<Vec<_>>();
        ys.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        ys[ys.len() / 2]
    };
    Ok(json!({
        "ok": true,
        "operator": "stats_v1",
        "status": "done",
        "run_id": req.run_id,
        "metrics": {
            "count": pairs.len(),
            "correlation": corr,
            "slope": slope,
            "intercept": intercept,
            "mean_x": mean_x,
            "mean_y": mean_y,
            "slope_stderr": stderr,
            "slope_t": t,
            "slope_p_value": p,
            "slope_ci95": [ci_low, ci_high],
            "median_y": robust_median_y
        }
    }))
}

pub(crate) fn normalize_entity(s: &str) -> String {
    s.trim()
        .to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c.is_alphanumeric() {
                c
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn run_entity_linking_v1(req: EntityLinkReq) -> Result<Value, String> {
    let id_field = req.id_field.unwrap_or_else(|| "entity_id".to_string());
    let mut out = Vec::new();
    let mut dict = Map::new();
    for r in req.rows {
        let Some(mut obj) = r.as_object().cloned() else {
            continue;
        };
        let raw = value_to_string_or_null(obj.get(&req.field));
        let norm = normalize_entity(&raw);
        let mut h = Sha256::new();
        h.update(norm.as_bytes());
        let id = format!("{:x}", h.finalize());
        let short = id.chars().take(12).collect::<String>();
        obj.insert(id_field.clone(), Value::String(short.clone()));
        obj.insert("entity_norm".to_string(), Value::String(norm.clone()));
        dict.insert(short, Value::String(norm));
        out.push(Value::Object(obj));
    }
    Ok(json!({
        "ok": true,
        "operator": "entity_linking_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out,
        "dictionary": dict
    }))
}

pub(crate) fn run_table_reconstruct_v1(req: TableReconstructReq) -> Result<Value, String> {
    let lines = if let Some(v) = req.lines {
        v
    } else {
        req.text
            .unwrap_or_default()
            .replace("\r\n", "\n")
            .split('\n')
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    };
    let delim = req.delimiter.unwrap_or_else(|| "\\s{2,}|\\t".to_string());
    let re = Regex::new(&delim).map_err(|e| format!("invalid delimiter regex: {e}"))?;
    let mut rows = Vec::new();
    let mut max_cols = 0usize;
    for line in lines {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let cols = re
            .split(t)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        if cols.is_empty() {
            continue;
        }
        max_cols = max_cols.max(cols.len());
        rows.push(cols);
    }
    if rows.is_empty() {
        return Err("table_reconstruct_v1 empty input".to_string());
    }
    let mut norm = Vec::new();
    for mut r in rows {
        if r.len() < max_cols {
            r.resize(max_cols, "".to_string());
        } else if r.len() > max_cols {
            r.truncate(max_cols);
        }
        norm.push(r);
    }
    let header = norm.first().cloned().unwrap_or_default();
    let body = norm
        .iter()
        .skip(1)
        .map(|r| json!({"cells": r, "col_count": r.len()}))
        .collect::<Vec<_>>();
    Ok(json!({
        "ok": true,
        "operator": "table_reconstruct_v1",
        "status": "done",
        "run_id": req.run_id,
        "header": header,
        "rows": body,
        "stats": {"col_count": max_cols, "row_count": norm.len()}
    }))
}

pub(crate) fn p_value_from_t(t_abs: f64, dof: f64) -> f64 {
    if !dof.is_finite() || dof <= 0.0 {
        return 1.0;
    }
    if let Ok(dist) = StudentsT::new(0.0, 1.0, dof) {
        2.0 * (1.0 - dist.cdf(t_abs.max(0.0)))
    } else {
        1.0
    }
}

pub(crate) fn run_feature_store_upsert_v1(req: FeatureStoreUpsertReq) -> Result<Value, String> {
    let mut store = load_feature_store();
    let mut upserted = 0usize;
    for r in req.rows {
        let Some(obj) = r.as_object() else { continue };
        let key = value_to_string_or_null(obj.get(&req.key_field));
        if key.trim().is_empty() || key == "null" {
            continue;
        }
        store.insert(key, Value::Object(obj.clone()));
        upserted += 1;
    }
    save_feature_store(&store)?;
    Ok(json!({
        "ok": true,
        "operator": "feature_store_v1_upsert",
        "status": "done",
        "run_id": req.run_id,
        "upserted": upserted,
        "total_keys": store.len()
    }))
}

pub(crate) fn run_feature_store_get_v1(req: FeatureStoreGetReq) -> Result<Value, String> {
    let store = load_feature_store();
    Ok(json!({
        "ok": true,
        "operator": "feature_store_v1_get",
        "status": "done",
        "run_id": req.run_id,
        "key": req.key,
        "value": store.get(&req.key).cloned().unwrap_or(Value::Null)
    }))
}

pub(crate) fn run_rule_simulator_v1(req: RuleSimulatorReq) -> Result<Value, String> {
    let base = run_transform_rows_v2(TransformRowsReq {
        run_id: req.run_id.clone(),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(req.rows.clone()),
        rules: Some(req.rules),
        rules_dsl: None,
        quality_gates: None,
        schema_hint: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    })?;
    let cand = run_transform_rows_v2(TransformRowsReq {
        run_id: req.run_id.clone(),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(req.rows),
        rules: Some(req.candidate_rules),
        rules_dsl: None,
        quality_gates: None,
        schema_hint: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    })?;
    let base_rows = base.rows;
    let cand_rows = cand.rows;
    let min_n = base_rows.len().min(cand_rows.len());
    let mut field_changed = HashMap::<String, usize>::new();
    for i in 0..min_n {
        let Some(bm) = base_rows[i].as_object() else {
            continue;
        };
        let Some(cm) = cand_rows[i].as_object() else {
            continue;
        };
        let keys = bm.keys().chain(cm.keys()).cloned().collect::<HashSet<_>>();
        for k in keys {
            let bv = bm.get(&k).cloned().unwrap_or(Value::Null);
            let cv = cm.get(&k).cloned().unwrap_or(Value::Null);
            if bv != cv {
                *field_changed.entry(k).or_insert(0) += 1;
            }
        }
    }
    let mut top_changed = field_changed
        .into_iter()
        .map(|(k, v)| json!({"field":k,"changed_rows":v}))
        .collect::<Vec<_>>();
    top_changed.sort_by(|a, b| {
        let av = a.get("changed_rows").and_then(|v| v.as_u64()).unwrap_or(0);
        let bv = b.get("changed_rows").and_then(|v| v.as_u64()).unwrap_or(0);
        bv.cmp(&av)
    });
    Ok(json!({
        "ok": true,
        "operator": "rule_simulator_v1",
        "status": "done",
        "run_id": req.run_id,
        "baseline_rows": base.stats.output_rows,
        "candidate_rows": cand.stats.output_rows,
        "delta_rows": cand.stats.output_rows as i64 - base.stats.output_rows as i64,
        "row_overlap_compared": min_n,
        "field_impact_top": top_changed.into_iter().take(20).collect::<Vec<_>>(),
        "baseline_quality": base.quality,
        "candidate_quality": cand.quality
    }))
}

pub(crate) fn run_constraint_solver_v1(req: ConstraintSolverReq) -> Result<Value, String> {
    let mut violations = Vec::new();
    for (idx, row) in req.rows.iter().enumerate() {
        let Some(obj) = row.as_object() else { continue };
        for c in &req.constraints {
            let Some(o) = c.as_object() else { continue };
            let kind = o.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            match kind {
                "sum_equals" => {
                    let left = o
                        .get("left")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>();
                    let right = o.get("right").and_then(|v| v.as_str()).unwrap_or("");
                    let lv = left
                        .iter()
                        .map(|f| obj.get(f).and_then(value_to_f64).unwrap_or(0.0))
                        .sum::<f64>();
                    let rv = obj.get(right).and_then(value_to_f64).unwrap_or(0.0);
                    let tol = o.get("tolerance").and_then(value_to_f64).unwrap_or(1e-6);
                    if (lv - rv).abs() > tol {
                        violations.push(json!({"row_index":idx,"kind":"sum_equals","left":left,"right":right,"left_value":lv,"right_value":rv,"tolerance":tol}));
                    }
                }
                "non_negative" => {
                    let field = o.get("field").and_then(|v| v.as_str()).unwrap_or("");
                    let v = obj.get(field).and_then(value_to_f64).unwrap_or(0.0);
                    if v < 0.0 {
                        violations.push(
                            json!({"row_index":idx,"kind":"non_negative","field":field,"value":v}),
                        );
                    }
                }
                _ => {}
            }
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "constraint_solver_v1",
        "status": "done",
        "run_id": req.run_id,
        "passed": violations.is_empty(),
        "violations": violations
    }))
}

pub(crate) fn run_chart_data_prep_v1(req: ChartDataPrepReq) -> Result<Value, String> {
    let top_n = req.top_n.unwrap_or(100).max(1);
    let mut m: HashMap<String, HashMap<String, f64>> = HashMap::new();
    for r in req.rows {
        let Some(obj) = r.as_object() else { continue };
        let cat = value_to_string_or_null(obj.get(&req.category_field));
        let ser = req
            .series_field
            .as_ref()
            .map(|f| value_to_string_or_null(obj.get(f)))
            .unwrap_or_else(|| "value".to_string());
        let val = obj
            .get(&req.value_field)
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        *m.entry(cat).or_default().entry(ser).or_insert(0.0) += val;
    }
    let mut cats = m.into_iter().collect::<Vec<_>>();
    cats.sort_by(|a, b| a.0.cmp(&b.0));
    cats.truncate(top_n);
    let categories = cats
        .iter()
        .map(|(c, _)| Value::String(c.clone()))
        .collect::<Vec<_>>();
    let mut series_keys = HashSet::new();
    for (_, sm) in &cats {
        for k in sm.keys() {
            series_keys.insert(k.clone());
        }
    }
    let mut series = Vec::new();
    let mut sk = series_keys.into_iter().collect::<Vec<_>>();
    sk.sort();
    for s in sk {
        let data = cats
            .iter()
            .map(|(_, sm)| json!(sm.get(&s).copied().unwrap_or(0.0)))
            .collect::<Vec<_>>();
        series.push(json!({"name": s, "data": data}));
    }
    Ok(json!({
        "ok": true,
        "operator": "chart_data_prep_v1",
        "status": "done",
        "run_id": req.run_id,
        "chart": {"categories": categories, "series": series}
    }))
}

pub(crate) fn run_diff_audit_v1(req: DiffAuditReq) -> Result<Value, String> {
    if req.keys.is_empty() {
        return Err("diff_audit_v1 requires keys".to_string());
    }
    let key_of = |o: &Map<String, Value>| {
        req.keys
            .iter()
            .map(|k| value_to_string_or_null(o.get(k)))
            .collect::<Vec<_>>()
            .join("|")
    };
    let mut left = HashMap::<String, Map<String, Value>>::new();
    let mut right = HashMap::<String, Map<String, Value>>::new();
    for r in req.left_rows {
        if let Some(o) = r.as_object() {
            left.insert(key_of(o), o.clone());
        }
    }
    for r in req.right_rows {
        if let Some(o) = r.as_object() {
            right.insert(key_of(o), o.clone());
        }
    }
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();
    for (k, rv) in &right {
        if !left.contains_key(k) {
            added.push(Value::Object(rv.clone()));
        }
    }
    for (k, lv) in &left {
        if !right.contains_key(k) {
            removed.push(Value::Object(lv.clone()));
        } else if let Some(rv) = right.get(k)
            && lv != rv
        {
            changed.push(json!({"key":k,"left":lv,"right":rv}));
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "diff_audit_v1",
        "status": "done",
        "run_id": req.run_id,
        "summary": {"added": added.len(), "removed": removed.len(), "changed": changed.len()},
        "added": added,
        "removed": removed,
        "changed": changed
    }))
}
