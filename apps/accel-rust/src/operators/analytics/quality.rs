use super::*;
use crate::transform_support::{
    is_missing, value_to_f64, value_to_string, value_to_string_or_null,
};
use serde_json::{Value, json};

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
