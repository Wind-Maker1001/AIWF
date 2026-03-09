use super::*;

pub(super) fn iqr_bounds(vals: &[f64]) -> Option<(f64, f64)> {
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

pub(super) fn calc_psi(expected: &[f64], actual: &[f64], bins: usize) -> Option<f64> {
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

pub(super) fn parse_quality_rules_dsl(dsl: &str) -> Value {
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
