use super::helpers::{calc_psi, iqr_bounds, parse_quality_rules_dsl};
use super::*;

pub(crate) fn run_quality_check_v3(req: QualityCheckV3Req) -> Result<QualityCheckResp, String> {
    let base = run_quality_check_v2(QualityCheckV2Req {
        run_id: req.run_id.clone(),
        rows: req.rows.clone(),
        rules: req.rules.clone(),
        metrics: None,
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
