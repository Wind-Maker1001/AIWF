use super::*;

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
