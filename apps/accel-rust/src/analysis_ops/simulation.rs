use super::*;

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
