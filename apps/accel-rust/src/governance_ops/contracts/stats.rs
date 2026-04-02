use super::*;
use crate::operator_catalog::{
    capability_catalog_entries, metadata_domain_summaries, workflow_catalog_entries,
};

pub(crate) fn run_runtime_stats_v1(req: RuntimeStatsV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let mut store = load_kv_store(&runtime_stats_store_path());
    if op == "record" {
        let operator = req.operator.unwrap_or_else(|| "unknown".to_string());
        let entry = store.entry(operator.clone()).or_insert(json!({
            "calls": 0u64, "ok": 0u64, "err": 0u64, "durations": [], "rows_in": 0u64, "rows_out": 0u64, "errors": {}
        }));
        let obj = entry
            .as_object_mut()
            .ok_or_else(|| "runtime_stats_v1 bad entry".to_string())?;
        let calls = obj.get("calls").and_then(|v| v.as_u64()).unwrap_or(0) + 1;
        let ok =
            obj.get("ok").and_then(|v| v.as_u64()).unwrap_or(0) + u64::from(req.ok.unwrap_or(true));
        let err = obj.get("err").and_then(|v| v.as_u64()).unwrap_or(0)
            + u64::from(!req.ok.unwrap_or(true));
        let rows_in = obj.get("rows_in").and_then(|v| v.as_u64()).unwrap_or(0)
            + req.rows_in.unwrap_or(0) as u64;
        let rows_out = obj.get("rows_out").and_then(|v| v.as_u64()).unwrap_or(0)
            + req.rows_out.unwrap_or(0) as u64;
        let mut durs = obj
            .get("durations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        if let Some(d) = req.duration_ms {
            durs.push(json!(d as u64));
            if durs.len() > 500 {
                durs = durs.split_off(durs.len() - 500);
            }
        }
        let mut errs = obj
            .get("errors")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        if let Some(ec) = req.error_code {
            let n = errs.get(&ec).and_then(|v| v.as_u64()).unwrap_or(0) + 1;
            errs.insert(ec, json!(n));
        }
        obj.insert("calls".to_string(), json!(calls));
        obj.insert("ok".to_string(), json!(ok));
        obj.insert("err".to_string(), json!(err));
        obj.insert("rows_in".to_string(), json!(rows_in));
        obj.insert("rows_out".to_string(), json!(rows_out));
        obj.insert("durations".to_string(), Value::Array(durs));
        obj.insert("errors".to_string(), Value::Object(errs));
        save_kv_store(&runtime_stats_store_path(), &store)?;
        return Ok(
            json!({"ok": true, "operator":"runtime_stats_v1", "status":"done", "run_id": req.run_id, "op":"record", "target": operator}),
        );
    }
    if op == "reset" {
        store.clear();
        save_kv_store(&runtime_stats_store_path(), &store)?;
        return Ok(
            json!({"ok": true, "operator":"runtime_stats_v1", "status":"done", "run_id": req.run_id, "op":"reset"}),
        );
    }
    let mut items = Vec::new();
    for (k, v) in store {
        let durs = v
            .get("durations")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|x| x.as_u64())
            .collect::<Vec<_>>();
        let mut s = durs.clone();
        s.sort();
        let p50 = if s.is_empty() { 0 } else { s[s.len() / 2] };
        let p95 = if s.is_empty() {
            0
        } else {
            s[((s.len() as f64 * 0.95).floor() as usize).min(s.len() - 1)]
        };
        items.push(json!({
            "operator": k,
            "calls": v.get("calls").and_then(|x| x.as_u64()).unwrap_or(0),
            "ok": v.get("ok").and_then(|x| x.as_u64()).unwrap_or(0),
            "err": v.get("err").and_then(|x| x.as_u64()).unwrap_or(0),
            "rows_in": v.get("rows_in").and_then(|x| x.as_u64()).unwrap_or(0),
            "rows_out": v.get("rows_out").and_then(|x| x.as_u64()).unwrap_or(0),
            "p50_ms": p50,
            "p95_ms": p95,
            "errors": v.get("errors").cloned().unwrap_or_else(|| json!({}))
        }));
    }
    items.sort_by(|a, b| {
        let av = a.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        let bv = b.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        av.cmp(bv)
    });
    Ok(
        json!({"ok": true, "operator":"runtime_stats_v1", "status":"done", "run_id": req.run_id, "op":"summary", "items": items}),
    )
}

pub(crate) fn run_capabilities_v1(req: CapabilitiesV1Req) -> Result<Value, String> {
    let mut published = capability_catalog_entries();
    let mut workflow = workflow_catalog_entries();
    if let Some(allow) = req.include_ops {
        let set: HashSet<String> = allow
            .into_iter()
            .map(|x| x.trim().to_lowercase())
            .filter(|x| !x.is_empty())
            .collect();
        if !set.is_empty() {
            published.retain(|entry| set.contains(&entry.operator.to_lowercase()));
            workflow.retain(|entry| set.contains(&entry.operator.to_lowercase()));
        }
    }
    let mut ops = published
        .iter()
        .map(|entry| entry.to_capabilities_item())
        .collect::<Vec<_>>();
    ops.sort_by(|a, b| {
        let av = a.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        let bv = b.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        av.cmp(bv)
    });
    let mut workflow_ops = workflow
        .iter()
        .map(|entry| entry.to_capabilities_item())
        .collect::<Vec<_>>();
    workflow_ops.sort_by(|a, b| {
        let av = a.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        let bv = b.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        av.cmp(bv)
    });
    Ok(json!({
        "ok": true,
        "operator": "capabilities_v1",
        "status": "done",
        "run_id": req.run_id,
        "schema_version": "aiwf.capabilities.v1",
        "domains": metadata_domain_summaries(&published),
        "workflow_domains": metadata_domain_summaries(&workflow),
        "workflow_items": workflow_ops,
        "items": ops
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn capabilities_v1_preserves_published_operator_set_and_ordering() {
        let out = run_capabilities_v1(CapabilitiesV1Req {
            run_id: Some("caps-1".to_string()),
            include_ops: None,
        })
        .expect("capabilities_v1 response");
        let items = out
            .get("items")
            .and_then(|v| v.as_array())
            .expect("capability items");
        let operators = items
            .iter()
            .filter_map(|item| item.get("operator").and_then(|v| v.as_str()))
            .collect::<Vec<_>>();
        let expected = BTreeSet::from([
            "aggregate_rows_v4",
            "anomaly_explain_v1",
            "capabilities_v1",
            "columnar_eval_v1",
            "contract_regression_v1",
            "explain_plan_v2",
            "failure_policy_v1",
            "finance_ratio_v1",
            "incremental_plan_v1",
            "io_contract_v1",
            "join_rows_v4",
            "lineage_provenance_v1",
            "load_rows_v3",
            "operator_policy_v1",
            "optimizer_adaptive_v2",
            "perf_baseline_v1",
            "plugin_operator_v1",
            "postprocess_rows_v1",
            "quality_check_v4",
            "runtime_stats_v1",
            "stream_reliability_v1",
            "stream_state_v2",
            "stream_window_v2",
            "tenant_isolation_v1",
            "transform_rows_v3",
            "vector_index_v2_build",
            "vector_index_v2_search",
        ]);
        assert_eq!(
            operators.iter().copied().collect::<BTreeSet<_>>(),
            expected,
            "capability operator set changed"
        );
        assert!(
            operators.windows(2).all(|window| window[0] < window[1]),
            "capability items must stay operator-sorted"
        );
        let transform = items
            .iter()
            .find(|item| item.get("operator").and_then(|v| v.as_str()) == Some("transform_rows_v3"))
            .expect("transform_rows_v3 metadata");
        assert_eq!(
            transform.get("domain").and_then(|v| v.as_str()),
            Some("transform")
        );
        assert_eq!(
            transform.get("catalog").and_then(|v| v.as_str()),
            Some("operators.transform")
        );
        assert_eq!(
            transform.get("checkpoint").and_then(|v| v.as_bool()),
            Some(true)
        );
        let workflow_items = out
            .get("workflow_items")
            .and_then(|v| v.as_array())
            .expect("workflow_items");
        assert!(
            workflow_items.iter().any(|item| item.get("operator").and_then(|v| v.as_str()) == Some("transform_rows_v2")),
            "workflow catalog should expose full workflow step set"
        );
        let workflow_domains = out
            .get("workflow_domains")
            .and_then(|v| v.as_array())
            .expect("workflow_domains");
        assert!(workflow_domains.iter().any(|item| item.get("name").and_then(|v| v.as_str()) == Some("transform")));
    }

    #[test]
    fn capabilities_v1_filters_case_insensitively_and_keeps_catalog_metadata() {
        let out = run_capabilities_v1(CapabilitiesV1Req {
            run_id: None,
            include_ops: Some(vec!["CAPABILITIES_V1".to_string(), "missing".to_string()]),
        })
        .expect("filtered capabilities");
        let items = out
            .get("items")
            .and_then(|v| v.as_array())
            .expect("capability items");
        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(
            item.get("operator").and_then(|v| v.as_str()),
            Some("capabilities_v1")
        );
        assert_eq!(
            item.get("domain").and_then(|v| v.as_str()),
            Some("governance")
        );
        assert_eq!(
            item.get("catalog").and_then(|v| v.as_str()),
            Some("governance.contracts")
        );
        let workflow_items = out
            .get("workflow_items")
            .and_then(|v| v.as_array())
            .expect("workflow_items");
        assert_eq!(workflow_items.len(), 1);
    }
}
