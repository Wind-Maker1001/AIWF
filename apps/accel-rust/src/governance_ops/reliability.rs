use crate::{
    api_types::{
        ContractRegressionV1Req, LineageProvenanceV1Req, PerfBaselineV1Req, ProvenanceSignReq,
        StreamReliabilityV1Req, VectorIndexBuildV2Req, VectorIndexEvalV2Req,
        VectorIndexSearchV2Req,
    },
    governance_ops::io_contract_errors,
    operators::workflow::{LineageV3Req, run_lineage_v3},
    platform_ops::{
        load_kv_store, perf_baseline_store_path, run_provenance_sign_v1, save_kv_store,
        stream_reliability_store_path, vector_index_v2_store_path,
    },
    transform_support::{
        unique_trace, utc_now_iso, value_to_f64, value_to_string, value_to_string_or_null,
    },
};
use serde_json::{Map, Value, json};
use std::collections::HashSet;

fn normalized_msg_id(value: Option<String>) -> Option<String> {
    value.and_then(|item| {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn effective_stream_msg_id(req: &StreamReliabilityV1Req) -> String {
    normalized_msg_id(req.msg_id.clone()).unwrap_or_else(|| unique_trace("stream_reliability_v1"))
}

pub(crate) fn run_vector_index_build_v2(req: VectorIndexBuildV2Req) -> Result<Value, String> {
    let shard = req.shard.unwrap_or_else(|| "default".to_string());
    let mut docs = req
        .rows
        .iter()
        .filter_map(|r| r.as_object())
        .map(|o| {
            let id = value_to_string_or_null(o.get(&req.id_field));
            let text = value_to_string_or_null(o.get(&req.text_field));
            let mut meta = Map::new();
            for f in req.metadata_fields.clone().unwrap_or_default() {
                if let Some(v) = o.get(&f) {
                    meta.insert(f, v.clone());
                }
            }
            json!({"id": id, "text": text, "meta": meta})
        })
        .collect::<Vec<_>>();
    let mut store = load_kv_store(&vector_index_v2_store_path());
    if req.replace.unwrap_or(false) {
        store.insert(shard.clone(), Value::Array(docs.clone()));
    } else {
        let mut arr = store
            .get(&shard)
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        arr.append(&mut docs);
        store.insert(shard.clone(), Value::Array(arr));
    }
    save_kv_store(&vector_index_v2_store_path(), &store)?;
    let size = store
        .get(&shard)
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    Ok(
        json!({"ok": true,"operator":"vector_index_v2_build","status":"done","run_id":req.run_id,"shard":shard,"size":size}),
    )
}

pub(crate) fn run_vector_index_search_v2(req: VectorIndexSearchV2Req) -> Result<Value, String> {
    let q = req.query.to_lowercase();
    let top_k = req.top_k.unwrap_or(5).max(1);
    let store = load_kv_store(&vector_index_v2_store_path());
    let mut docs = Vec::new();
    if let Some(shard) = req.shard.as_ref() {
        docs.extend(
            store
                .get(shard)
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
        );
    } else {
        for v in store.values() {
            docs.extend(v.as_array().cloned().unwrap_or_default());
        }
    }
    let filter = req.filter_eq.and_then(|v| v.as_object().cloned());
    let mut scored = Vec::<(f64, Value)>::new();
    for d in docs {
        let Some(o) = d.as_object() else { continue };
        if let Some(fm) = filter.as_ref() {
            let meta = o
                .get("meta")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            let mut pass = true;
            for (k, v) in fm {
                if value_to_string_or_null(meta.get(k)) != value_to_string(v) {
                    pass = false;
                    break;
                }
            }
            if !pass {
                continue;
            }
        }
        let text = o
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        let overlap = q.chars().filter(|c| text.contains(*c)).count() as f64;
        let mut score = overlap / (q.len().max(1) as f64);
        if let Some(field) = req.rerank_meta_field.as_ref() {
            let w = req.rerank_meta_weight.unwrap_or(0.0);
            if w.abs() > 0.000001 {
                let meta = o
                    .get("meta")
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();
                let mv = meta.get(field).and_then(value_to_f64).unwrap_or(0.0);
                score += mv * w;
            }
        }
        scored.push((score, Value::Object(o.clone())));
    }
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let hits = scored
        .into_iter()
        .take(top_k)
        .map(|(s, v)| json!({"score": s, "doc": v}))
        .collect::<Vec<_>>();
    Ok(
        json!({"ok": true, "operator":"vector_index_v2_search","status":"done","run_id":req.run_id,"hits":hits}),
    )
}

pub(crate) fn run_vector_index_eval_v2(req: VectorIndexEvalV2Req) -> Result<Value, String> {
    if req.cases.is_empty() {
        return Err("vector_index_v2_eval requires cases".to_string());
    }
    let k = req.top_k.unwrap_or(5).max(1);
    let mut hit = 0usize;
    let mut mrr = 0.0f64;
    let mut total = 0usize;
    let mut details = Vec::new();
    for c in req.cases {
        let Some(o) = c.as_object() else { continue };
        let q = o
            .get("query")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if q.trim().is_empty() {
            continue;
        }
        let expected = o
            .get("expected_ids")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<HashSet<String>>();
        let out = run_vector_index_search_v2(VectorIndexSearchV2Req {
            run_id: req.run_id.clone(),
            query: q.clone(),
            top_k: Some(k),
            shard: req.shard.clone(),
            filter_eq: None,
            rerank_meta_field: None,
            rerank_meta_weight: None,
        })?;
        let hits_arr = out
            .get("hits")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut found_rank = 0usize;
        for (idx, h) in hits_arr.iter().enumerate() {
            let id = h
                .get("doc")
                .and_then(|v| v.get("id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if expected.contains(id) {
                found_rank = idx + 1;
                break;
            }
        }
        total += 1;
        if found_rank > 0 {
            hit += 1;
            mrr += 1.0 / (found_rank as f64);
        }
        details.push(json!({"query": q, "found_rank": found_rank, "hit": found_rank > 0}));
    }
    let recall = if total > 0 {
        hit as f64 / total as f64
    } else {
        0.0
    };
    let mrr_score = if total > 0 { mrr / total as f64 } else { 0.0 };
    Ok(json!({
        "ok": true,
        "operator": "vector_index_v2_eval",
        "status": "done",
        "run_id": req.run_id,
        "top_k": k,
        "cases": total,
        "recall_at_k": recall,
        "mrr": mrr_score,
        "details": details
    }))
}

pub(crate) fn run_stream_reliability_v1(req: StreamReliabilityV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let mut store = load_kv_store(&stream_reliability_store_path());
    let key = req.stream_key.clone();
    let mut root = store
        .remove(&key)
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    let mut dedup = root
        .get("dedup")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|x| x.as_str().map(|s| s.to_string()))
        .collect::<HashSet<String>>();
    let mut dlq = root
        .get("dlq")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let checkpoint = root.get("checkpoint").and_then(|v| v.as_u64()).unwrap_or(0);
    let out = if op == "record" {
        let msg = effective_stream_msg_id(&req);
        if dedup.contains(&msg) {
            json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"duplicate":true,"msg_id":msg})
        } else {
            dedup.insert(msg.clone());
            if let Some(err) = req.error {
                dlq.push(json!({"msg_id": msg, "error": err, "row": req.row, "ts": utc_now_iso()}));
            }
            json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"duplicate":false,"msg_id":msg})
        }
    } else if op == "checkpoint" {
        let cp = req.checkpoint.unwrap_or(checkpoint);
        root.insert("checkpoint".to_string(), json!(cp));
        json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"checkpoint":cp})
    } else if op == "flush_dlq" {
        let n = dlq.len();
        dlq.clear();
        json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"flushed":n})
    } else if op == "replay" {
        let limit = req.checkpoint.unwrap_or(100) as usize;
        let items = dlq.iter().take(limit).cloned().collect::<Vec<_>>();
        json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"replay_items":items,"replay_count":items.len()})
    } else if op == "consistency_check" {
        let dedup_unique = dedup.len();
        let dlq_count = dlq.len();
        let consistent = dedup_unique >= dlq_count;
        json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"consistent":consistent,"dedup_size":dedup_unique,"dlq_size":dlq_count})
    } else if op == "stats" {
        json!({"ok": true, "operator":"stream_reliability_v1","status":"done","run_id":req.run_id,"stream_key":key,"dedup_size":dedup.len(),"dlq_size":dlq.len(),"checkpoint":checkpoint})
    } else {
        return Err(format!("stream_reliability_v1 unsupported op: {op}"));
    };
    root.insert(
        "dedup".to_string(),
        Value::Array(dedup.into_iter().map(Value::String).collect()),
    );
    root.insert("dlq".to_string(), Value::Array(dlq));
    store.insert(key.clone(), Value::Object(root));
    save_kv_store(&stream_reliability_store_path(), &store)?;
    Ok(out)
}

pub(crate) fn run_lineage_provenance_v1(req: LineageProvenanceV1Req) -> Result<Value, String> {
    let lineage = run_lineage_v3(LineageV3Req {
        run_id: req.run_id.clone(),
        rules: req.rules,
        computed_fields_v3: req.computed_fields_v3,
        workflow_steps: req.workflow_steps,
        rows: req.rows,
    })?;
    let prov = run_provenance_sign_v1(ProvenanceSignReq {
        run_id: req.run_id.clone(),
        payload: req.payload.unwrap_or_else(|| lineage.clone()),
        prev_hash: req.prev_hash,
    })?;
    Ok(json!({
        "ok": true,
        "operator": "lineage_provenance_v1",
        "status": "done",
        "run_id": req.run_id,
        "lineage": lineage,
        "provenance": prov
    }))
}

pub(crate) fn run_contract_regression_v1(req: ContractRegressionV1Req) -> Result<Value, String> {
    let operators = req.operators.unwrap_or_else(|| {
        vec![
            "transform_rows_v3".to_string(),
            "finance_ratio_v1".to_string(),
            "anomaly_explain_v1".to_string(),
            "stream_window_v2".to_string(),
            "plugin_operator_v1".to_string(),
        ]
    });
    let mut cases = Vec::new();
    for op in operators {
        let sample = match op.as_str() {
            "finance_ratio_v1" => json!({"rows":[{"assets":100.0,"liabilities":50.0}]}),
            "anomaly_explain_v1" => {
                json!({"rows":[{"score":0.9}], "score_field":"score","threshold":0.8})
            }
            "stream_window_v2" => {
                json!({"stream_key":"s1","rows":[{"ts":"2025-01-01","value":1}],"event_time_field":"ts","window_ms":60000})
            }
            "plugin_operator_v1" => json!({"plugin":"demo","op":"run","payload":{}}),
            _ => json!({"rows":[{"id":"1","amount":"10"}]}),
        };
        let valid = io_contract_errors(&op, &sample, false).is_empty();
        cases.push(json!({"operator":op,"sample_input":sample,"expect_valid":valid}));
    }
    Ok(
        json!({"ok": true, "operator":"contract_regression_v1","status":"done","run_id":req.run_id,"cases":cases}),
    )
}

pub(crate) fn run_perf_baseline_v1(req: PerfBaselineV1Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let name = req
        .operator
        .unwrap_or_else(|| "transform_rows_v3".to_string());
    let mut store = load_kv_store(&perf_baseline_store_path());
    if op == "set" {
        let p95 = req.p95_ms.unwrap_or(500);
        store.insert(
            name.clone(),
            json!({"p95_ms": p95, "updated_at": utc_now_iso()}),
        );
        save_kv_store(&perf_baseline_store_path(), &store)?;
        return Ok(
            json!({"ok": true, "operator":"perf_baseline_v1","status":"done","run_id":req.run_id,"target_operator":name,"baseline_p95_ms":p95}),
        );
    }
    if op == "check" {
        let baseline = store
            .get(&name)
            .and_then(|v| v.get("p95_ms"))
            .and_then(|v| v.as_u64())
            .unwrap_or(500) as u128;
        let current = req.max_p95_ms.unwrap_or(baseline);
        let passed = current <= baseline;
        return Ok(
            json!({"ok": true, "operator":"perf_baseline_v1","status":"done","run_id":req.run_id,"target_operator":name,"baseline_p95_ms":baseline,"current_p95_ms":current,"passed":passed}),
        );
    }
    if op == "get" {
        return Ok(
            json!({"ok": true, "operator":"perf_baseline_v1","status":"done","run_id":req.run_id,"items":store}),
        );
    }
    Err(format!("perf_baseline_v1 unsupported op: {op}"))
}

#[cfg(test)]
mod tests {
    use super::effective_stream_msg_id;
    use crate::StreamReliabilityV1Req;

    fn blank_req() -> StreamReliabilityV1Req {
        StreamReliabilityV1Req {
            run_id: None,
            op: "record".to_string(),
            stream_key: "s1".to_string(),
            msg_id: Some("  ".to_string()),
            row: None,
            error: None,
            checkpoint: None,
        }
    }

    #[test]
    fn generated_stream_msg_ids_are_unique() {
        let first = effective_stream_msg_id(&blank_req());
        let second = effective_stream_msg_id(&blank_req());
        assert_ne!(first, second);
    }
}
