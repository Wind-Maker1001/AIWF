use super::*;

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
