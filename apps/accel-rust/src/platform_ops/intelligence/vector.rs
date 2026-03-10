use super::*;

pub(crate) fn tokenize_text(s: &str) -> Vec<String> {
    s.to_lowercase()
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
        .map(|x| x.to_string())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>()
}

pub(crate) fn term_freq(tokens: &[String]) -> HashMap<String, f64> {
    let mut m = HashMap::<String, f64>::new();
    for t in tokens {
        *m.entry(t.clone()).or_insert(0.0) += 1.0;
    }
    let n = tokens.len().max(1) as f64;
    for v in m.values_mut() {
        *v /= n;
    }
    m
}

pub(crate) fn cosine_sparse(a: &HashMap<String, f64>, b: &HashMap<String, f64>) -> f64 {
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for v in a.values() {
        na += v * v;
    }
    for v in b.values() {
        nb += v * v;
    }
    for (k, va) in a {
        if let Some(vb) = b.get(k) {
            dot += va * vb;
        }
    }
    if na <= 0.0 || nb <= 0.0 {
        0.0
    } else {
        dot / (na.sqrt() * nb.sqrt())
    }
}

pub(crate) fn run_vector_index_build_v1(req: VectorIndexBuildReq) -> Result<Value, String> {
    let mut docs = Vec::new();
    for r in req.rows {
        let Some(o) = r.as_object() else { continue };
        let id = value_to_string_or_null(o.get(&req.id_field));
        let text = value_to_string_or_null(o.get(&req.text_field));
        if id.trim().is_empty() || text.trim().is_empty() {
            continue;
        }
        let tf = term_freq(&tokenize_text(&text));
        docs.push(json!({"id": id, "text": text, "tf": tf}));
    }
    let mut store = load_kv_store(&vector_index_store_path());
    store.insert(
        "default".to_string(),
        json!({"updated_at": utc_now_iso(), "size": docs.len(), "docs": docs}),
    );
    save_kv_store(&vector_index_store_path(), &store)?;
    Ok(
        json!({"ok": true, "operator": "vector_index_v1_build", "status": "done", "run_id": req.run_id, "size": docs.len()}),
    )
}

pub(crate) fn run_vector_index_search_v1(req: VectorIndexSearchReq) -> Result<Value, String> {
    let top_k = req.top_k.unwrap_or(5).clamp(1, 100);
    let store = load_kv_store(&vector_index_store_path());
    let docs = store
        .get("default")
        .and_then(|v| v.get("docs"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let qtf = term_freq(&tokenize_text(&req.query));
    let mut scored = Vec::new();
    for d in docs {
        let id = d.get("id").cloned().unwrap_or(Value::Null);
        let text = d.get("text").cloned().unwrap_or(Value::Null);
        let tfm = d
            .get("tf")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_f64().map(|x| (k.clone(), x)))
                    .collect::<HashMap<String, f64>>()
            })
            .unwrap_or_default();
        let score = cosine_sparse(&qtf, &tfm);
        scored.push(json!({"id": id, "text": text, "score": score}));
    }
    scored.sort_by(|a, b| {
        let av = a.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let bv = b.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        bv.partial_cmp(&av).unwrap_or(Ordering::Equal)
    });
    scored.truncate(top_k);
    Ok(
        json!({"ok": true, "operator": "vector_index_v1_search", "status": "done", "run_id": req.run_id, "hits": scored}),
    )
}
