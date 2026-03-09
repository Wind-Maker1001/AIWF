use crate::{
    analysis_ops::parse_time_order_key,
    api_types::{
        AnomalyExplainReq, EvidenceRankReq, FactCrosscheckReq, FinanceRatioReq, ProvenanceSignReq,
        TemplateBindReq, TimeSeriesForecastReq, VectorIndexBuildReq, VectorIndexSearchReq,
    },
    platform_ops::storage::{load_kv_store, save_kv_store, vector_index_store_path},
    transform_support::{
        unix_now_sec, utc_now_iso, value_to_f64, value_to_string, value_to_string_or_null,
    },
};
use regex::Regex;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

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

pub(crate) fn run_evidence_rank_v1(req: EvidenceRankReq) -> Result<Value, String> {
    let t_field = req.time_field.unwrap_or_else(|| "time".to_string());
    let s_field = req
        .source_field
        .unwrap_or_else(|| "source_score".to_string());
    let r_field = req
        .relevance_field
        .unwrap_or_else(|| "relevance".to_string());
    let c_field = req
        .consistency_field
        .unwrap_or_else(|| "consistency".to_string());
    let now = unix_now_sec() as f64;
    let mut out = Vec::new();
    for r in req.rows {
        let Some(mut o) = r.as_object().cloned() else {
            continue;
        };
        let rel = o.get(&r_field).and_then(value_to_f64).unwrap_or(0.0);
        let src = o.get(&s_field).and_then(value_to_f64).unwrap_or(0.0);
        let cons = o.get(&c_field).and_then(value_to_f64).unwrap_or(0.0);
        let time_score = o
            .get(&t_field)
            .and_then(|v| v.as_str())
            .map(parse_time_order_key)
            .map(|ts| {
                if ts <= 0 {
                    0.0
                } else {
                    let age_days = ((now - ts as f64) / 86400.0).max(0.0);
                    (1.0 / (1.0 + age_days / 30.0)).clamp(0.0, 1.0)
                }
            })
            .unwrap_or(0.5);
        let score = 0.45 * rel + 0.25 * src + 0.20 * cons + 0.10 * time_score;
        o.insert("evidence_score".to_string(), json!(score));
        out.push(Value::Object(o));
    }
    out.sort_by(|a, b| {
        let av = a
            .get("evidence_score")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let bv = b
            .get("evidence_score")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        bv.partial_cmp(&av).unwrap_or(Ordering::Equal)
    });
    Ok(
        json!({"ok": true, "operator": "evidence_rank_v1", "status": "done", "run_id": req.run_id, "rows": out}),
    )
}

pub(crate) fn canonical_claim(s: &str) -> String {
    tokenize_text(s).join(" ")
}

pub(crate) fn run_fact_crosscheck_v1(req: FactCrosscheckReq) -> Result<Value, String> {
    let src_field = req.source_field.unwrap_or_else(|| "source".to_string());
    let mut groups: HashMap<String, HashSet<String>> = HashMap::new();
    for r in req.rows {
        let Some(o) = r.as_object() else { continue };
        let claim = canonical_claim(&value_to_string_or_null(o.get(&req.claim_field)));
        if claim.is_empty() {
            continue;
        }
        let src = value_to_string_or_null(o.get(&src_field));
        groups.entry(claim).or_default().insert(src);
    }
    let mut out = Vec::new();
    for (claim, srcs) in groups {
        let status = if srcs.len() >= 2 {
            "supported"
        } else {
            "unverified"
        };
        out.push(
            json!({"claim": claim, "status": status, "source_count": srcs.len(), "sources": srcs}),
        );
    }
    Ok(
        json!({"ok": true, "operator": "fact_crosscheck_v1", "status": "done", "run_id": req.run_id, "results": out}),
    )
}

pub(crate) fn run_timeseries_forecast_v1(req: TimeSeriesForecastReq) -> Result<Value, String> {
    let horizon = req.horizon.unwrap_or(3).clamp(1, 60);
    let method = req
        .method
        .unwrap_or_else(|| "naive_drift".to_string())
        .to_lowercase();
    let mut rows = req
        .rows
        .into_iter()
        .filter_map(|r| r.as_object().cloned())
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        parse_time_order_key(&value_to_string_or_null(a.get(&req.time_field))).cmp(
            &parse_time_order_key(&value_to_string_or_null(b.get(&req.time_field))),
        )
    });
    let vals = rows
        .iter()
        .filter_map(|o| o.get(&req.value_field).and_then(value_to_f64))
        .collect::<Vec<_>>();
    if vals.is_empty() {
        return Err("timeseries_forecast_v1 requires non-empty numeric series".to_string());
    }
    let first = vals[0];
    let last = *vals.last().unwrap_or(&first);
    let drift = if vals.len() > 1 {
        (last - first) / (vals.len() as f64 - 1.0)
    } else {
        0.0
    };
    let mut forecast = Vec::new();
    for h in 1..=horizon {
        let pred = if method == "naive_last" {
            last
        } else {
            last + drift * h as f64
        };
        forecast.push(json!({"step": h, "prediction": pred}));
    }
    Ok(
        json!({"ok": true, "operator": "timeseries_forecast_v1", "status": "done", "run_id": req.run_id, "method": method, "forecast": forecast}),
    )
}

pub(crate) fn run_finance_ratio_v1(req: FinanceRatioReq) -> Result<Value, String> {
    let mut out = Vec::new();
    for r in req.rows {
        let Some(mut o) = r.as_object().cloned() else {
            continue;
        };
        let ca = o
            .get("current_assets")
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        let cl = o
            .get("current_liabilities")
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        let debt = o.get("total_debt").and_then(value_to_f64).unwrap_or(0.0);
        let equity = o.get("total_equity").and_then(value_to_f64).unwrap_or(0.0);
        let rev = o.get("revenue").and_then(value_to_f64).unwrap_or(0.0);
        let ni = o.get("net_income").and_then(value_to_f64).unwrap_or(0.0);
        let ocf = o
            .get("operating_cash_flow")
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        let qr = if cl.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(ca / cl)
        };
        let d2e = if equity.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(debt / equity)
        };
        let nm = if rev.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(ni / rev)
        };
        let ocf_margin = if rev.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(ocf / rev)
        };
        o.insert("ratio_current".to_string(), qr);
        o.insert("ratio_debt_to_equity".to_string(), d2e);
        o.insert("ratio_net_margin".to_string(), nm);
        o.insert("ratio_ocf_margin".to_string(), ocf_margin);
        out.push(Value::Object(o));
    }
    Ok(
        json!({"ok": true, "operator": "finance_ratio_v1", "status": "done", "run_id": req.run_id, "rows": out}),
    )
}

pub(crate) fn run_anomaly_explain_v1(req: AnomalyExplainReq) -> Result<Value, String> {
    let th = req.threshold.unwrap_or(0.8);
    let mut anomalies = Vec::new();
    for (idx, r) in req.rows.iter().enumerate() {
        let Some(o) = r.as_object() else { continue };
        let score = o
            .get(&req.score_field)
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        if score < th {
            continue;
        }
        let mut contrib = Vec::new();
        for (k, v) in o {
            if k == &req.score_field {
                continue;
            }
            if let Some(n) = value_to_f64(v) {
                contrib.push((k.clone(), n.abs()));
            }
        }
        contrib.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        anomalies.push(json!({
            "row_index": idx,
            "score": score,
            "top_contributors": contrib.into_iter().take(3).map(|(k,v)| json!({"field":k,"importance":v})).collect::<Vec<_>>()
        }));
    }
    Ok(
        json!({"ok": true, "operator": "anomaly_explain_v1", "status": "done", "run_id": req.run_id, "anomalies": anomalies}),
    )
}

pub(crate) fn template_lookup(data: &Value, key: &str) -> Option<Value> {
    let mut cur = data;
    for p in key.split('.') {
        if let Some(o) = cur.as_object() {
            cur = o.get(p)?;
        } else {
            return None;
        }
    }
    Some(cur.clone())
}

pub(crate) fn run_template_bind_v1(req: TemplateBindReq) -> Result<Value, String> {
    let re = Regex::new(r"\{\{\s*([A-Za-z0-9_.-]+)\s*\}\}").map_err(|e| e.to_string())?;
    let mut out = req.template_text.clone();
    let mut binds = 0usize;
    for cap in re.captures_iter(&req.template_text) {
        let all = cap.get(0).map(|m| m.as_str()).unwrap_or("");
        let key = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        if all.is_empty() || key.is_empty() {
            continue;
        }
        if let Some(v) = template_lookup(&req.data, key) {
            out = out.replace(all, &value_to_string(&v));
            binds += 1;
        }
    }
    Ok(
        json!({"ok": true, "operator": "template_bind_v1", "status": "done", "run_id": req.run_id, "bound_text": out, "bind_count": binds}),
    )
}

pub(crate) fn run_provenance_sign_v1(req: ProvenanceSignReq) -> Result<Value, String> {
    let payload_text = serde_json::to_string(&req.payload).map_err(|e| e.to_string())?;
    let prev = req.prev_hash.unwrap_or_default();
    let ts = utc_now_iso();
    let mut h = Sha256::new();
    h.update(format!("{prev}|{ts}|{payload_text}").as_bytes());
    let hash = format!("{:x}", h.finalize());
    Ok(json!({
        "ok": true,
        "operator": "provenance_sign_v1",
        "status": "done",
        "run_id": req.run_id,
        "record": {"timestamp": ts, "prev_hash": prev, "hash": hash}
    }))
}
