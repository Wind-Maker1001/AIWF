use super::vector::tokenize_text;
use super::*;

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
