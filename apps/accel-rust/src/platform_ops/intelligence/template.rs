use super::*;

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
