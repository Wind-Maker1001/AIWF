use super::*;

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
