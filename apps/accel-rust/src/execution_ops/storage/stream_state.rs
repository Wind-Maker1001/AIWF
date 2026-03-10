use super::*;

pub(crate) fn run_stream_state_v2(req: StreamStateV2Req) -> Result<Value, String> {
    let op = req.op.trim().to_lowercase();
    let backend = req
        .backend
        .as_deref()
        .unwrap_or("file")
        .trim()
        .to_lowercase();
    let event_late = if let (Some(ts), Some(max_late)) = (req.event_ts_ms, req.max_late_ms) {
        let now_ms = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()) as i64;
        now_ms.saturating_sub(ts) > max_late as i64
    } else {
        false
    };
    if event_late && (op == "save" || op == "upsert" || op == "checkpoint") {
        return Ok(json!({
            "ok": true,
            "operator": "stream_state_v2",
            "status": "done",
            "run_id": req.run_id,
            "op": op,
            "stream_key": req.stream_key,
            "late_dropped": true
        }));
    }
    if backend == "sqlite" {
        let conn = ensure_stream_state_sqlite(&stream_state_sqlite_path(&req))?;
        match op.as_str() {
            "load" | "get" | "restore" => {
                let mut stmt = conn
                    .prepare(
                        "SELECT state_json, offset_val, version, updated_at FROM stream_state WHERE stream_key=?1",
                    )
                    .map_err(|e| format!("stream sqlite prepare load: {e}"))?;
                let mut rows = stmt
                    .query([req.stream_key.as_str()])
                    .map_err(|e| format!("stream sqlite load query: {e}"))?;
                if let Some(r) = rows
                    .next()
                    .map_err(|e| format!("stream sqlite load next: {e}"))?
                {
                    let state_json: String = r
                        .get(0)
                        .map_err(|e| format!("stream sqlite get state: {e}"))?;
                    let offset: i64 = r
                        .get(1)
                        .map_err(|e| format!("stream sqlite get offset: {e}"))?;
                    let version: i64 = r
                        .get(2)
                        .map_err(|e| format!("stream sqlite get version: {e}"))?;
                    let updated_at: String = r
                        .get(3)
                        .map_err(|e| format!("stream sqlite get updated_at: {e}"))?;
                    let state = serde_json::from_str::<Value>(&state_json).unwrap_or(Value::Null);
                    return Ok(
                        json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"sqlite", "op": op, "stream_key": req.stream_key, "value": {"state": state, "offset": offset.max(0) as u64, "version": version.max(0) as u64, "updated_at": updated_at}}),
                    );
                }
                return Ok(
                    json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"sqlite", "op": op, "stream_key": req.stream_key, "value": Value::Null}),
                );
            }
            "delete" => {
                let n = conn
                    .execute(
                        "DELETE FROM stream_state WHERE stream_key=?1",
                        [req.stream_key.as_str()],
                    )
                    .map_err(|e| format!("stream sqlite delete: {e}"))?;
                return Ok(
                    json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"sqlite", "op": op, "stream_key": req.stream_key, "deleted": n > 0}),
                );
            }
            "save" | "upsert" | "checkpoint" => {
                let mut stmt = conn
                    .prepare("SELECT version FROM stream_state WHERE stream_key=?1")
                    .map_err(|e| format!("stream sqlite prepare version: {e}"))?;
                let mut rows = stmt
                    .query([req.stream_key.as_str()])
                    .map_err(|e| format!("stream sqlite query version: {e}"))?;
                let cur_ver = if let Some(r) = rows
                    .next()
                    .map_err(|e| format!("stream sqlite next version: {e}"))?
                {
                    let v: i64 = r
                        .get(0)
                        .map_err(|e| format!("stream sqlite get version: {e}"))?;
                    v.max(0) as u64
                } else {
                    0
                };
                if let Some(exp) = req.expected_version
                    && exp != cur_ver
                {
                    return Err(format!(
                        "stream_state_v2 version mismatch: expected={}, current={}",
                        exp, cur_ver
                    ));
                }
                let next_ver = req.checkpoint_version.unwrap_or(cur_ver + 1);
                let state = req.state.unwrap_or(Value::Null);
                let state_json = serde_json::to_string(&state)
                    .map_err(|e| format!("stream sqlite encode state: {e}"))?;
                conn.execute(
                    "INSERT INTO stream_state(stream_key, state_json, offset_val, version, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(stream_key) DO UPDATE SET
                       state_json=excluded.state_json,
                       offset_val=excluded.offset_val,
                       version=excluded.version,
                       updated_at=excluded.updated_at",
                    (
                        req.stream_key.as_str(),
                        state_json.as_str(),
                        req.offset.unwrap_or(0) as i64,
                        next_ver as i64,
                        utc_now_iso(),
                    ),
                )
                .map_err(|e| format!("stream sqlite upsert: {e}"))?;
                return Ok(
                    json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"sqlite", "op": op, "stream_key": req.stream_key, "version": next_ver}),
                );
            }
            "list" => {
                let mut stmt = conn
                    .prepare("SELECT stream_key, version, updated_at FROM stream_state ORDER BY updated_at DESC LIMIT 200")
                    .map_err(|e| format!("stream sqlite prepare list: {e}"))?;
                let mut rows = stmt
                    .query([])
                    .map_err(|e| format!("stream sqlite list query: {e}"))?;
                let mut items = Vec::new();
                while let Some(r) = rows
                    .next()
                    .map_err(|e| format!("stream sqlite list next: {e}"))?
                {
                    let stream_key: String = r
                        .get(0)
                        .map_err(|e| format!("stream sqlite list key: {e}"))?;
                    let version: i64 = r
                        .get(1)
                        .map_err(|e| format!("stream sqlite list version: {e}"))?;
                    let updated_at: String = r
                        .get(2)
                        .map_err(|e| format!("stream sqlite list updated_at: {e}"))?;
                    items.push(json!({"stream_key": stream_key, "version": version.max(0) as u64, "updated_at": updated_at}));
                }
                return Ok(
                    json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"sqlite", "op": op, "items": items}),
                );
            }
            _ => return Err(format!("stream_state_v2 unsupported op: {}", req.op)),
        }
    }
    let mut store = load_kv_store(&stream_state_store_path());
    match op.as_str() {
        "load" | "get" | "restore" => {
            let v = store.get(&req.stream_key).cloned().unwrap_or(Value::Null);
            Ok(
                json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"file", "op": op, "stream_key": req.stream_key, "value": v}),
            )
        }
        "delete" => {
            let existed = store.remove(&req.stream_key).is_some();
            save_kv_store(&stream_state_store_path(), &store)?;
            Ok(
                json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"file", "op": op, "stream_key": req.stream_key, "deleted": existed}),
            )
        }
        "save" | "upsert" | "checkpoint" => {
            let cur_ver = store
                .get(&req.stream_key)
                .and_then(|v| v.get("version"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if let Some(exp) = req.expected_version
                && exp != cur_ver
            {
                return Err(format!(
                    "stream_state_v2 version mismatch: expected={}, current={}",
                    exp, cur_ver
                ));
            }
            let next_ver = req.checkpoint_version.unwrap_or(cur_ver + 1);
            store.insert(
                req.stream_key.clone(),
                json!({
                    "state": req.state.unwrap_or(Value::Null),
                    "offset": req.offset.unwrap_or(0),
                    "version": next_ver,
                    "updated_at": utc_now_iso()
                }),
            );
            save_kv_store(&stream_state_store_path(), &store)?;
            Ok(
                json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"file", "op": op, "stream_key": req.stream_key, "version": next_ver}),
            )
        }
        "list" => {
            let items = store
                .iter()
                .map(|(k, v)| {
                    json!({
                        "stream_key": k,
                        "version": v.get("version").and_then(|x| x.as_u64()).unwrap_or(0),
                        "updated_at": v.get("updated_at").cloned().unwrap_or(Value::Null)
                    })
                })
                .collect::<Vec<_>>();
            Ok(
                json!({"ok": true, "operator": "stream_state_v2", "status": "done", "run_id": req.run_id, "backend":"file", "op": op, "items": items}),
            )
        }
        _ => Err(format!("stream_state_v2 unsupported op: {}", req.op)),
    }
}
