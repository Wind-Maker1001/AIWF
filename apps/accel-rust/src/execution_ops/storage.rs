use crate::{
    api_types::{ParquetIoV2Req, StreamStateV2Req, UdfWasmReq, UdfWasmV2Req},
    platform_ops::{
        ensure_stream_state_sqlite, load_kv_store, save_kv_store, stream_state_sqlite_path,
        stream_state_store_path,
    },
    row_io::{
        load_parquet_rows, parquet_compression_from_name,
        save_rows_parquet_payload_with_compression, save_rows_parquet_typed_with_compression,
    },
    transform_support::{utc_now_iso, value_to_string, value_to_string_or_null},
    wasm_ops::run_udf_wasm_v1,
};
use serde_json::{Map, Value, json};
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) fn run_parquet_io_v2(req: ParquetIoV2Req) -> Result<Value, String> {
    fn collect_parquet_paths(
        base: &Path,
        recursive: bool,
        out: &mut Vec<PathBuf>,
    ) -> Result<(), String> {
        let rd = fs::read_dir(base).map_err(|e| format!("read parquet dir: {e}"))?;
        for ent in rd {
            let ent = ent.map_err(|e| format!("read parquet dir entry: {e}"))?;
            let p = ent.path();
            if p.is_dir() && recursive {
                collect_parquet_paths(&p, true, out)?;
            } else if p
                .extension()
                .and_then(|x| x.to_str())
                .map(|x| x.eq_ignore_ascii_case("parquet"))
                .unwrap_or(false)
            {
                out.push(p);
            }
        }
        Ok(())
    }
    fn partition_predicate_match(path: &Path, field: Option<&str>, eqv: Option<&Value>) -> bool {
        let Some(field) = field else { return true };
        let Some(eqv) = eqv else { return true };
        let target = value_to_string(eqv);
        for seg in path.components() {
            let s = seg.as_os_str().to_string_lossy();
            if let Some((k, v)) = s.split_once('=')
                && k == field
            {
                return v == target;
            }
        }
        true
    }
    fn schema_file_path(base_path: &str, partitioned: bool) -> PathBuf {
        let p = Path::new(base_path);
        if partitioned || p.is_dir() || !base_path.to_lowercase().ends_with(".parquet") {
            return p.join("_schema.json");
        }
        PathBuf::from(format!("{base_path}.schema.json"))
    }
    fn json_type_name(v: &Value) -> String {
        if v.is_boolean() {
            "bool".to_string()
        } else if v.is_i64() || v.is_u64() {
            "int".to_string()
        } else if v.is_f64() {
            "float".to_string()
        } else if v.is_null() {
            "null".to_string()
        } else {
            "string".to_string()
        }
    }
    fn infer_rows_schema(rows: &[Value]) -> Map<String, Value> {
        let mut out = Map::new();
        for r in rows {
            let Some(o) = r.as_object() else {
                continue;
            };
            for (k, v) in o {
                let t = json_type_name(v);
                let prev = out.get(k).and_then(|x| x.as_str()).unwrap_or("");
                let merged = if prev.is_empty() || prev == t {
                    t
                } else if (prev == "int" && t == "float") || (prev == "float" && t == "int") {
                    "float".to_string()
                } else {
                    "string".to_string()
                };
                out.insert(k.clone(), json!(merged));
            }
        }
        out
    }
    fn schema_compatible(
        old_s: &Map<String, Value>,
        new_s: &Map<String, Value>,
        mode: &str,
    ) -> bool {
        let m = mode.trim().to_lowercase();
        if m == "strict" && old_s.len() != new_s.len() {
            return false;
        }
        for (k, ov) in old_s {
            let Some(nv) = new_s.get(k) else {
                return false;
            };
            let o = ov.as_str().unwrap_or("");
            let n = nv.as_str().unwrap_or("");
            if o == n {
                continue;
            }
            if m == "widen" && ((o == "int" && n == "float") || n == "string") {
                continue;
            }
            return false;
        }
        true
    }
    fn apply_schema_columns(rows: &mut [Value], schema: &Map<String, Value>) {
        for r in rows {
            let Some(o) = r.as_object_mut() else {
                continue;
            };
            for k in schema.keys() {
                if !o.contains_key(k) {
                    o.insert(k.clone(), Value::Null);
                }
            }
        }
    }
    let op = req.op.trim().to_lowercase();
    match op.as_str() {
        "write" | "save" => {
            let mut rows = req.rows.unwrap_or_default();
            let mode = req
                .parquet_mode
                .unwrap_or_else(|| "typed".to_string())
                .to_lowercase();
            let compression = req
                .compression
                .unwrap_or_else(|| "snappy".to_string())
                .to_lowercase();
            let comp = parquet_compression_from_name(&compression)?;
            let partition_by = req.partition_by.unwrap_or_default();
            let schema_mode = req
                .schema_mode
                .unwrap_or_else(|| "additive".to_string())
                .to_lowercase();
            let schema_path = schema_file_path(&req.path, !partition_by.is_empty());
            let new_schema = infer_rows_schema(&rows);
            let old_schema = if schema_path.exists() {
                let txt = fs::read_to_string(&schema_path).unwrap_or_default();
                serde_json::from_str::<Map<String, Value>>(&txt).unwrap_or_default()
            } else {
                Map::new()
            };
            if !old_schema.is_empty() && !schema_compatible(&old_schema, &new_schema, &schema_mode)
            {
                return Err(format!(
                    "parquet_io_v2 schema evolution incompatible under mode={schema_mode}"
                ));
            }
            if !old_schema.is_empty() {
                apply_schema_columns(&mut rows, &old_schema);
            }
            if let Some(parent) = schema_path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let final_schema = if old_schema.is_empty() {
                new_schema.clone()
            } else {
                let mut m = old_schema.clone();
                for (k, v) in new_schema {
                    m.insert(k, v);
                }
                m
            };
            let _ = fs::write(
                &schema_path,
                serde_json::to_string_pretty(&final_schema).unwrap_or_else(|_| "{}".to_string()),
            );
            if partition_by.is_empty() {
                if mode == "payload" {
                    save_rows_parquet_payload_with_compression(&req.path, &rows, comp)?;
                } else {
                    save_rows_parquet_typed_with_compression(&req.path, &rows, comp)?;
                }
                return Ok(
                    json!({"ok": true, "operator": "parquet_io_v2", "status": "done", "run_id": req.run_id, "op": op, "path": req.path, "written_rows": rows.len(), "mode": mode, "compression": compression, "schema_mode": schema_mode, "schema_path": schema_path.to_string_lossy().to_string()}),
                );
            }
            let mut parts = HashMap::<String, Vec<Value>>::new();
            for r in rows {
                let Some(obj) = r.as_object() else {
                    continue;
                };
                let mut key = Vec::new();
                for p in &partition_by {
                    let v = value_to_string_or_null(obj.get(p));
                    key.push(format!("{p}={v}"));
                }
                parts
                    .entry(key.join(std::path::MAIN_SEPARATOR_STR))
                    .or_default()
                    .push(Value::Object(obj.clone()));
            }
            let mut written = 0usize;
            let mut files = Vec::new();
            for (part, rows) in parts {
                let path = Path::new(&req.path).join(part).join("part-00001.parquet");
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)
                        .map_err(|e| format!("create parquet partition dir: {e}"))?;
                }
                let path_str = path.to_string_lossy().to_string();
                if mode == "payload" {
                    save_rows_parquet_payload_with_compression(&path_str, &rows, comp)?;
                } else {
                    save_rows_parquet_typed_with_compression(&path_str, &rows, comp)?;
                }
                written += rows.len();
                files.push(path_str);
            }
            Ok(
                json!({"ok": true, "operator": "parquet_io_v2", "status": "done", "run_id": req.run_id, "op": op, "path": req.path, "written_rows": written, "mode": mode, "compression": compression, "partition_by": partition_by, "files": files, "schema_mode": schema_mode, "schema_path": schema_path.to_string_lossy().to_string()}),
            )
        }
        "read" | "load" => {
            let base = Path::new(&req.path);
            let mut rows = if base.is_dir() {
                let mut files = Vec::new();
                collect_parquet_paths(base, req.recursive.unwrap_or(true), &mut files)?;
                let mut acc = Vec::new();
                let mut scanned_files = 0usize;
                let mut pruned_files = 0usize;
                for f in files {
                    if !partition_predicate_match(
                        &f,
                        req.predicate_field.as_deref(),
                        req.predicate_eq.as_ref(),
                    ) {
                        pruned_files += 1;
                        continue;
                    }
                    scanned_files += 1;
                    let path = f.to_string_lossy().to_string();
                    let mut part = load_parquet_rows(&path, req.limit.unwrap_or(10000))?;
                    acc.append(&mut part);
                }
                if let Some(obj) = acc.first_mut().and_then(|v| v.as_object_mut()) {
                    obj.insert("__parquet_scanned_files".to_string(), json!(scanned_files));
                    obj.insert("__parquet_pruned_files".to_string(), json!(pruned_files));
                }
                acc
            } else {
                load_parquet_rows(&req.path, req.limit.unwrap_or(10000))?
            };
            if let (Some(field), Some(eqv)) =
                (req.predicate_field.as_ref(), req.predicate_eq.as_ref())
            {
                let eq = value_to_string(eqv);
                rows.retain(|r| {
                    r.as_object()
                        .map(|o| value_to_string_or_null(o.get(field)) == eq)
                        .unwrap_or(false)
                });
            }
            if let Some(cols) = req.columns.as_ref() {
                rows = rows
                    .into_iter()
                    .filter_map(|r| {
                        let o = r.as_object()?;
                        let mut m = Map::new();
                        for c in cols {
                            if let Some(v) = o.get(c) {
                                m.insert(c.clone(), v.clone());
                            }
                        }
                        Some(Value::Object(m))
                    })
                    .collect();
            }
            let mut scanned_files = Value::Null;
            let mut pruned_files = Value::Null;
            if let Some(first) = rows.first_mut().and_then(|v| v.as_object_mut()) {
                scanned_files = first
                    .remove("__parquet_scanned_files")
                    .unwrap_or(Value::Null);
                pruned_files = first
                    .remove("__parquet_pruned_files")
                    .unwrap_or(Value::Null);
            }
            Ok(
                json!({"ok": true, "operator": "parquet_io_v2", "status": "done", "run_id": req.run_id, "op": op, "path": req.path, "rows": rows, "recursive": req.recursive.unwrap_or(true), "partition_pruning": {"scanned_files": scanned_files, "pruned_files": pruned_files}}),
            )
        }
        "inspect" | "inspect_schema" => {
            let md =
                fs::metadata(&req.path).map_err(|e| format!("parquet inspect metadata: {e}"))?;
            let sample = load_parquet_rows(&req.path, req.limit.unwrap_or(20))?;
            let columns = sample
                .first()
                .and_then(|v| v.as_object())
                .map(|o| o.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            let schema_hint = if let Some(first) = sample.first().and_then(|v| v.as_object()) {
                let mut m = Map::new();
                for (k, v) in first {
                    let t = if v.is_number() {
                        "number"
                    } else if v.is_boolean() {
                        "bool"
                    } else if v.is_null() {
                        "null"
                    } else {
                        "string"
                    };
                    m.insert(k.clone(), json!(t));
                }
                Value::Object(m)
            } else {
                json!({})
            };
            Ok(json!({
                "ok": true,
                "operator": "parquet_io_v2",
                "status": "done",
                "run_id": req.run_id,
                "op": op,
                "path": req.path,
                "bytes": md.len(),
                "sample_rows": sample.len(),
                "columns": columns,
                "schema_hint": schema_hint
            }))
        }
        "merge_small" => {
            let base = Path::new(&req.path);
            if !base.is_dir() {
                return Err("parquet_io_v2 merge_small requires directory path".to_string());
            }
            let mut files = Vec::new();
            collect_parquet_paths(base, true, &mut files)?;
            if files.is_empty() {
                return Ok(
                    json!({"ok": true, "operator":"parquet_io_v2", "status":"done", "run_id": req.run_id, "op": op, "merged": 0, "path": req.path}),
                );
            }
            let mut all_rows = Vec::new();
            for f in &files {
                let path = f.to_string_lossy().to_string();
                let mut part = load_parquet_rows(&path, 1_000_000)?;
                all_rows.append(&mut part);
            }
            let merged_path = base.join("_merged.parquet");
            save_rows_parquet_typed_with_compression(
                &merged_path.to_string_lossy(),
                &all_rows,
                parquet_compression_from_name(req.compression.as_deref().unwrap_or("snappy"))?,
            )?;
            Ok(json!({
                "ok": true,
                "operator": "parquet_io_v2",
                "status": "done",
                "run_id": req.run_id,
                "op": op,
                "input_files": files.len(),
                "merged_rows": all_rows.len(),
                "merged_path": merged_path.to_string_lossy().to_string()
            }))
        }
        _ => Err(format!("parquet_io_v2 unsupported op: {}", req.op)),
    }
}

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

pub(crate) fn run_udf_wasm_v2(req: UdfWasmV2Req) -> Result<Value, String> {
    if let Ok(token) = env::var("AIWF_UDF_V2_TOKEN")
        && !token.trim().is_empty()
    {
        let got = req.signed_token.clone().unwrap_or_default();
        if got != token {
            return Err("udf_wasm_v2 invalid signed_token".to_string());
        }
    }
    let op = req.op.clone().unwrap_or_else(|| "identity".to_string());
    if let Some(allow) = req.allowed_ops.as_ref()
        && !allow.iter().any(|x| x.eq_ignore_ascii_case(&op))
    {
        return Err(format!("udf_wasm_v2 op not allowed: {op}"));
    }
    let mut out = run_udf_wasm_v1(UdfWasmReq {
        run_id: req.run_id.clone(),
        rows: req.rows,
        field: req.field,
        output_field: req.output_field,
        op: Some(op),
        wasm_base64: req.wasm_base64,
    })?;
    let max_bytes = req.max_output_bytes.unwrap_or(1_000_000).max(4096);
    let mut truncated = 0usize;
    while serde_json::to_vec(&out).map(|v| v.len()).unwrap_or(0) > max_bytes {
        let Some(rows) = out.get_mut("rows").and_then(|v| v.as_array_mut()) else {
            break;
        };
        if rows.pop().is_none() {
            break;
        }
        truncated += 1;
    }
    if let Some(stats) = out.get_mut("stats").and_then(|v| v.as_object_mut()) {
        stats.insert("max_output_bytes".to_string(), json!(max_bytes));
        stats.insert("truncated_rows".to_string(), json!(truncated));
        stats.insert("udf_v2".to_string(), json!(true));
    }
    if let Some(obj) = out.as_object_mut() {
        obj.insert("operator".to_string(), json!("udf_wasm_v2"));
    }
    Ok(out)
}
