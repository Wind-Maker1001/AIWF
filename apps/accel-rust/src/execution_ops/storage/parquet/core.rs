use super::helpers::{
    apply_schema_columns, collect_parquet_paths, infer_rows_schema, partition_predicate_match,
    schema_compatible, schema_file_path,
};
use super::*;

pub(crate) fn run_parquet_io_v2(req: ParquetIoV2Req) -> Result<Value, String> {
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
