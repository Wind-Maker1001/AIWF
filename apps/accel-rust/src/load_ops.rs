use crate::{
    api_types::{LoadRowsReq, LoadRowsResp, LoadRowsV2Req, LoadRowsV3Req},
    row_io::{
        load_csv_rows, load_jsonl_rows, load_parquet_rows, load_sqlite_rows, load_sqlserver_rows,
    },
};
use serde_json::{Value, json};
use std::{fs, thread, time::Duration};

pub(crate) fn run_load_rows_v1(req: LoadRowsReq) -> Result<LoadRowsResp, String> {
    let st = req.source_type.to_lowercase();
    let limit = req.limit.unwrap_or(10000).max(1);
    let rows = match st.as_str() {
        "jsonl" => load_jsonl_rows(&req.source, limit)?,
        "csv" => load_csv_rows(&req.source, limit)?,
        "sqlite" => load_sqlite_rows(
            &req.source,
            req.query.as_deref().unwrap_or("SELECT * FROM data"),
            limit,
        )?,
        "sqlserver" => load_sqlserver_rows(
            &req.source,
            req.query
                .as_deref()
                .unwrap_or("SELECT TOP 100 * FROM dbo.workflow_tasks"),
            limit,
        )?,
        "parquet" => load_parquet_rows(&req.source, limit)?,
        _ => return Err(format!("unsupported source_type: {}", req.source_type)),
    };
    Ok(LoadRowsResp {
        ok: true,
        operator: "load_rows_v1".to_string(),
        status: "done".to_string(),
        stats: json!({"source_type": st, "rows": rows.len()}),
        rows,
    })
}

pub(crate) fn run_load_rows_v2(req: LoadRowsV2Req) -> Result<LoadRowsResp, String> {
    let st = req.source_type.to_lowercase();
    let limit = req.limit.unwrap_or(10000).max(1);
    let rows = match st.as_str() {
        "jsonl" => load_jsonl_rows(&req.source, limit)?,
        "csv" => load_csv_rows(&req.source, limit)?,
        "sqlite" => load_sqlite_rows(
            &req.source,
            req.query.as_deref().unwrap_or("SELECT * FROM data"),
            limit,
        )?,
        "sqlserver" => load_sqlserver_rows(
            &req.source,
            req.query
                .as_deref()
                .unwrap_or("SELECT TOP 100 * FROM dbo.workflow_tasks"),
            limit,
        )?,
        "parquet" => load_parquet_rows(&req.source, limit)?,
        "txt" => {
            let txt = fs::read_to_string(&req.source).map_err(|e| format!("read txt: {e}"))?;
            txt.lines()
                .take(limit)
                .enumerate()
                .map(|(i, line)| json!({"line_no": i + 1, "text": line}))
                .collect::<Vec<_>>()
        }
        "pdf" | "docx" | "xlsx" | "image" => {
            let meta =
                fs::metadata(&req.source).map_err(|e| format!("read source metadata: {e}"))?;
            vec![json!({
                "source": req.source,
                "source_type": st,
                "size_bytes": meta.len(),
                "extract_status": "metadata_only",
                "hint": "use glue-python ingest for rich extraction"
            })]
        }
        _ => return Err(format!("unsupported source_type: {}", req.source_type)),
    };
    Ok(LoadRowsResp {
        ok: true,
        operator: "load_rows_v2".to_string(),
        status: "done".to_string(),
        stats: json!({"source_type": st, "rows": rows.len()}),
        rows,
    })
}

pub(crate) fn run_load_rows_v3(req: LoadRowsV3Req) -> Result<LoadRowsResp, String> {
    let max_retries = req.max_retries.unwrap_or(2).min(8);
    let backoff_ms = req.retry_backoff_ms.unwrap_or(150).clamp(10, 10_000);
    let mut last_err = None::<String>;
    for attempt in 0..=max_retries {
        let out = run_load_rows_v2(LoadRowsV2Req {
            source_type: req.source_type.clone(),
            source: req.source.clone(),
            query: req.query.clone(),
            limit: req.limit,
        });
        match out {
            Ok(mut resp) => {
                resp.operator = "load_rows_v3".to_string();
                let mut stats = resp.stats.as_object().cloned().unwrap_or_default();
                stats.insert("attempt".to_string(), json!(attempt + 1));
                stats.insert("max_retries".to_string(), json!(max_retries));
                stats.insert("resume_token".to_string(), json!(req.resume_token));
                stats.insert(
                    "connector_options".to_string(),
                    req.connector_options.clone().unwrap_or_else(|| json!({})),
                );
                resp.stats = Value::Object(stats);
                return Ok(resp);
            }
            Err(e) => {
                last_err = Some(e);
                if attempt < max_retries {
                    thread::sleep(Duration::from_millis(backoff_ms * (attempt as u64 + 1)));
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| "load_rows_v3 failed".to_string()))
}
