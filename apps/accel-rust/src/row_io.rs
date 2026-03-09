use crate::transform_support::{
    validate_readonly_query, validate_sql_identifier, value_to_f64, value_to_i64,
    value_to_string_or_null,
};
use ::parquet::{
    basic::{Compression, LogicalType, Repetition, Type as PhysicalType},
    column::reader::ColumnReader,
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::reader::{FileReader, SerializedFileReader},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::Type,
};
use accel_rust::task_store::{escape_tsql, parse_sqlserver_conn_str, run_sqlcmd_query};
use rusqlite::Connection as SqliteConnection;
use serde_json::{Map, Value};
use std::{
    fs,
    io::{BufRead, BufReader},
    sync::Arc,
};

mod db;
mod parquet;
mod text;

pub(crate) use db::{load_sqlite_rows, load_sqlserver_rows, save_rows_sqlite, save_rows_sqlserver};
pub(crate) use parquet::{
    load_parquet_rows, parquet_compression_from_name, save_rows_parquet, save_rows_parquet_payload,
    save_rows_parquet_payload_with_compression, save_rows_parquet_typed,
    save_rows_parquet_typed_with_compression,
};
pub(crate) use text::{
    load_csv_rows, load_csv_rows_limited, load_jsonl_rows, load_jsonl_rows_limited, save_rows_csv,
    save_rows_jsonl,
};

pub(crate) fn load_rows_from_uri_limited(
    uri: &str,
    max_rows: usize,
    max_bytes: usize,
) -> Result<Vec<Value>, String> {
    let lower = uri.to_lowercase();
    if lower.ends_with(".jsonl") {
        return load_jsonl_rows_limited(uri, max_rows, max_bytes);
    }
    if lower.ends_with(".csv") {
        return load_csv_rows_limited(uri, max_rows, max_bytes);
    }
    if lower.ends_with(".parquet") {
        if let Ok(meta) = fs::metadata(uri)
            && meta.len() as usize > max_bytes
        {
            return Err(format!(
                "input parquet exceeds byte limit: {} > {}",
                meta.len(),
                max_bytes
            ));
        }
        return load_parquet_rows(uri, max_rows);
    }
    if lower.starts_with("sqlite://") {
        let p = uri.trim_start_matches("sqlite://");
        return load_sqlite_rows(p, "SELECT * FROM data", max_rows);
    }
    if lower.starts_with("sqlserver://") {
        let q = "SELECT TOP 10000 * FROM dbo.workflow_tasks";
        return load_sqlserver_rows(uri.trim_start_matches("sqlserver://"), q, max_rows);
    }
    Err("unsupported input_uri".to_string())
}

pub(crate) fn save_rows_to_uri(uri: &str, rows: &[Value]) -> Result<(), String> {
    let lower = uri.to_lowercase();
    if lower.ends_with(".jsonl") {
        return save_rows_jsonl(uri, rows);
    }
    if lower.ends_with(".csv") {
        return save_rows_csv(uri, rows);
    }
    if lower.ends_with(".parquet") {
        return save_rows_parquet(uri, rows);
    }
    Err("unsupported output_uri".to_string())
}
