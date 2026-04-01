use crate::{
    api_types::{
        DataSourceBrowserColumn, DataSourceBrowserItem, DataSourceBrowserV1Req,
        DataSourceBrowserV1Resp,
    },
    row_io::{load_sqlite_rows, load_sqlserver_rows},
    transform_support::validate_sql_identifier,
};
use accel_rust::task_store::{parse_sqlserver_conn_str, run_sqlcmd_query};
use rusqlite::Connection as SqliteConnection;
use serde_json::{Value, json};

pub(crate) fn run_data_source_browser_v1(
    req: DataSourceBrowserV1Req,
) -> Result<DataSourceBrowserV1Resp, String> {
    let source_type = req.source_type.trim().to_ascii_lowercase();
    let op = req.op.trim().to_ascii_lowercase();
    let limit = req.limit.unwrap_or(20).clamp(1, 200);
    let mut items = Vec::new();
    let mut columns = Vec::new();
    let mut rows = Vec::new();

    match (source_type.as_str(), op.as_str()) {
        ("sqlite", "validate_connection") => {
            let conn = SqliteConnection::open(&req.source).map_err(|e| format!("sqlite open: {e}"))?;
            conn.query_row("SELECT 1", [], |_| Ok(()))
                .map_err(|e| format!("sqlite probe: {e}"))?;
        }
        ("sqlite", "list_schemas") => {
            let conn = SqliteConnection::open(&req.source).map_err(|e| format!("sqlite open: {e}"))?;
            let mut stmt = conn
                .prepare("PRAGMA database_list")
                .map_err(|e| format!("sqlite pragma prepare: {e}"))?;
            let mapped = stmt
                .query_map([], |row| {
                    Ok(DataSourceBrowserItem {
                        name: row.get::<_, String>(1)?,
                        schema: None,
                        kind: Some("schema".to_string()),
                    })
                })
                .map_err(|e| format!("sqlite pragma query: {e}"))?;
            for item in mapped {
                items.push(item.map_err(|e| format!("sqlite pragma row: {e}"))?);
            }
        }
        ("sqlite", "list_tables") => {
            let conn = SqliteConnection::open(&req.source).map_err(|e| format!("sqlite open: {e}"))?;
            let mut stmt = conn
                .prepare(
                    "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY type, name",
                )
                .map_err(|e| format!("sqlite tables prepare: {e}"))?;
            let mapped = stmt
                .query_map([], |row| {
                    Ok(DataSourceBrowserItem {
                        name: row.get::<_, String>(0)?,
                        schema: Some(req.schema.clone().unwrap_or_else(|| "main".to_string())),
                        kind: Some(row.get::<_, String>(1)?),
                    })
                })
                .map_err(|e| format!("sqlite tables query: {e}"))?;
            for item in mapped {
                items.push(item.map_err(|e| format!("sqlite tables row: {e}"))?);
            }
        }
        ("sqlite", "describe_table") => {
            let conn = SqliteConnection::open(&req.source).map_err(|e| format!("sqlite open: {e}"))?;
            let table = validate_sql_identifier(req.table.as_deref().unwrap_or_default())?;
            let pragma = format!("PRAGMA table_info({table})");
            let mut stmt = conn
                .prepare(&pragma)
                .map_err(|e| format!("sqlite table_info prepare: {e}"))?;
            let mapped = stmt
                .query_map([], |row| {
                    Ok(DataSourceBrowserColumn {
                        name: row.get::<_, String>(1)?,
                        data_type: row.get::<_, String>(2)?,
                        nullable: row.get::<_, i64>(3)? == 0,
                    })
                })
                .map_err(|e| format!("sqlite table_info query: {e}"))?;
            for column in mapped {
                columns.push(column.map_err(|e| format!("sqlite table_info row: {e}"))?);
            }
        }
        ("sqlite", "sample_rows") => {
            let table = validate_sql_identifier(req.table.as_deref().unwrap_or_default())?;
            let sql = format!("SELECT * FROM {table}");
            rows = load_sqlite_rows(&req.source, &sql, limit)?;
        }
        ("sqlserver", "validate_connection") => {
            let cfg = parse_sqlserver_conn_str(&req.source);
            let probe = run_sqlcmd_query(
                &cfg,
                "SET NOCOUNT ON; SELECT name FROM sys.databases WHERE name = DB_NAME() FOR JSON PATH;",
            )?;
            parse_sqlserver_json_array(&probe)?;
        }
        ("sqlserver", "list_schemas") => {
            let cfg = parse_sqlserver_conn_str(&req.source);
            let raw = run_sqlcmd_query(
                &cfg,
                "SET NOCOUNT ON; SELECT name FROM sys.schemas ORDER BY name FOR JSON PATH;",
            )?;
            items = parse_sqlserver_json_array(&raw)?
                .into_iter()
                .map(|item| DataSourceBrowserItem {
                    name: item
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    schema: None,
                    kind: Some("schema".to_string()),
                })
                .collect();
        }
        ("sqlserver", "list_tables") => {
            let cfg = parse_sqlserver_conn_str(&req.source);
            let schema_filter = req
                .schema
                .as_deref()
                .map(validate_sql_identifier)
                .transpose()?;
            let query = match schema_filter.as_deref() {
                Some(schema_name) => format!(
                    "SET NOCOUNT ON; SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
                     FROM INFORMATION_SCHEMA.TABLES \
                     WHERE TABLE_SCHEMA = N'{schema_name}' \
                     ORDER BY TABLE_SCHEMA, TABLE_NAME FOR JSON PATH;"
                ),
                None => "SET NOCOUNT ON; SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
                         FROM INFORMATION_SCHEMA.TABLES \
                         ORDER BY TABLE_SCHEMA, TABLE_NAME FOR JSON PATH;"
                    .to_string(),
            };
            let raw = run_sqlcmd_query(&cfg, &query)?;
            items = parse_sqlserver_json_array(&raw)?
                .into_iter()
                .map(|item| DataSourceBrowserItem {
                    name: item
                        .get("TABLE_NAME")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    schema: item
                        .get("TABLE_SCHEMA")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    kind: item
                        .get("TABLE_TYPE")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                })
                .collect();
        }
        ("sqlserver", "describe_table") => {
            let cfg = parse_sqlserver_conn_str(&req.source);
            let schema = validate_sql_identifier(req.schema.as_deref().unwrap_or("dbo"))?;
            let table = validate_sql_identifier(req.table.as_deref().unwrap_or_default())?;
            let query = format!(
                "SET NOCOUNT ON; SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_SCHEMA = N'{schema}' AND TABLE_NAME = N'{table}' \
                 ORDER BY ORDINAL_POSITION FOR JSON PATH;"
            );
            let raw = run_sqlcmd_query(&cfg, &query)?;
            columns = parse_sqlserver_json_array(&raw)?
                .into_iter()
                .map(|item| DataSourceBrowserColumn {
                    name: item
                        .get("COLUMN_NAME")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    data_type: item
                        .get("DATA_TYPE")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    nullable: item
                        .get("IS_NULLABLE")
                        .and_then(Value::as_str)
                        .map(|value| value.eq_ignore_ascii_case("YES"))
                        .unwrap_or(true),
                })
                .collect();
        }
        ("sqlserver", "sample_rows") => {
            let schema = validate_sql_identifier(req.schema.as_deref().unwrap_or("dbo"))?;
            let table = validate_sql_identifier(req.table.as_deref().unwrap_or_default())?;
            let sql = format!("SELECT * FROM {schema}.{table}");
            rows = load_sqlserver_rows(&req.source, &sql, limit)?;
        }
        ("sqlite" | "sqlserver", _) => return Err(format!("unsupported data_source_browser_v1 op: {}", req.op)),
        _ => return Err(format!("unsupported data_source_browser_v1 source_type: {}", req.source_type)),
    }

    let item_count = items.len();
    let column_count = columns.len();
    let row_count = rows.len();
    Ok(DataSourceBrowserV1Resp {
        ok: true,
        operator: "data_source_browser_v1".to_string(),
        status: "done".to_string(),
        op,
        items,
        columns,
        rows: rows.clone(),
        stats: json!({
            "source_type": source_type,
            "item_count": item_count,
            "column_count": column_count,
            "row_count": row_count,
            "schema": req.schema,
            "table": req.table,
            "limit": limit,
        }),
    })
}

fn parse_sqlserver_json_array(raw: &str) -> Result<Vec<Value>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let parsed: Value =
        serde_json::from_str(trimmed).map_err(|e| format!("sqlserver browser json parse: {e}"))?;
    Ok(parsed.as_array().cloned().unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::{env, fs, path::PathBuf, time::{SystemTime, UNIX_EPOCH}};

    fn temp_sqlite_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        env::temp_dir().join(format!("aiwf_browser_{name}_{nanos}.db"))
    }

    fn seeded_sqlite_path() -> PathBuf {
        let path = temp_sqlite_path("seed");
        let conn = Connection::open(&path).expect("open sqlite");
        conn.execute(
            "CREATE TABLE metrics(id INTEGER PRIMARY KEY, category TEXT NOT NULL, amount REAL NOT NULL)",
            [],
        )
        .expect("create table");
        conn.execute(
            "INSERT INTO metrics(category, amount) VALUES ('books', 12.5), ('games', 44.0)",
            [],
        )
        .expect("seed rows");
        path
    }

    #[test]
    fn browser_lists_sqlite_schemas_tables_and_columns() {
        let path = seeded_sqlite_path();
        let path_str = path.to_string_lossy().to_string();

        let schemas = run_data_source_browser_v1(DataSourceBrowserV1Req {
            source_type: "sqlite".to_string(),
            source: path_str.clone(),
            op: "list_schemas".to_string(),
            schema: None,
            table: None,
            limit: None,
        })
        .expect("list schemas");
        assert!(schemas.items.iter().any(|item| item.name == "main"));

        let tables = run_data_source_browser_v1(DataSourceBrowserV1Req {
            source_type: "sqlite".to_string(),
            source: path_str.clone(),
            op: "list_tables".to_string(),
            schema: Some("main".to_string()),
            table: None,
            limit: None,
        })
        .expect("list tables");
        assert!(tables.items.iter().any(|item| item.name == "metrics"));

        let columns = run_data_source_browser_v1(DataSourceBrowserV1Req {
            source_type: "sqlite".to_string(),
            source: path_str.clone(),
            op: "describe_table".to_string(),
            schema: Some("main".to_string()),
            table: Some("metrics".to_string()),
            limit: None,
        })
        .expect("describe table");
        assert!(columns.columns.iter().any(|column| column.name == "category"));

        let rows = run_data_source_browser_v1(DataSourceBrowserV1Req {
            source_type: "sqlite".to_string(),
            source: path_str,
            op: "sample_rows".to_string(),
            schema: Some("main".to_string()),
            table: Some("metrics".to_string()),
            limit: Some(1),
        })
        .expect("sample rows");
        assert_eq!(rows.rows.len(), 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn browser_rejects_unknown_source_types() {
        let error = run_data_source_browser_v1(DataSourceBrowserV1Req {
            source_type: "postgres".to_string(),
            source: "db".to_string(),
            op: "validate_connection".to_string(),
            schema: None,
            table: None,
            limit: None,
        })
        .expect_err("unsupported source type");
        assert!(error.contains("unsupported data_source_browser_v1 source_type"));
    }
}
