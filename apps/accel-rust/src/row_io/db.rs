use super::*;

pub(crate) fn load_sqlite_rows(
    db_path: &str,
    query: &str,
    limit: usize,
) -> Result<Vec<Value>, String> {
    let safe_query = validate_readonly_query(query)?;
    let conn = SqliteConnection::open(db_path).map_err(|e| format!("sqlite open: {e}"))?;
    let q = format!("{safe_query} LIMIT {}", limit);
    let mut stmt = conn
        .prepare(&q)
        .map_err(|e| format!("sqlite prepare: {e}"))?;
    let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let mut rows = stmt.query([]).map_err(|e| format!("sqlite query: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("sqlite next: {e}"))? {
        let mut obj = Map::new();
        for (i, name) in col_names.iter().enumerate() {
            let s = row.get_ref(i).map(|v| format!("{v:?}")).unwrap_or_default();
            obj.insert(name.clone(), Value::String(s));
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

pub(crate) fn load_sqlserver_rows(
    conn_str: &str,
    query: &str,
    limit: usize,
) -> Result<Vec<Value>, String> {
    let safe_query = validate_readonly_query(query)?;
    let cfg = parse_sqlserver_conn_str(conn_str);
    let q = format!("SET NOCOUNT ON; SELECT TOP {limit} * FROM ({safe_query}) x FOR JSON PATH;");
    let out = run_sqlcmd_query(&cfg, &q)?;
    let s = out.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    let arr: Value = serde_json::from_str(s).map_err(|e| format!("sqlserver json parse: {e}"))?;
    Ok(arr.as_array().cloned().unwrap_or_default())
}

pub(crate) fn save_rows_sqlite(db_path: &str, table: &str, rows: &[Value]) -> Result<(), String> {
    let table = validate_sql_identifier(table)?;
    let conn = SqliteConnection::open(db_path).map_err(|e| format!("sqlite open: {e}"))?;
    conn.execute(
        &format!("CREATE TABLE IF NOT EXISTS {table} (payload TEXT NOT NULL)"),
        [],
    )
    .map_err(|e| format!("sqlite create: {e}"))?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| format!("sqlite tx: {e}"))?;
    for r in rows {
        let s = serde_json::to_string(r).map_err(|e| e.to_string())?;
        tx.execute(&format!("INSERT INTO {table}(payload) VALUES (?1)"), [&s])
            .map_err(|e| format!("sqlite insert: {e}"))?;
    }
    tx.commit().map_err(|e| format!("sqlite commit: {e}"))
}

pub(crate) fn save_rows_sqlserver(
    conn_str: &str,
    table: &str,
    rows: &[Value],
) -> Result<(), String> {
    let table = validate_sql_identifier(table)?;
    let cfg = parse_sqlserver_conn_str(conn_str);
    let q_create = format!(
        "IF OBJECT_ID('{table}','U') IS NULL CREATE TABLE {table}(payload NVARCHAR(MAX) NOT NULL);"
    );
    let _ = run_sqlcmd_query(&cfg, &q_create)?;
    for r in rows {
        let payload = escape_tsql(&serde_json::to_string(r).map_err(|e| e.to_string())?);
        let q = format!("INSERT INTO {table}(payload) VALUES (N'{payload}');");
        let _ = run_sqlcmd_query(&cfg, &q)?;
    }
    Ok(())
}
