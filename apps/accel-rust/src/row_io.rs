use crate::*;

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

pub(crate) fn load_jsonl_rows(path: &str, limit: usize) -> Result<Vec<Value>, String> {
    load_jsonl_rows_limited(path, limit, 128 * 1024 * 1024)
}

pub(crate) fn load_jsonl_rows_limited(
    path: &str,
    limit: usize,
    max_bytes: usize,
) -> Result<Vec<Value>, String> {
    if let Ok(meta) = fs::metadata(path)
        && meta.len() as usize > max_bytes
    {
        return Err(format!(
            "jsonl exceeds byte limit: {} > {}",
            meta.len(),
            max_bytes
        ));
    }
    let f = fs::File::open(path).map_err(|e| format!("read jsonl: {e}"))?;
    let mut rd = BufReader::new(f);
    let mut out = Vec::new();
    let mut line = String::new();
    let mut read_rows = 0usize;
    let mut read_bytes = 0usize;
    loop {
        line.clear();
        let n = rd
            .read_line(&mut line)
            .map_err(|e| format!("read jsonl line: {e}"))?;
        if n == 0 || read_rows >= limit {
            break;
        }
        read_bytes += n;
        if read_bytes > max_bytes {
            return Err(format!(
                "jsonl streaming bytes exceed limit: {} > {}",
                read_bytes, max_bytes
            ));
        }
        let s = line.trim();
        if s.is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(s).map_err(|e| format!("jsonl parse: {e}"))?;
        out.push(v);
        read_rows += 1;
    }
    Ok(out)
}

pub(crate) fn load_csv_rows(path: &str, limit: usize) -> Result<Vec<Value>, String> {
    load_csv_rows_limited(path, limit, 128 * 1024 * 1024)
}

pub(crate) fn load_csv_rows_limited(
    path: &str,
    limit: usize,
    max_bytes: usize,
) -> Result<Vec<Value>, String> {
    if let Ok(meta) = fs::metadata(path)
        && meta.len() as usize > max_bytes
    {
        return Err(format!(
            "csv exceeds byte limit: {} > {}",
            meta.len(),
            max_bytes
        ));
    }
    let f = fs::File::open(path).map_err(|e| format!("read csv: {e}"))?;
    let mut rd = BufReader::new(f);
    let mut header = String::new();
    let n = rd
        .read_line(&mut header)
        .map_err(|e| format!("read csv header: {e}"))?;
    if n == 0 {
        return Ok(Vec::new());
    }
    let cols: Vec<String> = header
        .trim_end()
        .split(',')
        .map(|x| x.trim().to_string())
        .collect();
    let mut out = Vec::new();
    let mut line = String::new();
    let mut read_rows = 0usize;
    let mut read_bytes = n;
    loop {
        line.clear();
        let n = rd
            .read_line(&mut line)
            .map_err(|e| format!("read csv line: {e}"))?;
        if n == 0 || read_rows >= limit {
            break;
        }
        read_bytes += n;
        if read_bytes > max_bytes {
            return Err(format!(
                "csv streaming bytes exceed limit: {} > {}",
                read_bytes, max_bytes
            ));
        }
        let vals: Vec<&str> = line.trim_end().split(',').collect();
        let mut obj = Map::new();
        for (i, c) in cols.iter().enumerate() {
            obj.insert(
                c.clone(),
                Value::String(vals.get(i).copied().unwrap_or("").trim().to_string()),
            );
        }
        out.push(Value::Object(obj));
        read_rows += 1;
    }
    Ok(out)
}

pub(crate) fn load_parquet_rows(path: &str, limit: usize) -> Result<Vec<Value>, String> {
    let file = fs::File::open(path).map_err(|e| format!("open parquet: {e}"))?;
    let reader = SerializedFileReader::new(file).map_err(|e| format!("read parquet: {e}"))?;
    let schema = reader
        .metadata()
        .file_metadata()
        .schema_descr_ptr()
        .root_schema()
        .clone();
    if schema.get_fields().len() == 1 && schema.get_fields()[0].name() == "payload" {
        return load_parquet_payload_rows(reader, limit);
    }
    let mut out: Vec<Value> = Vec::new();
    let iter = reader
        .get_row_iter(None)
        .map_err(|e| format!("parquet row iter: {e}"))?;
    for row in iter.take(limit) {
        let row = row.map_err(|e| format!("parquet row: {e}"))?;
        out.push(row.to_json_value());
    }
    Ok(out)
}

pub(crate) fn load_parquet_payload_rows(
    reader: SerializedFileReader<fs::File>,
    limit: usize,
) -> Result<Vec<Value>, String> {
    let mut out: Vec<Value> = Vec::new();
    for rg_i in 0..reader.num_row_groups() {
        if out.len() >= limit {
            break;
        }
        let rg = reader
            .get_row_group(rg_i)
            .map_err(|e| format!("parquet row group: {e}"))?;
        if rg.num_columns() == 0 {
            continue;
        }
        let mut col = rg
            .get_column_reader(0)
            .map_err(|e| format!("parquet column reader: {e}"))?;
        match col {
            ColumnReader::ByteArrayColumnReader(ref mut typed) => loop {
                if out.len() >= limit {
                    break;
                }
                let to_read = (limit - out.len()).min(2048);
                let mut vals: Vec<ByteArray> = Vec::with_capacity(to_read);
                let (rows_read, _, _) = typed
                    .read_records(to_read, None, None, &mut vals)
                    .map_err(|e| format!("parquet read records: {e}"))?;
                if rows_read == 0 {
                    break;
                }
                for b in vals.into_iter().take(rows_read) {
                    let parsed = std::str::from_utf8(b.data())
                        .ok()
                        .and_then(|s| serde_json::from_str::<Value>(s).ok())
                        .unwrap_or_else(|| {
                            Value::String(String::from_utf8_lossy(b.data()).to_string())
                        });
                    out.push(parsed);
                    if out.len() >= limit {
                        break;
                    }
                }
            },
            _ => return Err("parquet generic loader expects BYTE_ARRAY payload column".to_string()),
        }
    }
    Ok(out)
}

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

pub(crate) fn save_rows_jsonl(path: &str, rows: &[Value]) -> Result<(), String> {
    let mut out = String::new();
    for r in rows {
        out.push_str(&serde_json::to_string(r).map_err(|e| e.to_string())?);
        out.push('\n');
    }
    fs::write(path, out).map_err(|e| format!("write jsonl: {e}"))
}

pub(crate) fn save_rows_csv(path: &str, rows: &[Value]) -> Result<(), String> {
    let cols = rows
        .iter()
        .filter_map(|v| v.as_object())
        .flat_map(|m| m.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut out = String::new();
    out.push_str(&cols.join(","));
    out.push('\n');
    for r in rows {
        let Some(obj) = r.as_object() else {
            continue;
        };
        let line = cols
            .iter()
            .map(|c| value_to_string_or_null(obj.get(c)).replace(',', " "))
            .collect::<Vec<_>>()
            .join(",");
        out.push_str(&line);
        out.push('\n');
    }
    fs::write(path, out).map_err(|e| format!("write csv: {e}"))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TypedColKind {
    Bool,
    Int,
    Float,
    Str,
}

#[derive(Clone)]
struct TypedColSpec {
    name: String,
    kind: TypedColKind,
}

fn infer_typed_parquet_columns(rows: &[Value]) -> Vec<TypedColSpec> {
    let cols = rows
        .iter()
        .filter_map(|v| v.as_object())
        .flat_map(|m| m.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut out = Vec::new();
    for c in cols {
        let mut saw_bool = false;
        let mut saw_int = false;
        let mut saw_float = false;
        let mut saw_string = false;
        for r in rows {
            let Some(obj) = r.as_object() else {
                continue;
            };
            let Some(v) = obj.get(&c) else {
                continue;
            };
            if v.is_null() {
                continue;
            }
            match v {
                Value::Bool(_) => saw_bool = true,
                Value::Number(n) => {
                    if n.is_i64() || n.is_u64() {
                        saw_int = true;
                    } else {
                        saw_float = true;
                    }
                }
                Value::String(_) => saw_string = true,
                _ => saw_string = true,
            }
        }
        let kind = if saw_string || (saw_bool && (saw_int || saw_float)) {
            TypedColKind::Str
        } else if saw_float {
            TypedColKind::Float
        } else if saw_int {
            TypedColKind::Int
        } else if saw_bool {
            TypedColKind::Bool
        } else {
            TypedColKind::Str
        };
        out.push(TypedColSpec { name: c, kind });
    }
    out
}

pub(crate) fn save_rows_parquet(path: &str, rows: &[Value]) -> Result<(), String> {
    save_rows_parquet_typed(path, rows)
}

pub(crate) fn parquet_compression_from_name(name: &str) -> Result<Compression, String> {
    match name.trim().to_lowercase().as_str() {
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        other => Err(format!("unsupported parquet compression: {other}")),
    }
}

pub(crate) fn save_rows_parquet_payload(path: &str, rows: &[Value]) -> Result<(), String> {
    save_rows_parquet_payload_with_compression(path, rows, Compression::SNAPPY)
}

pub(crate) fn save_rows_parquet_payload_with_compression(
    path: &str,
    rows: &[Value],
    compression: Compression,
) -> Result<(), String> {
    let payload_col = Arc::new(
        Type::primitive_type_builder("payload", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .map_err(|e| format!("build parquet payload schema: {e}"))?,
    );
    let schema = Arc::new(
        Type::group_type_builder("aiwf_rows")
            .with_fields(vec![payload_col])
            .build()
            .map_err(|e| format!("build parquet schema: {e}"))?,
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(compression)
            .build(),
    );
    let file = fs::File::create(path).map_err(|e| format!("create parquet: {e}"))?;
    let mut writer = SerializedFileWriter::new(file, schema, props)
        .map_err(|e| format!("create parquet writer: {e}"))?;
    let mut row_group_writer = writer
        .next_row_group()
        .map_err(|e| format!("open parquet row group: {e}"))?;
    let payloads = rows
        .iter()
        .map(|r| {
            serde_json::to_string(r)
                .map(|s| ByteArray::from(s.into_bytes()))
                .map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<ByteArray>, String>>()?;
    while let Some(mut column_writer) = row_group_writer
        .next_column()
        .map_err(|e| format!("open parquet column: {e}"))?
    {
        match column_writer.untyped() {
            ColumnWriter::ByteArrayColumnWriter(typed) => {
                typed
                    .write_batch(&payloads, None, None)
                    .map_err(|e| format!("write parquet payload values: {e}"))?;
            }
            _ => return Err("unexpected parquet generic column type".to_string()),
        }
        column_writer
            .close()
            .map_err(|e| format!("close parquet column: {e}"))?;
    }
    row_group_writer
        .close()
        .map_err(|e| format!("close parquet row group: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("close parquet writer: {e}"))?;
    Ok(())
}

pub(crate) fn save_rows_parquet_typed(path: &str, rows: &[Value]) -> Result<(), String> {
    save_rows_parquet_typed_with_compression(path, rows, Compression::SNAPPY)
}

pub(crate) fn save_rows_parquet_typed_with_compression(
    path: &str,
    rows: &[Value],
    compression: Compression,
) -> Result<(), String> {
    let specs = infer_typed_parquet_columns(rows);
    if specs.is_empty() {
        return save_rows_parquet_payload_with_compression(path, rows, compression);
    }
    let mut fields = Vec::new();
    for c in &specs {
        let ty = match c.kind {
            TypedColKind::Bool => PhysicalType::BOOLEAN,
            TypedColKind::Int => PhysicalType::INT64,
            TypedColKind::Float => PhysicalType::DOUBLE,
            TypedColKind::Str => PhysicalType::BYTE_ARRAY,
        };
        fields.push(Arc::new(if c.kind == TypedColKind::Str {
            Type::primitive_type_builder(&c.name, ty)
                .with_repetition(Repetition::OPTIONAL)
                .with_logical_type(Some(LogicalType::String))
                .build()
                .map_err(|e| format!("build parquet typed string column schema: {e}"))?
        } else {
            Type::primitive_type_builder(&c.name, ty)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .map_err(|e| format!("build parquet typed column schema: {e}"))?
        }));
    }
    let schema = Arc::new(
        Type::group_type_builder("aiwf_rows_typed")
            .with_fields(fields)
            .build()
            .map_err(|e| format!("build parquet typed schema: {e}"))?,
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(compression)
            .build(),
    );
    let file = fs::File::create(path).map_err(|e| format!("create parquet: {e}"))?;
    let mut writer = SerializedFileWriter::new(file, schema, props)
        .map_err(|e| format!("create parquet writer: {e}"))?;
    let mut row_group_writer = writer
        .next_row_group()
        .map_err(|e| format!("open parquet row group: {e}"))?;
    let n = rows.len();
    for c in &specs {
        let Some(mut column_writer) = row_group_writer
            .next_column()
            .map_err(|e| format!("open parquet typed column: {e}"))?
        else {
            break;
        };
        match column_writer.untyped() {
            ColumnWriter::BoolColumnWriter(typed) if c.kind == TypedColKind::Bool => {
                let mut vals = Vec::<bool>::new();
                let mut defs = vec![0i16; n];
                for (i, r) in rows.iter().enumerate() {
                    if let Some(v) = r
                        .as_object()
                        .and_then(|o| o.get(&c.name))
                        .and_then(|v| v.as_bool())
                    {
                        defs[i] = 1;
                        vals.push(v);
                    }
                }
                typed
                    .write_batch(&vals, Some(&defs), None)
                    .map_err(|e| format!("write parquet bool column: {e}"))?;
            }
            ColumnWriter::Int64ColumnWriter(typed) if c.kind == TypedColKind::Int => {
                let mut vals = Vec::<i64>::new();
                let mut defs = vec![0i16; n];
                for (i, r) in rows.iter().enumerate() {
                    if let Some(v) = r
                        .as_object()
                        .and_then(|o| o.get(&c.name))
                        .and_then(value_to_i64)
                    {
                        defs[i] = 1;
                        vals.push(v);
                    }
                }
                typed
                    .write_batch(&vals, Some(&defs), None)
                    .map_err(|e| format!("write parquet int column: {e}"))?;
            }
            ColumnWriter::DoubleColumnWriter(typed) if c.kind == TypedColKind::Float => {
                let mut vals = Vec::<f64>::new();
                let mut defs = vec![0i16; n];
                for (i, r) in rows.iter().enumerate() {
                    if let Some(v) = r
                        .as_object()
                        .and_then(|o| o.get(&c.name))
                        .and_then(value_to_f64)
                    {
                        defs[i] = 1;
                        vals.push(v);
                    }
                }
                typed
                    .write_batch(&vals, Some(&defs), None)
                    .map_err(|e| format!("write parquet float column: {e}"))?;
            }
            ColumnWriter::ByteArrayColumnWriter(typed) if c.kind == TypedColKind::Str => {
                let mut vals = Vec::<ByteArray>::new();
                let mut defs = vec![0i16; n];
                for (i, r) in rows.iter().enumerate() {
                    let s = r
                        .as_object()
                        .and_then(|o| o.get(&c.name))
                        .map(|v| value_to_string_or_null(Some(v)))
                        .unwrap_or_default();
                    if !s.is_empty() {
                        defs[i] = 1;
                        vals.push(ByteArray::from(s.into_bytes()));
                    }
                }
                typed
                    .write_batch(&vals, Some(&defs), None)
                    .map_err(|e| format!("write parquet string column: {e}"))?;
            }
            _ => return Err("unexpected parquet typed column writer type".to_string()),
        }
        column_writer
            .close()
            .map_err(|e| format!("close parquet typed column: {e}"))?;
    }
    row_group_writer
        .close()
        .map_err(|e| format!("close parquet typed row group: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("close parquet typed writer: {e}"))?;
    Ok(())
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
