use super::*;

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
