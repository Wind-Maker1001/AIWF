use super::*;

pub(crate) fn write_cleaned_csv(path: &Path, rows: &[CleanRow]) -> Result<(), String> {
    let mut f = fs::File::create(path).map_err(|e| format!("create csv: {e}"))?;
    f.write_all(b"id,amount\n")
        .map_err(|e| format!("write csv header: {e}"))?;
    for r in rows {
        let line = format!("{},{}\n", r.id, r.amount);
        f.write_all(line.as_bytes())
            .map_err(|e| format!("write csv row: {e}"))?;
    }
    Ok(())
}

pub(crate) fn write_cleaned_parquet(path: &Path, rows: &[CleanRow]) -> Result<(), String> {
    let id_col = Arc::new(
        Type::primitive_type_builder("id", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .map_err(|e| format!("build parquet id column schema: {e}"))?,
    );
    let amount_col = Arc::new(
        Type::primitive_type_builder("amount", PhysicalType::DOUBLE)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .map_err(|e| format!("build parquet amount column schema: {e}"))?,
    );
    let schema = Arc::new(
        Type::group_type_builder("aiwf_cleaned")
            .with_fields(vec![id_col, amount_col])
            .build()
            .map_err(|e| format!("build parquet schema: {e}"))?,
    );

    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let file = fs::File::create(path).map_err(|e| format!("create parquet: {e}"))?;
    let mut writer = SerializedFileWriter::new(file, schema, props)
        .map_err(|e| format!("create parquet writer: {e}"))?;

    let mut row_group_writer = writer
        .next_row_group()
        .map_err(|e| format!("open parquet row group: {e}"))?;

    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let amounts: Vec<f64> = rows.iter().map(|r| r.amount).collect();
    while let Some(mut column_writer) = row_group_writer
        .next_column()
        .map_err(|e| format!("open parquet column: {e}"))?
    {
        match column_writer.untyped() {
            ColumnWriter::Int64ColumnWriter(typed) => {
                let values: &[i64] = &ids;
                typed
                    .write_batch(values, None, None)
                    .map_err(|e| format!("write parquet id values: {e}"))?;
            }
            ColumnWriter::DoubleColumnWriter(typed) => {
                let values: &[f64] = &amounts;
                typed
                    .write_batch(values, None, None)
                    .map_err(|e| format!("write parquet amount values: {e}"))?;
            }
            _ => {
                return Err("unexpected parquet column type".to_string());
            }
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

pub(crate) fn write_profile_json(path: &Path, rows: &[CleanRow]) -> Result<(), String> {
    let sum_amount: f64 = rows.iter().map(|r| r.amount).sum();
    let payload = json!({
        "profile": {"rows": rows.len(), "cols": 2, "sum_amount": sum_amount},
        "engine": "accel-rust",
    });
    let s = serde_json::to_string_pretty(&payload).map_err(|e| format!("json profile: {e}"))?;
    fs::write(path, s).map_err(|e| format!("write profile: {e}"))
}

pub(crate) fn office_mode() -> String {
    let mode = env::var("AIWF_ACCEL_OFFICE_MODE").unwrap_or_else(|_| "fallback".to_string());
    let lower = mode.trim().to_lowercase();
    if lower == "strict" {
        "strict".to_string()
    } else {
        "fallback".to_string()
    }
}

pub(crate) fn should_force_bad_parquet(force_bad_parquet: Option<bool>) -> bool {
    if force_bad_parquet.unwrap_or(false) {
        return true;
    }
    env::var("AIWF_ACCEL_FORCE_BAD_PARQUET")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true")
}

pub(crate) fn write_bad_parquet_placeholder(path: &Path) -> Result<(), String> {
    let mut f = fs::File::create(path).map_err(|e| format!("create parquet: {e}"))?;
    f.write_all(b"PARQUET_PLACEHOLDER\n")
        .map_err(|e| format!("write parquet: {e}"))?;
    Ok(())
}

pub(crate) fn find_python_command() -> Option<String> {
    for cmd in ["python", "py"] {
        let probe = if cmd == "py" {
            Command::new(cmd).arg("-3").arg("--version").output()
        } else {
            Command::new(cmd).arg("--version").output()
        };

        if let Ok(out) = probe
            && out.status.success()
        {
            return Some(cmd.to_string());
        }
    }
    None
}

pub(crate) fn write_placeholder_office_documents(
    xlsx: &Path,
    docx: &Path,
    pptx: &Path,
) -> Result<(), String> {
    write_placeholder_binary(xlsx, b"XLSX_PLACEHOLDER\n")?;
    write_placeholder_binary(docx, b"DOCX_PLACEHOLDER\n")?;
    write_placeholder_binary(pptx, b"PPTX_PLACEHOLDER\n")?;
    Ok(())
}

pub(crate) fn write_placeholder_binary(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut f = fs::File::create(path).map_err(|e| format!("create placeholder: {e}"))?;
    f.write_all(bytes)
        .map_err(|e| format!("write placeholder: {e}"))?;
    Ok(())
}

pub(crate) fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = fs::File::open(path).map_err(|e| format!("open for hash: {e}"))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];

    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("read for hash: {e}"))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    let digest = hasher.finalize();
    Ok(format!("{digest:x}"))
}

pub(crate) fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().to_string()
}
