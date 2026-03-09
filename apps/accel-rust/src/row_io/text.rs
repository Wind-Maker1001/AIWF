use super::*;

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
