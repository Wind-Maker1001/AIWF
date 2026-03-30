use serde_json::{Value, json};

pub(crate) fn evaluate_quality_gates(quality: &Value, gates: &Value) -> Value {
    let input_rows = quality
        .get("input_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as f64;
    let output_rows = quality
        .get("output_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let invalid_rows = quality
        .get("invalid_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let filtered_rows = quality
        .get("filtered_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let duplicate_rows_removed = quality
        .get("duplicate_rows_removed")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let required_missing_ratio = quality
        .get("required_missing_ratio")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let numeric_parse_rate = quality
        .get("numeric_parse_rate")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);
    let date_parse_rate = quality
        .get("date_parse_rate")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);
    let duplicate_key_ratio = quality
        .get("duplicate_key_ratio")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let blank_row_ratio = quality
        .get("blank_row_ratio")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let mut errors: Vec<String> = Vec::new();

    if let Some(max_invalid_rows) = gates.get("max_invalid_rows").and_then(|v| v.as_u64())
        && invalid_rows > max_invalid_rows
    {
        errors.push(format!(
            "invalid_rows={} exceeds max_invalid_rows={}",
            invalid_rows, max_invalid_rows
        ));
    }
    if let Some(min_output_rows) = gates.get("min_output_rows").and_then(|v| v.as_u64())
        && output_rows < min_output_rows
    {
        errors.push(format!(
            "output_rows={} below min_output_rows={}",
            output_rows, min_output_rows
        ));
    }
    if let Some(max_invalid_ratio) = gates.get("max_invalid_ratio").and_then(|v| v.as_f64()) {
        let ratio = if input_rows > 0.0 {
            invalid_rows as f64 / input_rows
        } else {
            0.0
        };
        if ratio > max_invalid_ratio {
            errors.push(format!(
                "invalid_ratio={:.6} exceeds max_invalid_ratio={:.6}",
                ratio, max_invalid_ratio
            ));
        }
    }
    if let Some(max_required_missing_ratio) = gates
        .get("max_required_missing_ratio")
        .and_then(|v| v.as_f64())
        && required_missing_ratio > max_required_missing_ratio
    {
        errors.push(format!(
            "required_missing_ratio={:.6} exceeds max_required_missing_ratio={:.6}",
            required_missing_ratio, max_required_missing_ratio
        ));
    }
    if let Some(max_filtered_rows) = gates.get("max_filtered_rows").and_then(|v| v.as_u64())
        && filtered_rows > max_filtered_rows
    {
        errors.push(format!(
            "filtered_rows={} exceeds max_filtered_rows={}",
            filtered_rows, max_filtered_rows
        ));
    }
    if let Some(max_duplicate_rows_removed) = gates
        .get("max_duplicate_rows_removed")
        .and_then(|v| v.as_u64())
        && duplicate_rows_removed > max_duplicate_rows_removed
    {
        errors.push(format!(
            "duplicate_rows_removed={} exceeds max_duplicate_rows_removed={}",
            duplicate_rows_removed, max_duplicate_rows_removed
        ));
    }
    if let Some(allow_empty_output) = gates.get("allow_empty_output").and_then(|v| v.as_bool())
        && !allow_empty_output
        && output_rows == 0
    {
        errors.push("output_rows=0 while allow_empty_output=false".to_string());
    }
    if let Some(min_numeric_parse_rate) = gates
        .get("numeric_parse_rate_min")
        .and_then(|v| v.as_f64())
        && numeric_parse_rate < min_numeric_parse_rate
    {
        errors.push(format!(
            "numeric_parse_rate={:.6} below numeric_parse_rate_min={:.6}",
            numeric_parse_rate, min_numeric_parse_rate
        ));
    }
    if let Some(min_date_parse_rate) = gates
        .get("date_parse_rate_min")
        .and_then(|v| v.as_f64())
        && date_parse_rate < min_date_parse_rate
    {
        errors.push(format!(
            "date_parse_rate={:.6} below date_parse_rate_min={:.6}",
            date_parse_rate, min_date_parse_rate
        ));
    }
    if let Some(max_duplicate_key_ratio) = gates
        .get("duplicate_key_ratio_max")
        .and_then(|v| v.as_f64())
        && duplicate_key_ratio > max_duplicate_key_ratio
    {
        errors.push(format!(
            "duplicate_key_ratio={:.6} exceeds duplicate_key_ratio_max={:.6}",
            duplicate_key_ratio, max_duplicate_key_ratio
        ));
    }
    if let Some(max_blank_row_ratio) = gates
        .get("blank_row_ratio_max")
        .and_then(|v| v.as_f64())
        && blank_row_ratio > max_blank_row_ratio
    {
        errors.push(format!(
            "blank_row_ratio={:.6} exceeds blank_row_ratio_max={:.6}",
            blank_row_ratio, max_blank_row_ratio
        ));
    }
    json!({
        "passed": errors.is_empty(),
        "errors": errors,
    })
}
