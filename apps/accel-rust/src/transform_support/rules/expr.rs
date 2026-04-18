use super::*;
use regex::Regex;

pub(crate) fn parse_expr_arg(token: &str, row: &Map<String, Value>) -> Value {
    let t = token.trim();
    if let Some(field) = t.strip_prefix('$') {
        return row.get(field).cloned().unwrap_or(Value::Null);
    }
    if (t.starts_with('"') && t.ends_with('"')) || (t.starts_with('\'') && t.ends_with('\'')) {
        return Value::String(t[1..t.len().saturating_sub(1)].to_string());
    }
    if let Ok(v) = t.parse::<i64>() {
        return Value::Number(v.into());
    }
    if let Ok(v) = t.parse::<f64>() {
        return serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    row.get(t).cloned().unwrap_or(Value::Null)
}

pub(crate) fn eval_simple_expr(expr: &str, row: &Map<String, Value>) -> Value {
    let e = expr.trim();
    let open = e.find('(');
    let close = e.rfind(')');
    let Some(l) = open else {
        return parse_expr_arg(e, row);
    };
    let Some(r) = close else {
        return parse_expr_arg(e, row);
    };
    if r <= l {
        return parse_expr_arg(e, row);
    }
    let fn_name = e[..l].trim().to_ascii_lowercase();
    let args_txt = &e[l + 1..r];
    let args = args_txt
        .split(',')
        .map(|s| parse_expr_arg(s, row))
        .collect::<Vec<_>>();
    match fn_name.as_str() {
        "add" => {
            let a = args.first().and_then(value_to_f64).unwrap_or(0.0);
            let b = args.get(1).and_then(value_to_f64).unwrap_or(0.0);
            json!(a + b)
        }
        "sub" => {
            let a = args.first().and_then(value_to_f64).unwrap_or(0.0);
            let b = args.get(1).and_then(value_to_f64).unwrap_or(0.0);
            json!(a - b)
        }
        "mul" => {
            let a = args.first().and_then(value_to_f64).unwrap_or(0.0);
            let b = args.get(1).and_then(value_to_f64).unwrap_or(0.0);
            json!(a * b)
        }
        "div" => {
            let a = args.first().and_then(value_to_f64).unwrap_or(0.0);
            let b = args.get(1).and_then(value_to_f64).unwrap_or(0.0);
            if b == 0.0 {
                Value::Null
            } else {
                json!(a / b)
            }
        }
        "concat" => Value::String(args.iter().map(value_to_string).collect::<Vec<_>>().join("")),
        "coalesce" => args
            .into_iter()
            .find(|v| !is_missing(Some(v)))
            .unwrap_or(Value::Null),
        "lower" => Value::String(
            args.first()
                .map(value_to_string)
                .unwrap_or_default()
                .to_lowercase(),
        ),
        "upper" => Value::String(
            args.first()
                .map(value_to_string)
                .unwrap_or_default()
                .to_uppercase(),
        ),
        "trim" => Value::String(
            args.first()
                .map(value_to_string)
                .unwrap_or_default()
                .trim()
                .to_string(),
        ),
        _ => Value::Null,
    }
}

pub(crate) fn apply_expression_fields(
    rows: &mut [Map<String, Value>],
    rules: &Value,
    rule_hits: &mut HashMap<String, usize>,
) {
    let Some(exprs) = rule_get(rules, "computed_fields").and_then(|v| v.as_object()) else {
        return;
    };
    for r in rows {
        for (field, expr_v) in exprs {
            let Some(expr) = expr_v.as_str() else {
                continue;
            };
            let v = eval_simple_expr(expr, r);
            r.insert(field.clone(), v);
            *rule_hits.entry("computed_fields".to_string()).or_insert(0) += 1;
        }
    }
}

pub(crate) fn parse_ymd_simple(s: &str) -> Option<(i64, i64, i64)> {
    let mut t = s.trim().to_string();
    if t.is_empty() {
        return None;
    }
    t = t
        .replace(['年', '.'], "-")
        .replace('月', "-")
        .replace('日', "")
        .replace('/', "-");
    let digits_only = t.chars().all(|ch| ch.is_ascii_digit());
    if digits_only && t.len() == 8 {
        let y = t[0..4].parse::<i64>().ok()?;
        let m = t[4..6].parse::<i64>().ok()?;
        let d = t[6..8].parse::<i64>().ok()?;
        if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
            return None;
        }
        return Some((y, m, d));
    }
    let parts = t
        .split('-')
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>();
    if parts.len() < 3 {
        return None;
    }
    let y = parts[0].parse::<i64>().ok()?;
    let m = parts[1].parse::<i64>().ok()?;
    let d = parts[2].parse::<i64>().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some((y, m, d))
}

fn round_half_up(v: f64, digits: i32) -> f64 {
    let factor = 10f64.powi(digits.max(0));
    (v * factor).round() / factor
}

fn collapse_ws_text(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalize_phone_cn(value: &str) -> String {
    let mut digits = value
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if digits.starts_with("86") && digits.len() >= 11 {
        digits = digits[2..].to_string();
    }
    if digits.len() == 11 && digits.starts_with('1') {
        format!("+86{digits}")
    } else {
        value.to_string()
    }
}

fn normalize_account_no(value: &str) -> String {
    let normalized = value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase();
    if normalized.is_empty() {
        value.to_string()
    } else {
        normalized
    }
}

fn normalize_name(value: &str) -> String {
    collapse_ws_text(&value.replace('\u{3000}', " "))
}

fn fix_ocr_digit_text(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'O' | 'o' => '0',
            'I' | 'l' | '|' => '1',
            'S' | 's' => '5',
            'B' => '8',
            _ => ch,
        })
        .collect()
}

fn detect_amount_multiplier(text: &str) -> f64 {
    let lowered = text.trim().to_lowercase();
    if lowered.contains("billion") || text.contains('亿') {
        100_000_000.0
    } else if lowered.contains("million") {
        1_000_000.0
    } else if lowered.contains("thousand") || text.contains('千') {
        1_000.0
    } else if text.contains('万') {
        10_000.0
    } else {
        1.0
    }
}

fn apply_field_op(current: &Value, obj: &Map<String, Value>, row: &Map<String, Value>) -> Option<Value> {
    let kind = obj
        .get("op")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if kind.is_empty() {
        return None;
    }
    let cur = value_to_string(current);
    match kind.as_str() {
        "trim" => Some(Value::String(cur.trim().to_string())),
        "lower" => Some(Value::String(cur.to_lowercase())),
        "upper" => Some(Value::String(cur.to_uppercase())),
        "collapse_whitespace" => Some(Value::String(collapse_ws_text(&cur))),
        "remove_urls" => Regex::new(r"https?://\S+|www\.\S+")
            .ok()
            .map(|re| Value::String(re.replace_all(&cur, "").trim().to_string())),
        "remove_emails" => Regex::new(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
            .ok()
            .map(|re| Value::String(re.replace_all(&cur, "").trim().to_string())),
        "regex_replace" => {
            let pattern = obj.get("pattern").map(value_to_string).unwrap_or_default();
            let replacement = obj
                .get("replace")
                .or_else(|| obj.get("to"))
                .map(value_to_string)
                .unwrap_or_default();
            Regex::new(&pattern)
                .ok()
                .map(|re| Value::String(re.replace_all(&cur, replacement.as_str()).to_string()))
        }
        "extract_regex" => {
            let pattern = obj.get("pattern").map(value_to_string).unwrap_or_default();
            let group = obj.get("group").and_then(value_to_i64).unwrap_or(0).max(0) as usize;
            let re = Regex::new(&pattern).ok()?;
            let captures = re.captures(&cur)?;
            captures
                .get(group)
                .map(|matched| Value::String(matched.as_str().to_string()))
        }
        "parse_number" => {
            let normalized = cur
                .replace(',', "")
                .replace('，', "")
                .replace('$', "")
                .replace('¥', "")
                .replace('￥', "")
                .replace('€', "")
                .replace('£', "");
            if normalized.trim().is_empty() {
                return Some(Value::Null);
            }
            normalized
                .trim()
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(Value::Number)
        }
        "strip_currency_symbol" => Regex::new(r"(?i)(cny|rmb|usd|eur|jpy|人民币|元|圆|\$|¥|￥|€|£)")
            .ok()
            .map(|re| Value::String(re.replace_all(&cur, "").trim().to_string())),
        "strip_thousands_sep" => Regex::new(r"(?<=\d)[,\s](?=\d{3}\b)")
            .ok()
            .map(|re| Value::String(re.replace_all(&cur, "").to_string())),
        "round_number" => {
            let digits = obj.get("digits").and_then(value_to_i64).unwrap_or(2) as i32;
            value_to_f64(current)
                .map(|value| round_half_up(value, digits))
                .and_then(serde_json::Number::from_f64)
                .map(Value::Number)
        }
        "scale_number" => {
            let multiplier = obj.get("multiplier").and_then(value_to_f64).unwrap_or(1.0);
            value_to_f64(current)
                .map(|value| value * multiplier)
                .and_then(serde_json::Number::from_f64)
                .map(Value::Number)
        }
        "map_value" => {
            let mapping = obj.get("mapping").and_then(|v| v.as_object())?;
            let key = cur.trim().to_string();
            mapping.get(&key).cloned().or_else(|| Some(current.clone()))
        }
        "fix_ocr_digits" => Some(Value::String(fix_ocr_digit_text(&cur))),
        "normalize_phone_cn" => Some(Value::String(normalize_phone_cn(&cur))),
        "normalize_account_no" => Some(Value::String(normalize_account_no(&cur))),
        "normalize_name" => Some(Value::String(normalize_name(&cur))),
        "scale_by_header_unit" => {
            let parsed = value_to_f64(current)?;
            let multiplier = obj
                .get("multiplier")
                .and_then(value_to_f64)
                .unwrap_or_else(|| {
                    detect_amount_multiplier(
                        &obj.get("raw_header")
                            .or_else(|| obj.get("header_text"))
                            .or_else(|| obj.get("unit"))
                            .map(value_to_string)
                            .unwrap_or_default(),
                    )
                });
            serde_json::Number::from_f64(parsed * multiplier).map(Value::Number)
        }
        "sign_amount_from_debit_credit" => {
            let debit_field = obj
                .get("debit_field")
                .and_then(|v| v.as_str())
                .unwrap_or("debit_amount");
            let credit_field = obj
                .get("credit_field")
                .and_then(|v| v.as_str())
                .unwrap_or("credit_amount");
            if is_missing(row.get(debit_field)) && is_missing(row.get(credit_field)) {
                None
            } else {
                let debit = row.get(debit_field).and_then(value_to_f64).unwrap_or(0.0);
                let credit = row.get(credit_field).and_then(value_to_f64).unwrap_or(0.0);
                serde_json::Number::from_f64(credit - debit).map(Value::Number)
            }
        }
        "parse_date" => parse_ymd_simple(&cur).map(|(y, m, d)| Value::String(format!("{y:04}-{m:02}-{d:02}"))),
        _ => None,
    }
}

pub(crate) fn apply_string_and_date_ops(
    rows: &mut [Map<String, Value>],
    rules: &Value,
    rule_hits: &mut HashMap<String, usize>,
) {
    let string_ops = rule_get(rules, "string_ops")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let date_ops = rule_get(rules, "date_ops")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let field_ops = rule_get(rules, "field_ops")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for r in rows {
        for op in &string_ops {
            let Some(obj) = op.as_object() else { continue };
            let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
            let kind = obj
                .get("op")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            if field.is_empty() || kind.is_empty() {
                continue;
            }
            if !r.contains_key(field) {
                continue;
            }
            let cur = r.get(field).map(value_to_string).unwrap_or_default();
            let next = match kind.as_str() {
                "trim" => cur.trim().to_string(),
                "lower" => cur.to_lowercase(),
                "upper" => cur.to_uppercase(),
                "replace" => {
                    let from = obj.get("from").map(value_to_string).unwrap_or_default();
                    let to = obj.get("to").map(value_to_string).unwrap_or_default();
                    cur.replace(&from, &to)
                }
                _ => cur,
            };
            r.insert(field.to_string(), Value::String(next));
            *rule_hits.entry("string_ops".to_string()).or_insert(0) += 1;
        }
        for op in &date_ops {
            let Some(obj) = op.as_object() else { continue };
            let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
            let kind = obj
                .get("op")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            let out_field = obj.get("as").and_then(|v| v.as_str()).unwrap_or(field);
            if field.is_empty() || kind.is_empty() {
                continue;
            }
            if !r.contains_key(field) {
                continue;
            }
            let raw = r.get(field).map(value_to_string).unwrap_or_default();
            let out = match parse_ymd_simple(&raw) {
                Some((y, m, d)) => match kind.as_str() {
                    "parse_ymd" => Value::String(format!("{y:04}-{m:02}-{d:02}")),
                    "year" => Value::Number(y.into()),
                    "month" => Value::Number(m.into()),
                    "day" => Value::Number(d.into()),
                    _ => Value::Null,
                },
                None => Value::Null,
            };
            r.insert(out_field.to_string(), out);
            *rule_hits.entry("date_ops".to_string()).or_insert(0) += 1;
        }
        for op in &field_ops {
            let Some(obj) = op.as_object() else { continue };
            let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
            let out_field = obj.get("as").and_then(|v| v.as_str()).unwrap_or(field);
            if field.is_empty() || out_field.is_empty() {
                continue;
            }
            if !r.contains_key(field) {
                continue;
            }
            let current = r.get(field).cloned().unwrap_or(Value::Null);
            let next = apply_field_op(&current, obj, r).unwrap_or(current);
            r.insert(out_field.to_string(), next);
            *rule_hits.entry("field_ops".to_string()).or_insert(0) += 1;
        }
    }
}
