use super::*;

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
            if b == 0.0 { Value::Null } else { json!(a / b) }
        }
        "concat" => Value::String(
            args.iter()
                .map(value_to_string)
                .collect::<Vec<_>>()
                .join(""),
        ),
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
    let t = s.trim();
    let sep = if t.contains('-') {
        '-'
    } else if t.contains('/') {
        '/'
    } else {
        return None;
    };
    let parts = t.split(sep).collect::<Vec<_>>();
    if parts.len() < 3 {
        return None;
    }
    let y = parts[0].trim().parse::<i64>().ok()?;
    let m = parts[1].trim().parse::<i64>().ok()?;
    let d = parts[2].trim().parse::<i64>().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some((y, m, d))
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
    }
}
