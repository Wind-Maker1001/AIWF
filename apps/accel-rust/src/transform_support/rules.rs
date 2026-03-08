use crate::*;

pub(crate) fn rule_get<'a>(rules: &'a Value, key: &str) -> Option<&'a Value> {
    rules.as_object().and_then(|m| m.get(key))
}

pub(crate) fn as_array_str(v: Option<&Value>) -> Vec<String> {
    v.and_then(|x| x.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn as_bool(v: Option<&Value>, default: bool) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) != 0,
        Some(Value::String(s)) => {
            let t = s.trim().to_lowercase();
            matches!(t.as_str(), "1" | "true" | "yes" | "on")
        }
        _ => default,
    }
}

pub(crate) fn is_missing(v: Option<&Value>) -> bool {
    match v {
        None => true,
        Some(Value::Null) => true,
        Some(Value::String(s)) => s.trim().is_empty(),
        _ => false,
    }
}

pub(crate) fn cast_value(v: Value, cast_type: &str) -> Option<Value> {
    if v.is_null() {
        return Some(Value::Null);
    }
    match cast_type {
        "string" | "str" => Some(Value::String(value_to_string(&v))),
        "int" | "integer" => {
            if let Some(n) = v.as_i64() {
                return Some(Value::Number(n.into()));
            }
            let s = value_to_string(&v);
            s.trim()
                .parse::<i64>()
                .ok()
                .map(|x| Value::Number(x.into()))
        }
        "float" | "double" | "number" => {
            if let Some(n) = v.as_f64() {
                return serde_json::Number::from_f64(n).map(Value::Number);
            }
            let s = value_to_string(&v).replace(',', "");
            s.trim()
                .parse::<f64>()
                .ok()
                .and_then(|x| serde_json::Number::from_f64(x).map(Value::Number))
        }
        "bool" | "boolean" => {
            if let Some(b) = v.as_bool() {
                return Some(Value::Bool(b));
            }
            let s = value_to_string(&v).to_lowercase();
            if matches!(s.as_str(), "1" | "true" | "yes" | "on") {
                Some(Value::Bool(true))
            } else if matches!(s.as_str(), "0" | "false" | "no" | "off") {
                Some(Value::Bool(false))
            } else {
                None
            }
        }
        _ => Some(v),
    }
}

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

pub(crate) fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "".to_string(),
        _ => v.to_string(),
    }
}

pub(crate) fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.replace(',', "").trim().parse::<f64>().ok(),
        _ => None,
    }
}

pub(crate) fn value_to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_u64().map(|x| x as i64)),
        Value::String(s) => s.replace(',', "").trim().parse::<i64>().ok(),
        _ => None,
    }
}

pub(crate) fn compile_filters(filters: &[Value]) -> Vec<CompiledFilter> {
    filters
        .iter()
        .map(|f| {
            let Some(obj) = f.as_object() else {
                return CompiledFilter {
                    field: String::new(),
                    op: FilterOp::Passthrough,
                };
            };
            let field = obj
                .get("field")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let op_name = obj
                .get("op")
                .and_then(|v| v.as_str())
                .unwrap_or("eq")
                .to_lowercase();
            let target = obj.get("value");
            let op = match op_name.as_str() {
                "exists" => FilterOp::Exists,
                "not_exists" => FilterOp::NotExists,
                "eq" => FilterOp::Eq(target.map(value_to_string).unwrap_or_default()),
                "ne" => FilterOp::Ne(target.map(value_to_string).unwrap_or_default()),
                "contains" => FilterOp::Contains(target.map(value_to_string).unwrap_or_default()),
                "in" => match target.and_then(|v| v.as_array()) {
                    Some(arr) => FilterOp::In(arr.iter().map(value_to_string).collect()),
                    None => FilterOp::Invalid,
                },
                "not_in" => match target.and_then(|v| v.as_array()) {
                    Some(arr) => FilterOp::NotIn(arr.iter().map(value_to_string).collect()),
                    None => FilterOp::Invalid,
                },
                "regex" => {
                    let pat = target.map(value_to_string).unwrap_or_default();
                    if pat.trim().is_empty() || pat.len() > 1024 {
                        FilterOp::Invalid
                    } else {
                        Regex::new(&pat)
                            .map(FilterOp::Regex)
                            .unwrap_or(FilterOp::Invalid)
                    }
                }
                "not_regex" => {
                    let pat = target.map(value_to_string).unwrap_or_default();
                    if pat.trim().is_empty() || pat.len() > 1024 {
                        FilterOp::Invalid
                    } else {
                        Regex::new(&pat)
                            .map(FilterOp::NotRegex)
                            .unwrap_or(FilterOp::Invalid)
                    }
                }
                "gt" => target
                    .and_then(value_to_f64)
                    .map(FilterOp::Gt)
                    .unwrap_or(FilterOp::Invalid),
                "gte" => target
                    .and_then(value_to_f64)
                    .map(FilterOp::Gte)
                    .unwrap_or(FilterOp::Invalid),
                "lt" => target
                    .and_then(value_to_f64)
                    .map(FilterOp::Lt)
                    .unwrap_or(FilterOp::Invalid),
                "lte" => target
                    .and_then(value_to_f64)
                    .map(FilterOp::Lte)
                    .unwrap_or(FilterOp::Invalid),
                _ => FilterOp::Passthrough,
            };
            CompiledFilter { field, op }
        })
        .collect()
}

pub(crate) fn filter_match_compiled(row: &Map<String, Value>, f: &CompiledFilter) -> bool {
    let val = row.get(&f.field);
    match &f.op {
        FilterOp::Exists => !is_missing(val),
        FilterOp::NotExists => is_missing(val),
        FilterOp::Eq(t) => value_to_string_or_null(val) == *t,
        FilterOp::Ne(t) => value_to_string_or_null(val) != *t,
        FilterOp::Contains(t) => value_to_string_or_null(val).contains(t),
        FilterOp::In(arr) => {
            let cur = value_to_string_or_null(val);
            arr.iter().any(|x| x == &cur)
        }
        FilterOp::NotIn(arr) => {
            let cur = value_to_string_or_null(val);
            arr.iter().all(|x| x != &cur)
        }
        FilterOp::Regex(re) => re.is_match(&value_to_string_or_null(val)),
        FilterOp::NotRegex(re) => !re.is_match(&value_to_string_or_null(val)),
        FilterOp::Gt(y) => val.and_then(value_to_f64).is_some_and(|x| x > *y),
        FilterOp::Gte(y) => val.and_then(value_to_f64).is_some_and(|x| x >= *y),
        FilterOp::Lt(y) => val.and_then(value_to_f64).is_some_and(|x| x < *y),
        FilterOp::Lte(y) => val.and_then(value_to_f64).is_some_and(|x| x <= *y),
        FilterOp::Invalid => false,
        FilterOp::Passthrough => true,
    }
}

pub(crate) fn value_to_string_or_null(v: Option<&Value>) -> String {
    v.map(value_to_string).unwrap_or_default()
}

pub(crate) fn dedup_key(row: &Map<String, Value>, fields: &[String]) -> String {
    fields
        .iter()
        .map(|f| value_to_string_or_null(row.get(f)))
        .collect::<Vec<String>>()
        .join("|")
}

pub(crate) fn compare_rows(
    a: &Map<String, Value>,
    b: &Map<String, Value>,
    sort_by: &[Value],
) -> std::cmp::Ordering {
    for item in sort_by {
        match item {
            Value::String(field) => {
                let av = value_to_string_or_null(a.get(field));
                let bv = value_to_string_or_null(b.get(field));
                let ord = av.cmp(&bv);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            Value::Object(obj) => {
                let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
                let desc = obj
                    .get("order")
                    .and_then(|v| v.as_str())
                    .unwrap_or("asc")
                    .eq_ignore_ascii_case("desc");
                let av = value_to_string_or_null(a.get(field));
                let bv = value_to_string_or_null(b.get(field));
                let mut ord = av.cmp(&bv);
                if desc {
                    ord = ord.reverse();
                }
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            _ => {}
        }
    }
    std::cmp::Ordering::Equal
}
