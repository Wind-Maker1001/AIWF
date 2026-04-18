use super::*;

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

pub(crate) fn detect_numeric_sign(text: &str) -> f64 {
    let trimmed = text.trim();
    let normalized = trimmed.to_lowercase();
    if (trimmed.starts_with('(') && trimmed.ends_with(')'))
        || (trimmed.starts_with('（') && trimmed.ends_with('）'))
        || trimmed.ends_with('-')
        || trimmed.starts_with('-')
        || normalized.starts_with("debit")
        || normalized.starts_with("dr")
        || normalized.contains('借')
        || normalized.contains("支出")
        || normalized.contains("付款")
    {
        -1.0
    } else {
        1.0
    }
}

pub(crate) fn detect_numeric_multiplier(text: &str) -> f64 {
    let normalized = text.trim().to_lowercase();
    if normalized.contains("billion") || normalized.contains('亿') {
        100_000_000.0
    } else if normalized.contains("million") {
        1_000_000.0
    } else if normalized.contains("thousand") || normalized.contains('千') {
        1_000.0
    } else if normalized.contains('万') {
        10_000.0
    } else {
        1.0
    }
}

pub(crate) fn parse_finance_number_text(text: &str) -> Option<f64> {
    let normalized = text
        .trim()
        .replace('\u{3000}', " ")
        .replace(['\u{2010}', '\u{2011}', '\u{2013}', '\u{2014}', '\u{2212}'], "-");
    if normalized.trim().is_empty() {
        return None;
    }
    let sign = detect_numeric_sign(&normalized);
    let multiplier = detect_numeric_multiplier(&normalized);
    let mut compact = normalized
        .to_lowercase()
        .trim_matches(|ch| matches!(ch, '(' | ')' | '（' | '）'))
        .trim_end_matches('-')
        .replace(',', "")
        .replace('，', "")
        .replace('$', "")
        .replace('¥', "")
        .replace('￥', "")
        .replace('€', "")
        .replace('£', "")
        .replace("人民币", "")
        .replace("usd", "")
        .replace("cny", "")
        .replace("rmb", "")
        .replace("eur", "")
        .replace("jpy", "")
        .replace("gbp", "")
        .replace("hkd", "")
        .replace("元", "")
        .replace("圆", "")
        .replace("亿元", "")
        .replace("万元", "")
        .replace("千元", "")
        .replace("亿", "")
        .replace("万", "")
        .replace("千", "")
        .replace("借方", "")
        .replace("贷方", "")
        .replace("借", "")
        .replace("贷", "")
        .replace("支出", "")
        .replace("收入", "")
        .replace("付款", "")
        .replace("收款", "")
        .replace("debit", "")
        .replace("credit", "")
        .replace("dr", "")
        .replace("cr", "")
        .replace(' ', "");
    compact = compact.trim_start_matches('+').trim_start_matches('-').to_string();
    if compact.is_empty() {
        return None;
    }
    compact
        .parse::<f64>()
        .ok()
        .map(|value| value * multiplier * sign)
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
            parse_finance_number_text(&s)
                .map(|value| Value::Number((value as i64).into()))
        }
        "float" | "double" | "number" => {
            if let Some(n) = v.as_f64() {
                return serde_json::Number::from_f64(n).map(Value::Number);
            }
            let s = value_to_string(&v);
            parse_finance_number_text(&s)
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
        Value::String(s) => parse_finance_number_text(s),
        _ => None,
    }
}

pub(crate) fn value_to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_u64().map(|x| x as i64)),
        Value::String(s) => parse_finance_number_text(s).map(|x| x as i64),
        _ => None,
    }
}
