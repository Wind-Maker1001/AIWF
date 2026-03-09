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
