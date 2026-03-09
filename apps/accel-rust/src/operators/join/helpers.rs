use super::*;

pub(crate) fn parse_join_keys(v: &Value) -> Result<Vec<String>, String> {
    if let Some(s) = v.as_str() {
        let t = s.trim();
        if t.is_empty() {
            return Err("join key is empty".to_string());
        }
        return Ok(vec![t.to_string()]);
    }
    if let Some(arr) = v.as_array() {
        let out = arr
            .iter()
            .filter_map(|x| x.as_str().map(|s| s.trim().to_string()))
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        if out.is_empty() {
            return Err("join keys are empty".to_string());
        }
        return Ok(out);
    }
    Err("join keys must be string or array".to_string())
}

pub(crate) fn join_key_multi(obj: &Map<String, Value>, keys: &[String]) -> String {
    keys.iter()
        .map(|k| value_to_string_or_null(obj.get(k)))
        .collect::<Vec<_>>()
        .join("\u{1f}")
}
