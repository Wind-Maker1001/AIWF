use super::*;

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
