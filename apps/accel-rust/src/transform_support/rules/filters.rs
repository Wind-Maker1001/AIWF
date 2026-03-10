use super::*;

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
