use super::*;

const SUBTOTAL_KEYWORDS: &[&str] = &[
    "subtotal",
    "sub total",
    "total",
    "grand total",
    "合计",
    "小计",
    "本页合计",
    "累计",
    "汇总",
];
const NOTE_KEYWORDS: &[&str] = &["note", "notes", "备注", "说明", "注", "附注"];
const HEADER_REPEAT_HINTS: &[&str] = &[
    "id",
    "amount",
    "currency",
    "biz_date",
    "account_no",
    "txn_date",
    "debit_amount",
    "credit_amount",
    "balance",
    "counterparty_name",
    "remark",
    "ref_no",
    "txn_type",
    "customer_name",
    "phone",
    "city",
];

fn row_text_values(row: &Map<String, Value>) -> Vec<String> {
    row.values()
        .filter(|value| !is_missing(Some(value)))
        .map(value_to_string)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect()
}

fn first_non_missing_text(row: &Map<String, Value>) -> String {
    row.values()
        .find(|value| !is_missing(Some(value)))
        .map(value_to_string)
        .map(|value| value.trim().to_string())
        .unwrap_or_default()
}

fn contains_keyword(text: &str, keywords: &[String]) -> bool {
    let lowered = text.trim().to_lowercase();
    !lowered.is_empty() && keywords.iter().any(|keyword| lowered.contains(keyword))
}

fn looks_like_blank_row(row: &Map<String, Value>) -> bool {
    if row.is_empty() {
        return true;
    }
    for value in row.values() {
        if is_missing(Some(value)) {
            continue;
        }
        let text = value_to_string(value);
        let stripped = text
            .chars()
            .filter(|ch| ch.is_alphanumeric())
            .collect::<String>();
        if !stripped.is_empty() {
            return false;
        }
    }
    true
}

fn looks_like_subtotal_row(row: &Map<String, Value>, keywords: &[String]) -> bool {
    row_text_values(row)
        .iter()
        .any(|value| contains_keyword(value, keywords))
}

fn looks_like_note_row(row: &Map<String, Value>, keywords: &[String]) -> bool {
    let first = first_non_missing_text(row);
    !first.is_empty() && contains_keyword(&first, keywords)
}

fn normalize_header_repeat_text(text: &str) -> String {
    let replaced = Regex::new(r"[\s\-/]+")
        .ok()
        .map(|re| re.replace_all(text.trim().to_lowercase().as_str(), "_").to_string())
        .unwrap_or_else(|| text.trim().to_lowercase());
    Regex::new(r"[^0-9a-z_\u4e00-\u9fff]+")
        .ok()
        .map(|re| re.replace_all(&replaced, "").trim_matches('_').to_string())
        .unwrap_or(replaced)
}

fn looks_like_header_repeat_row(
    row: &Map<String, Value>,
    header_values: &[String],
    min_matches: usize,
) -> bool {
    let mut matched = 0usize;
    for value in row_text_values(row) {
        let normalized = normalize_header_repeat_text(&value);
        if header_values.iter().any(|item| item == &normalized) {
            matched += 1;
        }
    }
    matched >= min_matches.max(1)
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
                "blank_row" => FilterOp::BlankRow,
                "subtotal_row" => {
                    let keywords = obj
                        .get("keywords")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .map(value_to_string)
                                .map(|item| item.trim().to_lowercase())
                                .filter(|item| !item.is_empty())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_else(|| SUBTOTAL_KEYWORDS.iter().map(|item| item.to_string()).collect());
                    FilterOp::SubtotalRow(keywords)
                }
                "header_repeat_row" => {
                    let header_values = obj
                        .get("header_values")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .map(value_to_string)
                                .map(|item| normalize_header_repeat_text(&item))
                                .filter(|item| !item.is_empty())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_else(|| {
                            HEADER_REPEAT_HINTS
                                .iter()
                                .map(|item| item.to_string())
                                .collect::<Vec<_>>()
                        });
                    let min_matches = obj
                        .get("min_matches")
                        .and_then(|v| v.as_u64())
                        .map(|value| value.max(1) as usize)
                        .unwrap_or(2);
                    FilterOp::HeaderRepeatRow {
                        header_values,
                        min_matches,
                    }
                }
                "note_row" => {
                    let keywords = obj
                        .get("keywords")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .map(value_to_string)
                                .map(|item| item.trim().to_lowercase())
                                .filter(|item| !item.is_empty())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_else(|| NOTE_KEYWORDS.iter().map(|item| item.to_string()).collect());
                    FilterOp::NoteRow(keywords)
                }
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
        FilterOp::BlankRow => !looks_like_blank_row(row),
        FilterOp::SubtotalRow(keywords) => !looks_like_subtotal_row(row, keywords),
        FilterOp::HeaderRepeatRow {
            header_values,
            min_matches,
        } => !looks_like_header_repeat_row(row, header_values, *min_matches),
        FilterOp::NoteRow(keywords) => !looks_like_note_row(row, keywords),
        FilterOp::Invalid => false,
        FilterOp::Passthrough => true,
    }
}
