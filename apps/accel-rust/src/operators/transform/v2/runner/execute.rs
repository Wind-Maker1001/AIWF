use super::*;
use crate::FilterOp;
use std::cmp::Ordering;

const INTERNAL_ROW_INDEX_FIELD: &str = "__aiwf_row_index";

fn reason_sample_keys() -> [&'static str; 5] {
    [
        "invalid_object",
        "cast_failed",
        "required_missing",
        "filter_rejected",
        "duplicate_removed",
    ]
}

fn empty_reason_samples() -> HashMap<String, Vec<Value>> {
    let mut out = HashMap::new();
    for key in reason_sample_keys() {
        out.insert(key.to_string(), Vec::new());
    }
    out
}

fn row_index_value(row: &Map<String, Value>) -> usize {
    row.get(INTERNAL_ROW_INDEX_FIELD)
        .and_then(|value| value.as_u64())
        .map(|value| value as usize)
        .or_else(|| {
            row.get(INTERNAL_ROW_INDEX_FIELD)
                .and_then(|value| value.as_i64())
                .map(|value| value.max(0) as usize)
        })
        .unwrap_or(0)
}

fn row_excerpt(row: &Map<String, Value>) -> Value {
    let mut clean = row.clone();
    clean.remove(INTERNAL_ROW_INDEX_FIELD);
    Value::Object(clean)
}

fn key_values(row: &Map<String, Value>, fields: &[String]) -> Value {
    Value::Array(
        fields
            .iter()
            .map(|field| row.get(field).cloned().unwrap_or(Value::Null))
            .collect(),
    )
}

fn push_reason_sample(
    reason_samples: &mut HashMap<String, Vec<Value>>,
    reason_code: &str,
    sample_limit: usize,
    sample: Value,
) {
    let items = reason_samples.entry(reason_code.to_string()).or_default();
    if items.len() < sample_limit {
        items.push(sample);
    }
}

fn filter_detail(filter: &CompiledFilter) -> String {
    let field = filter.field.trim();
    let op = match &filter.op {
        FilterOp::Exists => "exists",
        FilterOp::NotExists => "not_exists",
        FilterOp::Eq(_) => "eq",
        FilterOp::Ne(_) => "ne",
        FilterOp::Contains(_) => "contains",
        FilterOp::In(_) => "in",
        FilterOp::NotIn(_) => "not_in",
        FilterOp::Regex(_) => "regex",
        FilterOp::NotRegex(_) => "not_regex",
        FilterOp::Gt(_) => "gt",
        FilterOp::Gte(_) => "gte",
        FilterOp::Lt(_) => "lt",
        FilterOp::Lte(_) => "lte",
        FilterOp::BlankRow => "blank_row",
        FilterOp::SubtotalRow(_) => "subtotal_row",
        FilterOp::HeaderRepeatRow { .. } => "header_repeat_row",
        FilterOp::NoteRow(_) => "note_row",
        FilterOp::Invalid => "invalid",
        FilterOp::Passthrough => "passthrough",
    };
    if field.is_empty() {
        op.to_string()
    } else {
        format!("{field}:{op}")
    }
}

fn null_or_string(value: Option<&str>) -> Value {
    match value {
        Some(text) if !text.trim().is_empty() => Value::String(text.to_string()),
        _ => Value::Null,
    }
}

#[derive(Clone)]
enum SurvivorshipValue {
    Missing,
    Text(String),
    Number(f64),
    Date(i64),
}

impl SurvivorshipValue {
    fn rank(&self) -> i32 {
        match self {
            SurvivorshipValue::Missing => 0,
            SurvivorshipValue::Text(_) => 1,
            SurvivorshipValue::Number(_) => 2,
            SurvivorshipValue::Date(_) => 3,
        }
    }
}

fn survivorship_value(value: Option<&Value>) -> SurvivorshipValue {
    if is_missing(value) {
        return SurvivorshipValue::Missing;
    }
    if let Some(text) = value.map(value_to_string)
        && let Some((year, month, day)) = parse_ymd_simple(&text)
    {
        return SurvivorshipValue::Date(year * 10_000 + month * 100 + day);
    }
    if let Some(number) = value.and_then(value_to_f64) {
        return SurvivorshipValue::Number(number);
    }
    SurvivorshipValue::Text(value.map(value_to_string).unwrap_or_default())
}

fn compare_survivorship_values(left: Option<&Value>, right: Option<&Value>) -> Ordering {
    let left_value = survivorship_value(left);
    let right_value = survivorship_value(right);
    let rank_order = left_value.rank().cmp(&right_value.rank());
    if rank_order != Ordering::Equal {
        return rank_order;
    }
    match (left_value, right_value) {
        (SurvivorshipValue::Missing, SurvivorshipValue::Missing) => Ordering::Equal,
        (SurvivorshipValue::Text(a), SurvivorshipValue::Text(b)) => a.cmp(&b),
        (SurvivorshipValue::Number(a), SurvivorshipValue::Number(b)) => {
            a.partial_cmp(&b).unwrap_or(Ordering::Equal)
        }
        (SurvivorshipValue::Date(a), SurvivorshipValue::Date(b)) => a.cmp(&b),
        _ => Ordering::Equal,
    }
}

fn survivorship_fields(survivorship: &Value, key: &str) -> Vec<String> {
    survivorship
        .as_object()
        .and_then(|obj| obj.get(key))
        .map(|value| as_array_str(Some(value)))
        .unwrap_or_default()
}

fn choose_survivor(
    winner: &Map<String, Value>,
    candidate: &Map<String, Value>,
    survivorship: &Value,
    tie_breaker: &str,
) -> (bool, Vec<String>) {
    let mut decision_basis: Vec<String> = Vec::new();

    for field in survivorship_fields(survivorship, "score_fields") {
        match compare_survivorship_values(candidate.get(&field), winner.get(&field)) {
            Ordering::Greater => return (true, vec![format!("score_fields:{field}")]),
            Ordering::Less => return (false, vec![format!("score_fields:{field}")]),
            Ordering::Equal => {
                if !matches!(survivorship_value(candidate.get(&field)), SurvivorshipValue::Missing) {
                    decision_basis.push(format!("score_fields:{field}=tie"));
                }
            }
        }
    }

    for field in survivorship_fields(survivorship, "prefer_non_null_fields") {
        let winner_has = !is_missing(winner.get(&field));
        let candidate_has = !is_missing(candidate.get(&field));
        if candidate_has && !winner_has {
            return (true, vec![format!("prefer_non_null_fields:{field}")]);
        }
        if winner_has && !candidate_has {
            return (false, vec![format!("prefer_non_null_fields:{field}")]);
        }
    }

    for field in survivorship_fields(survivorship, "prefer_latest_fields") {
        match compare_survivorship_values(candidate.get(&field), winner.get(&field)) {
            Ordering::Greater => return (true, vec![format!("prefer_latest_fields:{field}")]),
            Ordering::Less => return (false, vec![format!("prefer_latest_fields:{field}")]),
            Ordering::Equal => {
                if !matches!(survivorship_value(candidate.get(&field)), SurvivorshipValue::Missing) {
                    decision_basis.push(format!("prefer_latest_fields:{field}=tie"));
                }
            }
        }
    }

    let winner_index = row_index_value(winner);
    let candidate_index = row_index_value(candidate);
    let candidate_wins = if tie_breaker == "first" {
        candidate_index < winner_index
    } else {
        candidate_index >= winner_index
    };
    if decision_basis.is_empty() {
        decision_basis.push(format!("tie_breaker:{tie_breaker}"));
    }
    (candidate_wins, decision_basis)
}

pub(super) fn execute_transform_rows(
    prepared: &TransformPrepared,
    cancel_flag: &Option<Arc<AtomicBool>>,
) -> Result<TransformExecution, String> {
    let mut invalid_rows = 0usize;
    let mut filtered_rows = 0usize;
    let mut rule_hits: HashMap<String, usize> = HashMap::new();
    let mut reason_samples = empty_reason_samples();
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    let mut prepared_rows: Vec<Map<String, Value>> = Vec::new();
    let mut numeric_cells_total = 0usize;
    let mut numeric_cells_parsed = 0usize;

    for (row_index, row) in prepared.rows_in.clone().into_iter().enumerate() {
        if is_cancelled(cancel_flag) {
            return Err("task cancelled".to_string());
        }
        let Some(obj) = row.as_object() else {
            invalid_rows += 1;
            *rule_hits.entry("invalid_object".to_string()).or_insert(0) += 1;
            push_reason_sample(
                &mut reason_samples,
                "invalid_object",
                prepared.audit_sample_limit,
                json!({
                    "reason_code": "invalid_object",
                    "row_index": row_index + 1,
                    "field": Value::Null,
                    "key": Value::Null,
                    "raw_row": row,
                    "detail": "row is not an object"
                }),
            );
            continue;
        };
        let mut out: Map<String, Value> = Map::new();
        for (key, value) in obj {
            let renamed = prepared
                .rename_map
                .get(key)
                .cloned()
                .unwrap_or_else(|| key.clone());
            let mut next_value = value.clone();
            if let Some(s) = next_value.as_str() {
                let mut normalized = s.to_string();
                if prepared.trim_strings {
                    normalized = normalized.trim().to_string();
                }
                if prepared
                    .null_values
                    .iter()
                    .any(|candidate| candidate == &normalized.to_lowercase())
                {
                    next_value = Value::Null;
                } else {
                    next_value = Value::String(normalized);
                }
            }
            out.insert(renamed, next_value);
        }

        for (key, value) in &prepared.default_values {
            let should_fill = match out.get(key) {
                None => true,
                Some(Value::Null) => true,
                Some(Value::String(s)) => s.trim().is_empty(),
                Some(_) => false,
            };
            if should_fill {
                out.insert(key.clone(), value.clone());
            }
        }

        if prepared.use_columnar {
            prepared_rows.push(out);
            continue;
        }

        for (field, cast_type) in &prepared.casts {
            if let Some(value) = out.get(field).cloned() {
                let normalized_cast = cast_type.trim().to_ascii_lowercase();
                let is_numeric_cast = matches!(
                    normalized_cast.as_str(),
                    "int" | "integer" | "float" | "double" | "number" | "decimal"
                );
                if is_numeric_cast && !is_missing(Some(&value)) {
                    numeric_cells_total += 1;
                }
                match cast_value(value, cast_type) {
                    Some(casted) => {
                        if is_numeric_cast {
                            numeric_cells_parsed += 1;
                        }
                        out.insert(field.clone(), casted);
                    }
                    None => {
                        invalid_rows += 1;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                        push_reason_sample(
                            &mut reason_samples,
                            "cast_failed",
                            prepared.audit_sample_limit,
                            json!({
                                "reason_code": "cast_failed",
                                "row_index": row_index + 1,
                                "field": field,
                                "key": Value::Null,
                                "raw_row": row,
                                "detail": format!("cast failed for {field} as {cast_type}")
                            }),
                        );
                        out.clear();
                        break;
                    }
                }
            }
        }
        if out.is_empty() {
            continue;
        }

        if !prepared.required_fields.is_empty() {
            let mut missing = false;
            for field in &prepared.required_fields {
                if is_missing(out.get(field)) {
                    missing = true;
                    break;
                }
            }
            if missing {
                invalid_rows += 1;
                *rule_hits.entry("required_missing".to_string()).or_insert(0) += 1;
                let missing_fields = prepared
                    .required_fields
                    .iter()
                    .filter(|field| is_missing(out.get(field.as_str())))
                    .cloned()
                    .collect::<Vec<String>>();
                push_reason_sample(
                    &mut reason_samples,
                    "required_missing",
                    prepared.audit_sample_limit,
                    json!({
                        "reason_code": "required_missing",
                        "row_index": row_index + 1,
                        "field": null_or_string(missing_fields.first().map(|s| s.as_str())),
                        "key": Value::Null,
                        "row_excerpt": row_excerpt(&out),
                        "detail": format!("missing required fields: {}", missing_fields.join(","))
                    }),
                );
                continue;
            }
        }

        if !prepared.compiled_filters.is_empty() {
            let failed_filter = prepared
                .compiled_filters
                .iter()
                .find(|filter| !filter_match_compiled(&out, filter));
            if let Some(filter) = failed_filter {
                filtered_rows += 1;
                *rule_hits.entry("filtered_by_rule".to_string()).or_insert(0) += 1;
                push_reason_sample(
                    &mut reason_samples,
                    "filter_rejected",
                    prepared.audit_sample_limit,
                    json!({
                        "reason_code": "filter_rejected",
                        "row_index": row_index + 1,
                        "field": null_or_string(if filter.field.trim().is_empty() { None } else { Some(filter.field.as_str()) }),
                        "key": Value::Null,
                        "row_excerpt": row_excerpt(&out),
                        "detail": filter_detail(filter)
                    }),
                );
                continue;
            }
        }

        if !prepared.include_fields.is_empty() {
            let mut next: Map<String, Value> = Map::new();
            for field in &prepared.include_fields {
                if let Some(value) = out.get(field) {
                    next.insert(field.clone(), value.clone());
                }
            }
            out = next;
        }
        for field in &prepared.exclude_fields {
            out.remove(field);
        }
        out.insert(
            INTERNAL_ROW_INDEX_FIELD.to_string(),
            Value::Number(((row_index + 1) as u64).into()),
        );
        rows.push(out);
    }

    if prepared.use_columnar {
        let (mut out_rows, bad_rows, f_rows) = if prepared.engine == "columnar_arrow_v1" {
            apply_transform_columnar_arrow_v1(
                prepared_rows,
                &prepared.casts,
                &prepared.required_fields,
                &prepared.compiled_filters,
                &mut rule_hits,
            )
        } else {
            apply_transform_columnar_v1(
                prepared_rows,
                &prepared.casts,
                &prepared.required_fields,
                &prepared.compiled_filters,
                &mut rule_hits,
            )
        };
        invalid_rows += bad_rows;
        filtered_rows += f_rows;
        if !prepared.include_fields.is_empty() {
            for row in &mut out_rows {
                let mut next: Map<String, Value> = Map::new();
                for field in &prepared.include_fields {
                    if let Some(value) = row.get(field) {
                        next.insert(field.clone(), value.clone());
                    }
                }
                *row = next;
            }
        }
        if !prepared.exclude_fields.is_empty() {
            for row in &mut out_rows {
                for field in &prepared.exclude_fields {
                    row.remove(field);
                }
            }
        }
        rows = out_rows;
    }

    apply_expression_fields(&mut rows, &prepared.rules, &mut rule_hits);

    let date_ops = rule_get(&prepared.rules, "date_ops")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut date_cells_total = 0usize;
    for row in &rows {
        for op in &date_ops {
            let Some(obj) = op.as_object() else { continue };
            let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
            if field.is_empty() {
                continue;
            }
            if !is_missing(row.get(field)) {
                date_cells_total += 1;
            }
        }
    }
    apply_string_and_date_ops(&mut rows, &prepared.rules, &mut rule_hits);
    let mut date_cells_parsed = 0usize;
    for row in &rows {
        for op in &date_ops {
            let Some(obj) = op.as_object() else { continue };
            let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
            let out_field = obj.get("as").and_then(|v| v.as_str()).unwrap_or(field);
            if out_field.is_empty() {
                continue;
            }
            if !is_missing(row.get(out_field)) {
                date_cells_parsed += 1;
            }
        }
    }

    let mut duplicate_rows_removed = 0usize;
    if prepared.use_columnar {
        let before = rows.len();
        let (out_rows, dup_removed) = apply_dedup_sort_columnar_v1(
            rows,
            &prepared.deduplicate_by,
            &prepared.dedup_keep,
            &prepared.sort_by,
        );
        rows = out_rows;
        duplicate_rows_removed = if !prepared.deduplicate_by.is_empty() {
            dup_removed
        } else {
            before.saturating_sub(rows.len())
        };
    } else {
        let has_survivorship = prepared
            .survivorship
            .as_object()
            .map(|obj| !obj.is_empty())
            .unwrap_or(false)
            && !prepared.survivorship_keys.is_empty();
        let dedup_key_fields = if has_survivorship {
            &prepared.survivorship_keys
        } else {
            &prepared.deduplicate_by
        };
        if !dedup_key_fields.is_empty() {
            let before = rows.len();
            if has_survivorship {
                let key_fields = dedup_key_fields;
                let tie_breaker = prepared
                    .survivorship
                    .as_object()
                    .and_then(|obj| obj.get("tie_breaker"))
                    .and_then(|value| value.as_str())
                    .unwrap_or(prepared.dedup_keep.as_str())
                    .to_ascii_lowercase();
                let mut deduped: HashMap<String, Map<String, Value>> = HashMap::new();
                for row in rows {
                    let key = dedup_key(&row, key_fields);
                    if let Some(previous) = deduped.get(&key).cloned() {
                        let (candidate_wins, decision_basis) =
                            choose_survivor(&previous, &row, &prepared.survivorship, &tie_breaker);
                        let (winner, loser) = if candidate_wins {
                            (row.clone(), previous.clone())
                        } else {
                            (previous.clone(), row.clone())
                        };
                        deduped.insert(key.clone(), winner.clone());
                        push_reason_sample(
                            &mut reason_samples,
                            "duplicate_removed",
                            prepared.audit_sample_limit,
                            json!({
                                "reason_code": "duplicate_removed",
                                "row_index": Value::Null,
                                "field": Value::Null,
                                "key": key_values(&winner, key_fields),
                                "deduplicate_keep": tie_breaker,
                                "winner_row_id": row_index_value(&winner),
                                "loser_row_id": row_index_value(&loser),
                                "winner_row": row_excerpt(&winner),
                                "loser_row": row_excerpt(&loser),
                                "decision_basis": decision_basis,
                                "detail": "duplicate removed via survivorship"
                            }),
                        );
                    } else {
                        deduped.insert(key, row);
                    }
                }
                rows = deduped.into_values().collect();
            } else {
                let key_fields = dedup_key_fields;
                let mut deduped: HashMap<String, Map<String, Value>> = HashMap::new();
                if prepared.dedup_keep == "first" {
                    for row in rows {
                        let key = dedup_key(&row, key_fields);
                        if let Some(previous) = deduped.get(&key).cloned() {
                            push_reason_sample(
                                &mut reason_samples,
                                "duplicate_removed",
                                prepared.audit_sample_limit,
                                json!({
                                    "reason_code": "duplicate_removed",
                                    "row_index": Value::Null,
                                    "field": Value::Null,
                                    "key": key_values(&row, key_fields),
                                    "deduplicate_keep": "first",
                                    "winner_row_id": row_index_value(&previous),
                                    "loser_row_id": row_index_value(&row),
                                    "winner_row": row_excerpt(&previous),
                                    "loser_row": row_excerpt(&row),
                                    "decision_basis": ["tie_breaker:first"],
                                    "detail": "duplicate removed while keeping first"
                                }),
                            );
                        } else {
                            deduped.insert(key, row);
                        }
                    }
                } else {
                    for row in rows {
                        let key = dedup_key(&row, key_fields);
                        if let Some(previous) = deduped.get(&key).cloned() {
                            push_reason_sample(
                                &mut reason_samples,
                                "duplicate_removed",
                                prepared.audit_sample_limit,
                                json!({
                                    "reason_code": "duplicate_removed",
                                    "row_index": Value::Null,
                                    "field": Value::Null,
                                    "key": key_values(&row, key_fields),
                                    "deduplicate_keep": "last",
                                    "winner_row_id": row_index_value(&row),
                                    "loser_row_id": row_index_value(&previous),
                                    "winner_row": row_excerpt(&row),
                                    "loser_row": row_excerpt(&previous),
                                    "decision_basis": ["tie_breaker:last"],
                                    "detail": "duplicate removed while keeping last"
                                }),
                            );
                        }
                        deduped.insert(key, row);
                    }
                }
                rows = deduped.into_values().collect();
            }
            duplicate_rows_removed = before.saturating_sub(rows.len());
        }

        if !prepared.sort_by.is_empty() {
            rows.sort_by(|a, b| compare_rows(a, b, &prepared.sort_by));
        }
    }

    Ok(TransformExecution {
        rows,
        invalid_rows,
        filtered_rows,
        duplicate_rows_removed,
        numeric_cells_total,
        numeric_cells_parsed,
        date_cells_total,
        date_cells_parsed,
        rule_hits,
        reason_samples,
    })
}
