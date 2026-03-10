use super::*;

pub(super) fn execute_transform_rows(
    prepared: &TransformPrepared,
    cancel_flag: &Option<Arc<AtomicBool>>,
) -> Result<TransformExecution, String> {
    let mut invalid_rows = 0usize;
    let mut filtered_rows = 0usize;
    let mut rule_hits: HashMap<String, usize> = HashMap::new();
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    let mut prepared_rows: Vec<Map<String, Value>> = Vec::new();

    for row in prepared.rows_in.clone() {
        if is_cancelled(cancel_flag) {
            return Err("task cancelled".to_string());
        }
        let Some(obj) = row.as_object() else {
            invalid_rows += 1;
            *rule_hits.entry("invalid_object".to_string()).or_insert(0) += 1;
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
                match cast_value(value, cast_type) {
                    Some(casted) => {
                        out.insert(field.clone(), casted);
                    }
                    None => {
                        invalid_rows += 1;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
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
                continue;
            }
        }

        if !prepared.compiled_filters.is_empty()
            && !prepared
                .compiled_filters
                .iter()
                .all(|filter| filter_match_compiled(&out, filter))
        {
            filtered_rows += 1;
            *rule_hits.entry("filtered_by_rule".to_string()).or_insert(0) += 1;
            continue;
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
    apply_string_and_date_ops(&mut rows, &prepared.rules, &mut rule_hits);

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
        if !prepared.deduplicate_by.is_empty() {
            let mut deduped: HashMap<String, Map<String, Value>> = HashMap::new();
            if prepared.dedup_keep == "first" {
                for row in rows {
                    let key = dedup_key(&row, &prepared.deduplicate_by);
                    deduped.entry(key).or_insert(row);
                }
            } else {
                for row in rows {
                    let key = dedup_key(&row, &prepared.deduplicate_by);
                    deduped.insert(key, row);
                }
            }
            let out_len = deduped.len();
            duplicate_rows_removed = prepared
                .input_rows
                .saturating_sub(invalid_rows + filtered_rows + out_len);
            rows = deduped.into_values().collect();
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
        rule_hits,
    })
}
