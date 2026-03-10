use super::*;

pub(crate) fn apply_transform_columnar_v1(
    mut rows: Vec<Map<String, Value>>,
    casts: &HashMap<String, String>,
    required_fields: &[String],
    compiled_filters: &[CompiledFilter],
    rule_hits: &mut HashMap<String, usize>,
) -> (Vec<Map<String, Value>>, usize, usize) {
    if rows.is_empty() {
        return (rows, 0, 0);
    }
    let n = rows.len();
    let mut invalid = vec![false; n];
    let mut filtered = vec![false; n];

    for (field, cast_type) in casts {
        for i in 0..n {
            if invalid[i] {
                continue;
            }
            if let Some(slot) = rows[i].get_mut(field) {
                let raw = std::mem::take(slot);
                match cast_value(raw, cast_type) {
                    Some(casted) => {
                        *slot = casted;
                    }
                    None => {
                        invalid[i] = true;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                    }
                }
            }
        }
    }

    if !required_fields.is_empty() {
        for i in 0..n {
            if invalid[i] {
                continue;
            }
            let mut missing = false;
            for f in required_fields {
                if is_missing(rows[i].get(f)) {
                    missing = true;
                    break;
                }
            }
            if missing {
                invalid[i] = true;
                *rule_hits.entry("required_missing".to_string()).or_insert(0) += 1;
            }
        }
    }

    if !compiled_filters.is_empty() {
        for i in 0..n {
            if invalid[i] {
                continue;
            }
            if !compiled_filters
                .iter()
                .all(|f| filter_match_compiled(&rows[i], f))
            {
                filtered[i] = true;
                *rule_hits.entry("filtered_by_rule".to_string()).or_insert(0) += 1;
            }
        }
    }

    let mut out = Vec::with_capacity(n);
    let mut invalid_rows = 0usize;
    let mut filtered_rows = 0usize;
    for i in 0..n {
        if invalid[i] {
            invalid_rows += 1;
            continue;
        }
        if filtered[i] {
            filtered_rows += 1;
            continue;
        }
        out.push(std::mem::take(&mut rows[i]));
    }
    (out, invalid_rows, filtered_rows)
}
