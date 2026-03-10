use super::*;

pub(crate) fn apply_dedup_sort_columnar_v1(
    mut rows: Vec<Map<String, Value>>,
    deduplicate_by: &[String],
    dedup_keep: &str,
    sort_by: &[Value],
) -> (Vec<Map<String, Value>>, usize) {
    if rows.is_empty() {
        return (rows, 0);
    }
    let mut indices: Vec<usize> = (0..rows.len()).collect();
    let mut duplicate_rows_removed = 0usize;
    if !deduplicate_by.is_empty() {
        let mut key_keep_idx: HashMap<String, usize> = HashMap::new();
        for idx in &indices {
            let key = dedup_key(&rows[*idx], deduplicate_by);
            if dedup_keep == "first" {
                key_keep_idx.entry(key).or_insert(*idx);
            } else {
                key_keep_idx.insert(key, *idx);
            }
        }
        let before = indices.len();
        indices = key_keep_idx.into_values().collect();
        duplicate_rows_removed = before.saturating_sub(indices.len());
    }
    if !sort_by.is_empty() {
        // For single-key sort, comparator sort is generally faster than
        // building Arrow sort columns and index remapping.
        if sort_by.len() == 1 {
            indices.sort_by(|a, b| compare_rows(&rows[*a], &rows[*b], sort_by));
        } else {
            let mut sort_cols: Vec<SortColumn> = Vec::new();
            for s in sort_by {
                let (field, desc) = match s {
                    Value::String(name) => (name.clone(), false),
                    Value::Object(obj) => (
                        obj.get("field")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        obj.get("order")
                            .and_then(|v| v.as_str())
                            .unwrap_or("asc")
                            .eq_ignore_ascii_case("desc"),
                    ),
                    _ => (String::new(), false),
                };
                if field.is_empty() {
                    continue;
                }
                let mut as_num = true;
                let mut fvals: Vec<Option<f64>> = Vec::with_capacity(indices.len());
                for idx in &indices {
                    match rows[*idx].get(&field).and_then(value_to_f64) {
                        Some(v) => fvals.push(Some(v)),
                        None => {
                            fvals.push(None);
                            if !is_missing(rows[*idx].get(&field)) {
                                as_num = false;
                            }
                        }
                    }
                }
                if as_num {
                    let arr = Float64Array::from(fvals);
                    sort_cols.push(SortColumn {
                        values: Arc::new(arr) as ArrayRef,
                        options: Some(SortOptions {
                            descending: desc,
                            nulls_first: false,
                        }),
                    });
                } else {
                    let svals: Vec<Option<String>> = indices
                        .iter()
                        .map(|idx| value_to_arrow_string(rows[*idx].get(&field)))
                        .collect();
                    let arr = StringArray::from(svals);
                    sort_cols.push(SortColumn {
                        values: Arc::new(arr) as ArrayRef,
                        options: Some(SortOptions {
                            descending: desc,
                            nulls_first: false,
                        }),
                    });
                }
            }
            if !sort_cols.is_empty() {
                if let Ok(order) = lexsort_to_indices(&sort_cols, None) {
                    let mut next: Vec<usize> = Vec::with_capacity(indices.len());
                    for i in 0..order.len() {
                        let pos = order.value(i) as usize;
                        if let Some(v) = indices.get(pos) {
                            next.push(*v);
                        }
                    }
                    if next.len() == indices.len() {
                        indices = next;
                    } else {
                        indices.sort_by(|a, b| compare_rows(&rows[*a], &rows[*b], sort_by));
                    }
                } else {
                    indices.sort_by(|a, b| compare_rows(&rows[*a], &rows[*b], sort_by));
                }
            } else {
                indices.sort_by(|a, b| compare_rows(&rows[*a], &rows[*b], sort_by));
            }
        }
    }
    let mut out: Vec<Map<String, Value>> = Vec::with_capacity(indices.len());
    for idx in indices {
        out.push(std::mem::take(&mut rows[idx]));
    }
    (out, duplicate_rows_removed)
}
