use super::*;
use crate::{
    api_types::CompiledFilter, operators::transform::TransformRowsReq,
    transform_support::cast_value,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
};
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::Path,
    sync::Arc,
};

pub(crate) fn resolve_transform_engine(rules: &Value) -> String {
    if let Some(v) = rule_get(rules, "execution_engine").and_then(|x| x.as_str()) {
        let t = v.trim().to_ascii_lowercase();
        if !t.is_empty() {
            return t;
        }
    }
    env::var("AIWF_RUST_TRANSFORM_ENGINE")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "auto_v1".to_string())
}

pub(crate) fn estimate_rule_complexity(rules: &Value) -> usize {
    let casts = rule_get(rules, "casts")
        .and_then(|v| v.as_object())
        .map(|m| m.len())
        .unwrap_or(0);
    let filters = rule_get(rules, "filters")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let dedup = rule_get(rules, "deduplicate_by")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let sort = rule_get(rules, "sort_by")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let has_agg = if rule_get(rules, "aggregate").is_some() {
        2
    } else {
        0
    };
    casts + filters + dedup + sort + has_agg
}

#[derive(Clone)]
struct EngineProfile {
    medium_rows_threshold: usize,
    large_rows_threshold: usize,
    medium_complexity_threshold: usize,
    medium_bytes_threshold: usize,
    large_bytes_threshold: usize,
    row_cost_per_row: f64,
    columnar_cost_per_row: f64,
    arrow_cost_per_row: f64,
    complexity_weight: f64,
}

fn default_engine_profile() -> EngineProfile {
    EngineProfile {
        medium_rows_threshold: 20_000,
        large_rows_threshold: 120_000,
        medium_complexity_threshold: 8,
        medium_bytes_threshold: 12 * 1024 * 1024,
        large_bytes_threshold: 48 * 1024 * 1024,
        row_cost_per_row: 1.0,
        columnar_cost_per_row: 0.9,
        arrow_cost_per_row: 0.8,
        complexity_weight: 0.08,
    }
}

fn load_engine_profile() -> EngineProfile {
    let default = default_engine_profile();
    let path = env::var("AIWF_RUST_ENGINE_PROFILE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| {
            Path::new(".")
                .join("conf")
                .join("transform_engine_profile.json")
                .to_string_lossy()
                .to_string()
        });
    let txt = match fs::read_to_string(&path) {
        Ok(v) => v,
        Err(_) => return default,
    };
    let v: Value = match serde_json::from_str(&txt) {
        Ok(v) => v,
        Err(_) => return default,
    };
    let get_u = |k: &str, dv: usize| -> usize {
        v.get(k)
            .and_then(|x| x.as_u64())
            .map(|x| x as usize)
            .unwrap_or(dv)
    };
    let get_f = |k: &str, dv: f64| -> f64 { v.get(k).and_then(|x| x.as_f64()).unwrap_or(dv) };
    EngineProfile {
        medium_rows_threshold: get_u("medium_rows_threshold", default.medium_rows_threshold),
        large_rows_threshold: get_u("large_rows_threshold", default.large_rows_threshold),
        medium_complexity_threshold: get_u(
            "medium_complexity_threshold",
            default.medium_complexity_threshold,
        ),
        medium_bytes_threshold: get_u("medium_bytes_threshold", default.medium_bytes_threshold),
        large_bytes_threshold: get_u("large_bytes_threshold", default.large_bytes_threshold),
        row_cost_per_row: get_f("row_cost_per_row", default.row_cost_per_row),
        columnar_cost_per_row: get_f("columnar_cost_per_row", default.columnar_cost_per_row),
        arrow_cost_per_row: get_f("arrow_cost_per_row", default.arrow_cost_per_row),
        complexity_weight: get_f("complexity_weight", default.complexity_weight),
    }
}

pub(crate) fn auto_select_engine(
    input_rows: usize,
    estimated_bytes: usize,
    rules: &Value,
) -> (String, String) {
    let p = load_engine_profile();
    let complexity = estimate_rule_complexity(rules);
    let row_cost =
        p.row_cost_per_row * input_rows as f64 * (1.0 + p.complexity_weight * complexity as f64);
    let col_cost = p.columnar_cost_per_row
        * input_rows as f64
        * (1.0 + 0.6 * p.complexity_weight * complexity as f64);
    let arrow_cost = p.arrow_cost_per_row
        * input_rows as f64
        * (1.0 + 0.5 * p.complexity_weight * complexity as f64);
    if input_rows >= p.large_rows_threshold || estimated_bytes >= p.large_bytes_threshold {
        return (
            "columnar_arrow_v1".to_string(),
            format!(
                "auto: cost large rows={} bytes={} complexity={} row={:.2} col={:.2} arrow={:.2}",
                input_rows, estimated_bytes, complexity, row_cost, col_cost, arrow_cost
            ),
        );
    }
    if input_rows >= p.medium_rows_threshold
        || estimated_bytes >= p.medium_bytes_threshold
        || complexity >= p.medium_complexity_threshold
    {
        let (eng, best) = if col_cost <= row_cost {
            ("columnar_v1", col_cost)
        } else {
            ("row_v1", row_cost)
        };
        return (
            eng.to_string(),
            format!(
                "auto: cost medium rows={} bytes={} complexity={} row={:.2} col={:.2} choose={:.2}",
                input_rows, estimated_bytes, complexity, row_cost, col_cost, best
            ),
        );
    }
    (
        "row_v1".to_string(),
        format!(
            "auto: cost small rows={} complexity={} row={:.2} col={:.2}",
            input_rows, complexity, row_cost, col_cost
        ),
    )
}

pub(crate) fn request_prefers_columnar(req: &TransformRowsReq) -> bool {
    if let Some(rules) = req.rules.as_ref() {
        let eng = resolve_transform_engine(rules);
        return eng == "columnar_v1" || eng == "columnar_arrow_v1" || eng == "auto_v1";
    }
    env::var("AIWF_RUST_TRANSFORM_ENGINE")
        .ok()
        .map(|v| {
            let t = v.trim().to_ascii_lowercase();
            t == "columnar_v1" || t == "columnar_arrow_v1" || t == "auto_v1"
        })
        .unwrap_or(false)
}

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

pub(crate) fn value_to_arrow_string(v: Option<&Value>) -> Option<String> {
    match v {
        None => None,
        Some(Value::Null) => None,
        Some(Value::String(s)) => {
            if s.trim().is_empty() {
                None
            } else {
                Some(s.clone())
            }
        }
        Some(other) => {
            let s = value_to_string(other);
            if s.trim().is_empty() { None } else { Some(s) }
        }
    }
}

pub(crate) fn scalar_from_array(arr: &ArrayRef, idx: usize) -> Value {
    if idx >= arr.len() || arr.is_null(idx) {
        return Value::Null;
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Value::String(a.value(idx).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Value::Number(a.value(idx).into());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return serde_json::Number::from_f64(a.value(idx))
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Value::Bool(a.value(idx));
    }
    Value::Null
}

pub(crate) fn apply_transform_columnar_arrow_v1(
    rows: Vec<Map<String, Value>>,
    casts: &HashMap<String, String>,
    required_fields: &[String],
    compiled_filters: &[CompiledFilter],
    rule_hits: &mut HashMap<String, usize>,
) -> (Vec<Map<String, Value>>, usize, usize) {
    if rows.is_empty() {
        return (rows, 0, 0);
    }
    let n = rows.len();
    let mut field_set: HashSet<String> = HashSet::new();
    for r in &rows {
        for k in r.keys() {
            field_set.insert(k.clone());
        }
    }
    for k in casts.keys() {
        field_set.insert(k.clone());
    }
    for f in required_fields {
        field_set.insert(f.clone());
    }
    for f in compiled_filters {
        if !f.field.is_empty() {
            field_set.insert(f.field.clone());
        }
    }
    let mut fields: Vec<String> = field_set.into_iter().collect();
    fields.sort();
    if fields.is_empty() {
        return (Vec::new(), 0, 0);
    }

    let mut schema_fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for f in &fields {
        let mut b = StringBuilder::new();
        for r in &rows {
            match value_to_arrow_string(r.get(f)) {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            }
        }
        schema_fields.push(Field::new(f, DataType::Utf8, true));
        columns.push(Arc::new(b.finish()) as ArrayRef);
    }
    let mut field_types: Vec<DataType> = vec![DataType::Utf8; fields.len()];
    let mut batch = match RecordBatch::try_new(Arc::new(Schema::new(schema_fields)), columns) {
        Ok(b) => b,
        Err(_) => return (Vec::new(), n, 0),
    };

    let mut invalid = vec![false; n];
    for (field, cast_type) in casts {
        let Some(idx) = fields.iter().position(|x| x == field) else {
            continue;
        };
        let src = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
            .unwrap_or_else(|| StringArray::from(vec![Option::<String>::None; n]));
        let mut next: ArrayRef = batch.column(idx).clone();
        let cast_t = cast_type.as_str();
        if cast_t == "int" || cast_t == "integer" {
            let mut builder = Int64Builder::new();
            for (i, invalid_i) in invalid.iter_mut().enumerate().take(n) {
                if src.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = src.value(i).trim().replace(',', "");
                if s.is_empty() {
                    builder.append_null();
                    continue;
                }
                match s.parse::<i64>() {
                    Ok(v) => builder.append_value(v),
                    Err(_) => {
                        builder.append_null();
                        *invalid_i = true;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                    }
                }
            }
            next = Arc::new(builder.finish());
            field_types[idx] = DataType::Int64;
        } else if cast_t == "float" || cast_t == "double" || cast_t == "number" {
            let mut builder = Float64Builder::new();
            for (i, invalid_i) in invalid.iter_mut().enumerate().take(n) {
                if src.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = src.value(i).trim().replace(',', "");
                if s.is_empty() {
                    builder.append_null();
                    continue;
                }
                match s.parse::<f64>() {
                    Ok(v) => builder.append_value(v),
                    Err(_) => {
                        builder.append_null();
                        *invalid_i = true;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                    }
                }
            }
            next = Arc::new(builder.finish());
            field_types[idx] = DataType::Float64;
        } else if cast_t == "bool" || cast_t == "boolean" {
            let mut builder = BooleanBuilder::new();
            for (i, invalid_i) in invalid.iter_mut().enumerate().take(n) {
                if src.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = src.value(i).trim().to_ascii_lowercase();
                match s.as_str() {
                    "1" | "true" | "yes" | "on" => builder.append_value(true),
                    "0" | "false" | "no" | "off" => builder.append_value(false),
                    "" => builder.append_null(),
                    _ => {
                        builder.append_null();
                        *invalid_i = true;
                        *rule_hits.entry(format!("cast_fail_{field}")).or_insert(0) += 1;
                    }
                }
            }
            next = Arc::new(builder.finish());
            field_types[idx] = DataType::Boolean;
        }
        let mut next_cols = batch.columns().to_vec();
        next_cols[idx] = next;
        let rebuilt_schema = Schema::new(
            fields
                .iter()
                .enumerate()
                .map(|(i, f)| Field::new(f, field_types[i].clone(), true))
                .collect::<Vec<Field>>(),
        );
        batch = match RecordBatch::try_new(Arc::new(rebuilt_schema), next_cols) {
            Ok(b) => b,
            Err(_) => return (Vec::new(), n, 0),
        };
    }

    let mut filtered = vec![false; n];
    for i in 0..n {
        if invalid[i] {
            continue;
        }
        let mut row = Map::<String, Value>::new();
        for (j, f) in fields.iter().enumerate() {
            row.insert(f.clone(), scalar_from_array(batch.column(j), i));
        }
        let mut missing = false;
        for f in required_fields {
            if is_missing(row.get(f)) {
                missing = true;
                break;
            }
        }
        if missing {
            invalid[i] = true;
            *rule_hits.entry("required_missing".to_string()).or_insert(0) += 1;
            continue;
        }
        if !compiled_filters.is_empty()
            && !compiled_filters
                .iter()
                .all(|flt| filter_match_compiled(&row, flt))
        {
            filtered[i] = true;
            *rule_hits.entry("filtered_by_rule".to_string()).or_insert(0) += 1;
        }
    }

    let keep_idx: Vec<u32> = (0..n)
        .filter(|i| !invalid[*i] && !filtered[*i])
        .map(|i| i as u32)
        .collect();
    let invalid_rows = invalid.iter().filter(|x| **x).count();
    let filtered_rows = filtered.iter().filter(|x| **x).count();
    if keep_idx.is_empty() {
        return (Vec::new(), invalid_rows, filtered_rows);
    }
    let idx_arr = UInt32Array::from(keep_idx);
    let mut taken_cols: Vec<ArrayRef> = Vec::new();
    for col in batch.columns() {
        match take(col.as_ref(), &idx_arr, None) {
            Ok(c) => taken_cols.push(c),
            Err(_) => return (Vec::new(), invalid_rows, filtered_rows),
        }
    }
    let final_schema = Schema::new(
        fields
            .iter()
            .enumerate()
            .map(|(i, f)| Field::new(f, field_types[i].clone(), true))
            .collect::<Vec<Field>>(),
    );
    let kept = match RecordBatch::try_new(Arc::new(final_schema), taken_cols) {
        Ok(b) => b,
        Err(_) => return (Vec::new(), invalid_rows, filtered_rows),
    };
    let m = kept.num_rows();
    let mut out: Vec<Map<String, Value>> = Vec::with_capacity(m);
    for i in 0..m {
        let mut row = Map::<String, Value>::new();
        for (j, f) in fields.iter().enumerate() {
            row.insert(f.clone(), scalar_from_array(kept.column(j), i));
        }
        out.push(row);
    }
    (out, invalid_rows, filtered_rows)
}

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
