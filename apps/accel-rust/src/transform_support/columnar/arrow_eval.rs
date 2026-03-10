use super::*;

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
