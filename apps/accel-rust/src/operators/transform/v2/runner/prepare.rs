use super::*;

pub(super) fn prepare_transform_request(
    req: &TransformRowsReq,
) -> Result<TransformPrepared, String> {
    let max_rows = env::var("AIWF_RUST_V2_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(200000);
    let max_bytes = env::var("AIWF_RUST_V2_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(128 * 1024 * 1024);
    let mut rows_in = req.rows.clone().unwrap_or_default();
    if rows_in.is_empty()
        && let Some(uri) = req.input_uri.clone()
    {
        rows_in = load_rows_from_uri_limited(&uri, max_rows, max_bytes)?;
    }
    let input_rows = rows_in.len();
    if input_rows > max_rows {
        return Err(format!(
            "input rows exceed limit: {} > {}",
            input_rows, max_rows
        ));
    }
    let estimated_bytes = serde_json::to_vec(&rows_in).map(|b| b.len()).unwrap_or(0);
    if estimated_bytes > max_bytes {
        return Err(format!(
            "input payload exceeds limit: {} > {}",
            estimated_bytes, max_bytes
        ));
    }
    let rules = if req.rules.is_some() {
        req.rules.clone().unwrap_or_else(|| json!({}))
    } else if let Some(dsl) = req.rules_dsl.clone() {
        compile_rules_dsl(&dsl)?
    } else {
        json!({})
    };
    let gates = req.quality_gates.clone().unwrap_or_else(|| json!({}));

    let null_values: Vec<String> = as_array_str(rule_get(&rules, "null_values"))
        .into_iter()
        .map(|s| s.to_lowercase())
        .collect();
    let trim_strings = as_bool(rule_get(&rules, "trim_strings"), true);

    let rename_map: HashMap<String, String> = rule_get(&rules, "rename_map")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<HashMap<String, String>>()
        })
        .unwrap_or_default();
    let casts: HashMap<String, String> = rule_get(&rules, "casts")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.trim().to_lowercase())))
                .collect::<HashMap<String, String>>()
        })
        .unwrap_or_default();

    let filters = rule_get(&rules, "filters")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let compiled_filters = compile_filters(&filters);
    let required_fields = as_array_str(rule_get(&rules, "required_fields"));
    let default_values = rule_get(&rules, "default_values")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let include_fields = as_array_str(rule_get(&rules, "include_fields"));
    let exclude_fields = as_array_str(rule_get(&rules, "exclude_fields"));
    let deduplicate_by = as_array_str(rule_get(&rules, "deduplicate_by"));
    let dedup_keep = rule_get(&rules, "deduplicate_keep")
        .and_then(|v| v.as_str())
        .unwrap_or("last")
        .to_lowercase();
    let sort_by = rule_get(&rules, "sort_by")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let requested_engine = resolve_transform_engine(&rules);
    let (engine, engine_reason) = if requested_engine == "auto_v1" {
        auto_select_engine(input_rows, estimated_bytes, &rules)
    } else {
        (
            requested_engine.clone(),
            format!("requested: {}", requested_engine),
        )
    };
    let use_columnar = engine == "columnar_v1" || engine == "columnar_arrow_v1";

    Ok(TransformPrepared {
        rows_in,
        input_rows,
        estimated_bytes,
        max_rows,
        max_bytes,
        rules,
        gates,
        null_values,
        trim_strings,
        rename_map,
        casts,
        compiled_filters,
        required_fields,
        default_values,
        include_fields,
        exclude_fields,
        deduplicate_by,
        dedup_keep,
        sort_by,
        requested_engine,
        engine,
        engine_reason,
        use_columnar,
    })
}
