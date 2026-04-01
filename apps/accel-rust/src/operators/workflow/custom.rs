use super::support::{parse_workflow_input, serialize_workflow_output};
use super::*;

pub(super) fn workflow_transform_rows_v2_handler(
    state: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<TransformRowsReq>(input)?;
    let resp = run_transform_rows_v2_with_cache(
        req,
        None,
        Some(&state.transform_cache),
        Some(&state.metrics),
    )?;
    serialize_workflow_output(resp)
}

pub(super) fn workflow_plugin_health_v1_handler(
    _: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<PluginHealthReq>(input)?;
    let plugin = safe_pkg_token(&req.plugin)?;
    let details = run_plugin_healthcheck(&plugin, req.tenant_id.as_deref())?;
    Ok(json!({
        "ok": true,
        "operator": "plugin_health_v1",
        "status": "done",
        "plugin": plugin,
        "details": details
    }))
}

pub(super) fn workflow_schema_registry_v2_register_handler(
    state: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<SchemaRegisterReq>(input)?;
    let mut resp = run_schema_registry_register_v1(state, req)?;
    resp.operator = "schema_registry_v2_register".to_string();
    serialize_workflow_output(resp)
}

pub(super) fn workflow_schema_registry_v2_get_handler(
    state: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<SchemaGetReq>(input)?;
    let mut resp = run_schema_registry_get_v1(state, req)?;
    resp.operator = "schema_registry_v2_get".to_string();
    serialize_workflow_output(resp)
}

pub(super) fn workflow_schema_registry_v2_infer_handler(
    state: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<SchemaInferReq>(input)?;
    let mut resp = run_schema_registry_infer_v1(state, req)?;
    resp.operator = "schema_registry_v2_infer".to_string();
    serialize_workflow_output(resp)
}

pub(super) fn workflow_ingest_files_handler(
    _: &AppState,
    input: Value,
) -> Result<Value, String> {
    let params = input
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let input_files = params.get("input_files").cloned().unwrap_or_else(|| json!(""));
    Ok(json!({
        "ok": true,
        "operator": "ingest_files",
        "status": "done",
        "input_files": input_files,
        "detail": "draft ingest prepared",
    }))
}

pub(super) fn workflow_clean_md_handler(
    _: &AppState,
    input: Value,
) -> Result<Value, String> {
    let job_context = input.get("job_context").and_then(Value::as_object).cloned().unwrap_or_default();
    let job_root = job_context.get("job_root").and_then(Value::as_str).unwrap_or("");
    let ai_corpus_path = if job_root.is_empty() {
        "".to_string()
    } else {
        std::path::Path::new(job_root).join("stage").join("ai_corpus.md").to_string_lossy().to_string()
    };
    Ok(json!({
        "ok": true,
        "operator": "clean_md",
        "status": "done",
        "job_id": input.get("job_id").cloned().unwrap_or_else(|| json!("")),
        "ai_corpus_path": ai_corpus_path,
        "rust_v2_used": true,
        "warnings": [],
        "detail": "draft clean markdown prepared",
    }))
}

pub(super) fn workflow_compute_rust_handler(
    _: &AppState,
    input: Value,
) -> Result<Value, String> {
    let text = input
        .get("text")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            input.get("workflow_inputs")
                .and_then(Value::as_object)
                .and_then(|map| map.values().find_map(|value| value.get("detail").and_then(Value::as_str).map(str::to_string)))
        })
        .unwrap_or_default();
    let resp = run_compute_metrics(ComputeReq {
        run_id: input.get("run_id").and_then(Value::as_str).map(str::to_string),
        text,
    })?;
    serialize_workflow_output(resp)
}

pub(super) fn workflow_ai_refine_handler(
    _: &AppState,
    input: Value,
) -> Result<Value, String> {
    let detail = input
        .get("workflow_inputs")
        .and_then(Value::as_object)
        .and_then(|map| map.values().find_map(|value| value.get("detail").and_then(Value::as_str)))
        .unwrap_or("");
    Ok(json!({
        "ok": true,
        "operator": "ai_refine",
        "status": "done",
        "ai_mode": "compat",
        "ai_text_chars": detail.len(),
        "detail": detail,
    }))
}

pub(super) fn workflow_ai_audit_handler(
    _: &AppState,
    input: Value,
) -> Result<Value, String> {
    let should_block = input
        .get("config")
        .and_then(Value::as_object)
        .and_then(|config| config.get("force_block"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(json!({
        "ok": !should_block,
        "operator": "ai_audit",
        "status": if should_block { "quality_blocked" } else { "done" },
        "passed": !should_block,
        "reasons": if should_block { json!(["forced_block"]) } else { json!([]) },
        "metrics_hash": "",
        "ai_hash": "",
    }))
}

pub(super) fn workflow_md_output_handler(
    _: &AppState,
    input: Value,
) -> Result<Value, String> {
    let job_context = input.get("job_context").and_then(Value::as_object).cloned().unwrap_or_default();
    let job_root = job_context.get("job_root").and_then(Value::as_str).unwrap_or("");
    let path = if job_root.is_empty() {
        "".to_string()
    } else {
        std::path::Path::new(job_root).join("artifacts").join("workflow_summary.md").to_string_lossy().to_string()
    };
    Ok(json!({
        "ok": true,
        "operator": "md_output",
        "status": "done",
        "artifact_id": "workflow_summary_001",
        "kind": "md",
        "path": path,
        "sha256": "",
    }))
}

pub(super) fn workflow_sql_chart_v1_handler(
    _: &AppState,
    input: Value,
) -> Result<Value, String> {
    let cfg = input.as_object().cloned().unwrap_or_default();
    let rows = cfg
        .get("rows")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| {
            cfg.get("workflow_inputs")
                .and_then(Value::as_object)
                .and_then(|map| map.values().find_map(|value| value.get("rows").and_then(Value::as_array).cloned()))
        })
        .unwrap_or_default();
    let category_field = cfg
        .get("category_field")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("category");
    let value_field = cfg
        .get("value_field")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("value");
    let series_field = cfg
        .get("series_field")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("series");
    let chart_type = cfg
        .get("chart_type")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("bar");
    let top_n = cfg
        .get("top_n")
        .and_then(Value::as_u64)
        .map(|value| value.max(1) as usize)
        .unwrap_or(100);

    let mut grouped: HashMap<String, HashMap<String, f64>> = HashMap::new();
    let mut categories = Vec::<String>::new();
    let mut series_names = Vec::<String>::new();
    for row in &rows {
        let Some(obj) = row.as_object() else {
            continue;
        };
        let category = value_to_string(obj.get(category_field).unwrap_or(&Value::Null));
        if !grouped.contains_key(&category) {
            categories.push(category.clone());
            grouped.insert(category.clone(), HashMap::new());
        }
        let series = value_to_string(obj.get(series_field).unwrap_or(&Value::Null));
        if !series_names.iter().any(|item| item == &series) {
            series_names.push(series.clone());
        }
        let value = obj
            .get(value_field)
            .and_then(Value::as_f64)
            .unwrap_or_else(|| {
                obj.get(value_field)
                    .map(value_to_string)
                    .and_then(|text| text.parse::<f64>().ok())
                    .unwrap_or(0.0)
            });
        let entry = grouped
            .entry(category)
            .or_default()
            .entry(series)
            .or_insert(0.0);
        *entry += value;
    }

    categories.truncate(top_n);
    let series = series_names
        .into_iter()
        .map(|name| {
            json!({
                "name": name,
                "data": categories
                    .iter()
                    .map(|category| grouped.get(category).and_then(|bucket| bucket.get(&name)).copied().unwrap_or(0.0))
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "ok": true,
        "operator": "sql_chart_v1",
        "status": "done",
        "chart_type": chart_type,
        "categories": categories,
        "series": series,
        "rows_in": rows.len(),
    }))
}

pub(super) fn workflow_manual_review_handler(
    _: &AppState,
    input: Value,
) -> Result<Value, String> {
    let config = input.get("config").and_then(Value::as_object).cloned().unwrap_or_default();
    let params = input.get("params").and_then(Value::as_object).cloned().unwrap_or_default();
    let review_key = config
        .get("review_key")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| "manual_review".to_string());
    let review_bag = params
        .get("manual_review")
        .and_then(Value::as_object)
        .and_then(|bag| bag.get(&review_key))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let reviewer = review_bag
        .get("reviewer")
        .and_then(Value::as_str)
        .or_else(|| config.get("default_reviewer").and_then(Value::as_str))
        .unwrap_or("unassigned");
    let comment = review_bag
        .get("comment")
        .and_then(Value::as_str)
        .or_else(|| config.get("default_comment").and_then(Value::as_str))
        .unwrap_or("");
    if review_bag.get("approved").and_then(Value::as_bool).is_none() {
        return Ok(json!({
            "ok": false,
            "operator": "manual_review",
            "status": "pending_review",
            "review_key": review_key,
            "reviewer": reviewer,
            "comment": comment,
            "pending_reviews": [{
                "run_id": input.get("run_id").cloned().unwrap_or_else(|| json!("")),
                "workflow_id": input.get("job_id").cloned().unwrap_or_else(|| json!("")),
                "node_id": input.get("step_id").cloned().unwrap_or_else(|| json!(review_key)),
                "review_key": review_key,
                "reviewer": reviewer,
                "comment": comment,
                "status": "pending",
            }],
        }));
    }
    let approved = review_bag.get("approved").and_then(Value::as_bool).unwrap_or(false);
    Ok(json!({
        "ok": approved,
        "operator": "manual_review",
        "status": if approved { "approved" } else { "rejected" },
        "approved": approved,
        "review_key": review_key,
        "reviewer": reviewer,
        "comment": comment,
    }))
}
