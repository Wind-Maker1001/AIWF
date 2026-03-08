use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;

#[derive(Deserialize)]
pub(crate) struct LineageV2Req {
    pub run_id: Option<String>,
    pub rules: Option<Value>,
    pub computed_fields_v3: Option<Vec<Value>>,
}

#[derive(Deserialize)]
pub(crate) struct LineageV3Req {
    pub run_id: Option<String>,
    pub rules: Option<Value>,
    pub computed_fields_v3: Option<Vec<Value>>,
    pub workflow_steps: Option<Vec<Value>>,
    pub rows: Option<Vec<Value>>,
}

#[derive(Deserialize)]
pub(crate) struct WorkflowRunReq {
    pub run_id: Option<String>,
    pub trace_id: Option<String>,
    pub traceparent: Option<String>,
    pub tenant_id: Option<String>,
    pub context: Option<Value>,
    pub steps: Vec<Value>,
}

#[derive(Serialize)]
pub(crate) struct WorkflowRunResp {
    pub ok: bool,
    pub operator: String,
    pub status: String,
    pub trace_id: String,
    pub run_id: Option<String>,
    pub context: Value,
    pub steps: Vec<WorkflowStepReplay>,
    pub failed_step: Option<String>,
    pub error: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct WorkflowStepReplay {
    pub id: String,
    pub operator: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: u128,
    pub input_summary: Value,
    pub output_summary: Option<Value>,
    pub error: Option<String>,
}

pub(crate) fn run_lineage_v2(req: LineageV2Req) -> Result<Value, String> {
    let mut edges = Vec::<Value>::new();
    if let Some(rules) = req.rules.as_ref()
        && let Some(m) = rules.get("computed_fields").and_then(|v| v.as_object())
    {
        for (target, expr) in m {
            let mut deps = HashSet::new();
            let re = Regex::new(r"\$([A-Za-z0-9_]+)").map_err(|e| e.to_string())?;
            let s = value_to_string(expr);
            for c in re.captures_iter(&s) {
                if let Some(f) = c.get(1) {
                    deps.insert(f.as_str().to_string());
                }
            }
            for d in deps {
                edges.push(json!({"from": d, "to": target, "kind":"computed_fields"}));
            }
        }
    }
    if let Some(specs) = req.computed_fields_v3.as_ref() {
        for sp in specs {
            let Some(o) = sp.as_object() else { continue };
            let target = o
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let expr = o.get("expr").cloned().unwrap_or(Value::Null);
            let mut deps = HashSet::new();
            collect_expr_lineage(&expr, &mut deps);
            for d in deps {
                edges.push(json!({"from": d, "to": target, "kind":"computed_fields_v3"}));
            }
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "lineage_v2",
        "status": "done",
        "run_id": req.run_id,
        "edges": edges
    }))
}

pub(crate) fn run_lineage_v3(req: LineageV3Req) -> Result<Value, String> {
    let mut out = run_lineage_v2(LineageV2Req {
        run_id: req.run_id.clone(),
        rules: req.rules,
        computed_fields_v3: req.computed_fields_v3,
    })?;
    if let Some(m) = out.as_object_mut() {
        if let Some(rows) = req.rows.as_ref()
            && let Some(obj) = rows.first().and_then(|v| v.as_object())
        {
            m.insert(
                "source_columns".to_string(),
                Value::Array(obj.keys().map(|k| Value::String(k.clone())).collect()),
            );
        }
        let edges = req
            .workflow_steps
            .unwrap_or_default()
            .into_iter()
            .filter_map(|s| s.as_object().cloned())
            .flat_map(|o| {
                let to = o
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let deps = o
                    .get("depends_on")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                deps.into_iter().filter_map(move |d| {
                    d.as_str()
                        .map(|from| json!({"from": from, "to": to, "type": "step_dep"}))
                })
            })
            .collect::<Vec<_>>();
        m.insert("step_lineage".to_string(), json!(edges));
        m.insert("operator".to_string(), json!("lineage_v3"));
    }
    Ok(out)
}

pub(crate) fn summarize_value(v: &Value) -> Value {
    match v {
        Value::Array(a) => json!({"type":"array","len":a.len()}),
        Value::Object(m) => {
            let keys = m.keys().take(12).cloned().collect::<Vec<_>>();
            json!({"type":"object","keys":keys,"size":m.len()})
        }
        Value::String(s) => json!({"type":"string","len":s.chars().count()}),
        Value::Number(n) => json!({"type":"number","value":n}),
        Value::Bool(b) => json!({"type":"bool","value":b}),
        Value::Null => json!({"type":"null"}),
    }
}

pub(crate) fn run_workflow(req: WorkflowRunReq) -> Result<WorkflowRunResp, String> {
    let step_limit = tenant_max_workflow_steps_for(req.tenant_id.as_deref());
    if req.steps.len() > step_limit {
        return Err(format!(
            "workflow step quota exceeded: {} > {}",
            req.steps.len(),
            step_limit
        ));
    }
    let trace_id = resolve_trace_id(
        req.trace_id.as_deref(),
        req.traceparent.as_deref(),
        &format!(
            "wf:{}:{}:{}",
            req.run_id.clone().unwrap_or_default(),
            req.tenant_id
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            req.steps.len()
        ),
    );
    let mut ctx = req.context.unwrap_or_else(|| json!({}));
    let mut trace: Vec<WorkflowStepReplay> = Vec::new();
    let mut failed_step: Option<String> = None;
    let mut failed_error: Option<String> = None;
    for step in &req.steps {
        let Some(obj) = step.as_object() else {
            return Err("workflow step must be object".to_string());
        };
        let id = obj.get("id").and_then(|v| v.as_str()).unwrap_or("step");
        let op = obj.get("operator").and_then(|v| v.as_str()).unwrap_or("");
        let input = obj.get("input").cloned().unwrap_or_else(|| json!({}));
        if !operator_allowed_for_tenant(op, req.tenant_id.as_deref()) {
            return Err(format!(
                "operator_forbidden: tenant={} operator={}",
                req.tenant_id.as_deref().unwrap_or("default"),
                op
            ));
        }
        let started_at = utc_now_iso();
        let begin = Instant::now();
        let step_result: Result<Value, String> = match op {
            "transform_rows_v2" => serde_json::from_value::<TransformRowsReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_transform_rows_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "transform_rows_v3" => serde_json::from_value::<TransformRowsV3Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_transform_rows_v3)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "text_preprocess_v2" => serde_json::from_value::<TextPreprocessReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_text_preprocess_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "compute_metrics" => serde_json::from_value::<ComputeReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_compute_metrics)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "join_rows_v1" => serde_json::from_value::<JoinRowsReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_join_rows_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "join_rows_v2" => serde_json::from_value::<JoinRowsV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_join_rows_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "join_rows_v3" => serde_json::from_value::<JoinRowsV3Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_join_rows_v3)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "normalize_schema_v1" => serde_json::from_value::<NormalizeSchemaReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_normalize_schema_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "entity_extract_v1" => serde_json::from_value::<EntityExtractReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_entity_extract_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "aggregate_rows_v1" => serde_json::from_value::<AggregateRowsReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_aggregate_rows_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "aggregate_rows_v2" => serde_json::from_value::<AggregateRowsV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_aggregate_rows_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "aggregate_rows_v3" => serde_json::from_value::<AggregateRowsV3Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_aggregate_rows_v3)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "quality_check_v1" => serde_json::from_value::<QualityCheckReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_quality_check_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "quality_check_v2" => serde_json::from_value::<QualityCheckV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_quality_check_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "quality_check_v3" => serde_json::from_value::<QualityCheckV3Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_quality_check_v3)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "aggregate_pushdown_v1" => serde_json::from_value::<AggregatePushdownReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_aggregate_pushdown_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "plugin_exec_v1" => serde_json::from_value::<PluginExecReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_plugin_exec_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "plugin_health_v1" => serde_json::from_value::<PluginHealthReq>(input)
                .map_err(|e| e.to_string())
                .and_then(|r| {
                    let plugin = safe_pkg_token(&r.plugin)?;
                    run_plugin_healthcheck(&plugin, r.tenant_id.as_deref()).map(|details| {
                        json!({
                            "ok": true,
                            "operator": "plugin_health_v1",
                            "status": "done",
                            "plugin": plugin,
                            "details": details
                        })
                    })
                }),
            "plugin_registry_v1" => serde_json::from_value::<PluginRegistryV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_plugin_registry_v1),
            "plugin_operator_v1" => serde_json::from_value::<PluginOperatorV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_plugin_operator_v1),
            "rules_package_publish_v1" => serde_json::from_value::<RulesPackagePublishReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_rules_package_publish_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "rules_package_get_v1" => serde_json::from_value::<RulesPackageGetReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_rules_package_get_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "load_rows_v2" => serde_json::from_value::<LoadRowsV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_load_rows_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "load_rows_v3" => serde_json::from_value::<LoadRowsV3Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_load_rows_v3)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "schema_registry_v2_check_compat" => serde_json::from_value::<SchemaCompatReq>(input)
                .map_err(|e| e.to_string())
                .and_then(|r| {
                    let st = AppState {
                        service: "workflow_local".to_string(),
                        tasks: Arc::new(Mutex::new(HashMap::new())),
                        metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
                        task_cfg: Arc::new(Mutex::new(task_store_config_from_env())),
                        cancel_flags: Arc::new(Mutex::new(HashMap::new())),
                        tenant_running: Arc::new(Mutex::new(HashMap::new())),
                        idempotency_index: Arc::new(Mutex::new(HashMap::new())),
                        transform_cache: Arc::new(Mutex::new(HashMap::new())),
                        schema_registry: Arc::new(Mutex::new(load_schema_registry_store())),
                    };
                    run_schema_registry_check_compat_v2(&st, r)
                })
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "schema_registry_v2_suggest_migration" => {
                serde_json::from_value::<SchemaMigrationSuggestReq>(input)
                    .map_err(|e| e.to_string())
                    .and_then(|r| {
                        let st = AppState {
                            service: "workflow_local".to_string(),
                            tasks: Arc::new(Mutex::new(HashMap::new())),
                            metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
                            task_cfg: Arc::new(Mutex::new(task_store_config_from_env())),
                            cancel_flags: Arc::new(Mutex::new(HashMap::new())),
                            tenant_running: Arc::new(Mutex::new(HashMap::new())),
                            idempotency_index: Arc::new(Mutex::new(HashMap::new())),
                            transform_cache: Arc::new(Mutex::new(HashMap::new())),
                            schema_registry: Arc::new(Mutex::new(load_schema_registry_store())),
                        };
                        run_schema_registry_suggest_migration_v2(&st, r)
                    })
                    .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string()))
            }
            "udf_wasm_v1" => serde_json::from_value::<UdfWasmReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_udf_wasm_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "schema_registry_v1_register" => serde_json::from_value::<SchemaRegisterReq>(input)
                .map_err(|e| e.to_string())
                .and_then(|r| run_schema_registry_register_local(&r))
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "schema_registry_v1_get" => serde_json::from_value::<SchemaGetReq>(input)
                .map_err(|e| e.to_string())
                .and_then(|r| run_schema_registry_get_local(&r))
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "schema_registry_v1_infer" => serde_json::from_value::<SchemaInferReq>(input)
                .map_err(|e| e.to_string())
                .and_then(|r| run_schema_registry_infer_local(&r))
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "schema_registry_v2_register" => serde_json::from_value::<SchemaRegisterReq>(input)
                .map_err(|e| e.to_string())
                .and_then(|r| run_schema_registry_register_local(&r))
                .and_then(|mut v| {
                    v.operator = "schema_registry_v2_register".to_string();
                    serde_json::to_value(v).map_err(|e| e.to_string())
                }),
            "schema_registry_v2_get" => serde_json::from_value::<SchemaGetReq>(input)
                .map_err(|e| e.to_string())
                .and_then(|r| run_schema_registry_get_local(&r))
                .and_then(|mut v| {
                    v.operator = "schema_registry_v2_get".to_string();
                    serde_json::to_value(v).map_err(|e| e.to_string())
                }),
            "schema_registry_v2_infer" => serde_json::from_value::<SchemaInferReq>(input)
                .map_err(|e| e.to_string())
                .and_then(|r| run_schema_registry_infer_local(&r))
                .and_then(|mut v| {
                    v.operator = "schema_registry_v2_infer".to_string();
                    serde_json::to_value(v).map_err(|e| e.to_string())
                }),
            "time_series_v1" => serde_json::from_value::<TimeSeriesReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_time_series_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "stats_v1" => serde_json::from_value::<StatsReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_stats_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "entity_linking_v1" => serde_json::from_value::<EntityLinkReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_entity_linking_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "table_reconstruct_v1" => serde_json::from_value::<TableReconstructReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_table_reconstruct_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "feature_store_v1_upsert" => serde_json::from_value::<FeatureStoreUpsertReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_feature_store_upsert_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "feature_store_v1_get" => serde_json::from_value::<FeatureStoreGetReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_feature_store_get_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "lineage_v2" => serde_json::from_value::<LineageV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_lineage_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "rule_simulator_v1" => serde_json::from_value::<RuleSimulatorReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_rule_simulator_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "constraint_solver_v1" => serde_json::from_value::<ConstraintSolverReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_constraint_solver_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "chart_data_prep_v1" => serde_json::from_value::<ChartDataPrepReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_chart_data_prep_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "diff_audit_v1" => serde_json::from_value::<DiffAuditReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_diff_audit_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "vector_index_v1_build" => serde_json::from_value::<VectorIndexBuildReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_vector_index_build_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "vector_index_v1_search" => serde_json::from_value::<VectorIndexSearchReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_vector_index_search_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "evidence_rank_v1" => serde_json::from_value::<EvidenceRankReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_evidence_rank_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "fact_crosscheck_v1" => serde_json::from_value::<FactCrosscheckReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_fact_crosscheck_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "timeseries_forecast_v1" => serde_json::from_value::<TimeSeriesForecastReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_timeseries_forecast_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "finance_ratio_v1" => serde_json::from_value::<FinanceRatioReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_finance_ratio_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "anomaly_explain_v1" => serde_json::from_value::<AnomalyExplainReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_anomaly_explain_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "template_bind_v1" => serde_json::from_value::<TemplateBindReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_template_bind_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "provenance_sign_v1" => serde_json::from_value::<ProvenanceSignReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_provenance_sign_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "stream_state_v1_save" => serde_json::from_value::<StreamStateSaveReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_stream_state_save_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "stream_state_v1_load" => serde_json::from_value::<StreamStateLoadReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_stream_state_load_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "query_lang_v1" => serde_json::from_value::<QueryLangReq>(input)
                .map_err(|e| e.to_string())
                .and_then(run_query_lang_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "columnar_eval_v1" => serde_json::from_value::<ColumnarEvalV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_columnar_eval_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "stream_window_v1" => serde_json::from_value::<StreamWindowV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_stream_window_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "stream_window_v2" => serde_json::from_value::<StreamWindowV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_stream_window_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "sketch_v1" => serde_json::from_value::<SketchV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_sketch_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "runtime_stats_v1" => serde_json::from_value::<RuntimeStatsV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_runtime_stats_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "capabilities_v1" => serde_json::from_value::<CapabilitiesV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_capabilities_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "io_contract_v1" => serde_json::from_value::<IoContractV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_io_contract_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "failure_policy_v1" => serde_json::from_value::<FailurePolicyV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_failure_policy_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "window_rows_v1" => serde_json::from_value::<WindowRowsV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_window_rows_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "optimizer_v1" => serde_json::from_value::<OptimizerV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_optimizer_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "join_rows_v4" => serde_json::from_value::<JoinRowsV4Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_join_rows_v4)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "aggregate_rows_v4" => serde_json::from_value::<AggregateRowsV4Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_aggregate_rows_v4)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "quality_check_v4" => serde_json::from_value::<QualityCheckV4Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_quality_check_v4)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "lineage_v3" => serde_json::from_value::<LineageV3Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_lineage_v3)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "parquet_io_v2" => serde_json::from_value::<ParquetIoV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_parquet_io_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "stream_state_v2" => serde_json::from_value::<StreamStateV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_stream_state_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "udf_wasm_v2" => serde_json::from_value::<UdfWasmV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_udf_wasm_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "explain_plan_v1" => serde_json::from_value::<ExplainPlanV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_explain_plan_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "explain_plan_v2" => serde_json::from_value::<ExplainPlanV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_explain_plan_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "incremental_plan_v1" => serde_json::from_value::<IncrementalPlanV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(|r| {
                    let st = AppState {
                        service: "workflow_local".to_string(),
                        tasks: Arc::new(Mutex::new(HashMap::new())),
                        metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
                        task_cfg: Arc::new(Mutex::new(task_store_config_from_env())),
                        cancel_flags: Arc::new(Mutex::new(HashMap::new())),
                        tenant_running: Arc::new(Mutex::new(HashMap::new())),
                        idempotency_index: Arc::new(Mutex::new(HashMap::new())),
                        transform_cache: Arc::new(Mutex::new(HashMap::new())),
                        schema_registry: Arc::new(Mutex::new(load_schema_registry_store())),
                    };
                    run_incremental_plan_v1(&st, r)
                })
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "tenant_isolation_v1" => serde_json::from_value::<TenantIsolationV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_tenant_isolation_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "operator_policy_v1" => serde_json::from_value::<OperatorPolicyV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_operator_policy_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "optimizer_adaptive_v2" => serde_json::from_value::<OptimizerAdaptiveV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_optimizer_adaptive_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "vector_index_v2_build" => serde_json::from_value::<VectorIndexBuildV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_vector_index_build_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "vector_index_v2_search" => serde_json::from_value::<VectorIndexSearchV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_vector_index_search_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "vector_index_v2_eval" => serde_json::from_value::<VectorIndexEvalV2Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_vector_index_eval_v2)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "stream_reliability_v1" => serde_json::from_value::<StreamReliabilityV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_stream_reliability_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "lineage_provenance_v1" => serde_json::from_value::<LineageProvenanceV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_lineage_provenance_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "contract_regression_v1" => serde_json::from_value::<ContractRegressionV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_contract_regression_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            "perf_baseline_v1" => serde_json::from_value::<PerfBaselineV1Req>(input)
                .map_err(|e| e.to_string())
                .and_then(run_perf_baseline_v1)
                .and_then(|v| serde_json::to_value(v).map_err(|e| e.to_string())),
            _ => Err(format!("unsupported workflow operator: {op}")),
        };
        let output = match step_result {
            Ok(v) => v,
            Err(err) => {
                let _ = run_runtime_stats_v1(RuntimeStatsV1Req {
                    run_id: req.run_id.clone(),
                    op: "record".to_string(),
                    operator: Some(op.to_string()),
                    ok: Some(false),
                    error_code: Some(normalize_error_code(&err)),
                    duration_ms: Some(begin.elapsed().as_millis()),
                    rows_in: None,
                    rows_out: None,
                });
                trace.push(WorkflowStepReplay {
                    id: id.to_string(),
                    operator: op.to_string(),
                    status: "failed".to_string(),
                    started_at,
                    finished_at: utc_now_iso(),
                    duration_ms: begin.elapsed().as_millis(),
                    input_summary: summarize_value(
                        &obj.get("input").cloned().unwrap_or_else(|| json!({})),
                    ),
                    output_summary: None,
                    error: Some(err.clone()),
                });
                failed_step = Some(id.to_string());
                failed_error = Some(err);
                break;
            }
        };
        let finished_at = utc_now_iso();
        if let Some(map) = ctx.as_object_mut()
            && failed_step.is_none()
        {
            map.insert(id.to_string(), output.clone());
        }
        trace.push(WorkflowStepReplay {
            id: id.to_string(),
            operator: op.to_string(),
            status: "done".to_string(),
            started_at,
            finished_at,
            duration_ms: begin.elapsed().as_millis(),
            input_summary: summarize_value(&obj.get("input").cloned().unwrap_or_else(|| json!({}))),
            output_summary: Some(summarize_value(&output)),
            error: None,
        });
        let _ = run_runtime_stats_v1(RuntimeStatsV1Req {
            run_id: req.run_id.clone(),
            op: "record".to_string(),
            operator: Some(op.to_string()),
            ok: Some(true),
            error_code: None,
            duration_ms: Some(begin.elapsed().as_millis()),
            rows_in: None,
            rows_out: None,
        });
    }
    let status = if failed_step.is_some() {
        "failed"
    } else {
        "done"
    };
    Ok(WorkflowRunResp {
        ok: failed_step.is_none(),
        operator: "workflow_run".to_string(),
        status: status.to_string(),
        trace_id,
        run_id: req.run_id,
        context: ctx,
        steps: trace,
        failed_step,
        error: failed_error,
    })
}
