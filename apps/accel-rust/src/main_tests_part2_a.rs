use super::*;

use crate::*;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

#[test]
fn transform_rows_v3_supports_expr_and_lineage() {
    let out = run_transform_rows_v3(TransformRowsV3Req {
        run_id: Some("tv3-1".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![json!({"a":2,"b":3}), json!({"a":1,"b":0})]),
        rules: Some(json!({})),
        rules_dsl: None,
        quality_gates: None,
        schema_hint: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
        computed_fields_v3: Some(vec![
            json!({"name":"c","expr":{"op":"add","args":[{"field":"a"},{"field":"b"}]}}),
        ]),
        filter_expr_v3: Some(json!({"op":"gt","args":[{"field":"a"},{"const":1}]})),
    })
    .expect("transform v3");
    assert_eq!(out.operator, "transform_rows_v3");
    assert_eq!(out.rows.len(), 1);
    let row = out.rows[0].as_object().cloned().unwrap_or_default();
    assert_eq!(row.get("c").and_then(|v| v.as_f64()).unwrap_or(0.0), 5.0);
    let lineage = out
        .audit
        .get("lineage_v3")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert!(lineage.get("c").is_some());
}

#[test]
fn join_aggregate_quality_v3_basics_work() {
    let j = run_join_rows_v3(JoinRowsV3Req {
        run_id: Some("j3".to_string()),
        left_rows: vec![json!({"id":"1","x":10}), json!({"id":"2","x":20})],
        right_rows: vec![json!({"id":"1","y":3})],
        left_on: json!(["id"]),
        right_on: json!(["id"]),
        join_type: Some("left".to_string()),
        strategy: Some("auto".to_string()),
        spill_path: None,
        chunk_size: Some(1),
    })
    .expect("join v3");
    assert_eq!(j.operator, "join_rows_v3");
    assert_eq!(j.rows.len(), 2);

    let a = run_aggregate_rows_v3(AggregateRowsV3Req {
        run_id: Some("a3".to_string()),
        rows: vec![
            json!({"g":"x","v":1}),
            json!({"g":"x","v":2}),
            json!({"g":"x","v":2}),
        ],
        group_by: vec!["g".to_string()],
        aggregates: vec![
            json!({"op":"count","as":"n"}),
            json!({"op":"approx_count_distinct","field":"v","as":"d"}),
            json!({"op":"approx_percentile_p50","field":"v","as":"p50"}),
        ],
        approx_sample_size: Some(128),
    })
    .expect("agg v3");
    assert_eq!(a.operator, "aggregate_rows_v3");
    let r = a.rows[0].as_object().cloned().unwrap_or_default();
    assert_eq!(r.get("n").and_then(|v| v.as_u64()).unwrap_or(0), 3);
    assert!(r.get("d").is_some());
    assert!(r.get("p50").is_some());

    let q = run_quality_check_v3(QualityCheckV3Req {
        run_id: Some("q3".to_string()),
        rows: vec![json!({"v":1.0}), json!({"v":2.0}), json!({"v":100.0})],
        rules: json!({
            "anomaly_iqr":[{"field":"v"}],
            "drift_psi":{"field":"v","expected":[1,2,2,1,2,1],"max_psi":0.01}
        }),
    })
    .expect("qc v3");
    assert_eq!(q.operator, "quality_check_v3");
    assert!(!q.passed);
}

#[test]
fn load_v3_schema_v2_udf_v1_basics_work() {
    let now = utc_now_iso().replace([':', '-'], "");
    let p = std::env::temp_dir().join(format!("aiwf_load_v3_{now}.txt"));
    fs::write(&p, "l1\nl2\n").expect("write temp");
    let l = run_load_rows_v3(LoadRowsV3Req {
        source_type: "txt".to_string(),
        source: p.to_string_lossy().to_string(),
        query: None,
        limit: Some(10),
        max_retries: Some(1),
        retry_backoff_ms: Some(10),
        resume_token: Some("r1".to_string()),
        connector_options: Some(json!({"connector":"local"})),
    })
    .expect("load v3");
    assert_eq!(l.operator, "load_rows_v3");
    assert_eq!(l.rows.len(), 2);
    let _ = fs::remove_file(p);

    let state = AppState {
        service: "accel-rust".to_string(),
        tasks: Arc::new(Mutex::new(HashMap::<String, TaskState>::new())),
        metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
        task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
            ttl_sec: 3600,
            max_tasks: 1000,
            store_path: None,
            remote_enabled: false,
            backend: "memory".to_string(),
            base_api_url: None,
            base_api_key: None,
            sql_host: "localhost".to_string(),
            sql_port: 1433,
            sql_db: "master".to_string(),
            sql_user: None,
            sql_password: None,
            sql_use_windows_auth: true,
        })),
        cancel_flags: Arc::new(Mutex::new(HashMap::new())),
        tenant_running: Arc::new(Mutex::new(HashMap::new())),
        idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        transform_cache: Arc::new(Mutex::new(HashMap::new())),
        schema_registry: Arc::new(Mutex::new(HashMap::new())),
    };
    let _ = crate::run_schema_registry_register_v1(
        &state,
        crate::SchemaRegisterReq {
            name: "s".to_string(),
            version: "v1".to_string(),
            schema: json!({"id":"int","amount":"int"}),
        },
    )
    .expect("register v1");
    let _ = crate::run_schema_registry_register_v1(
        &state,
        crate::SchemaRegisterReq {
            name: "s".to_string(),
            version: "v2".to_string(),
            schema: json!({"id":"int","amount":"float","extra":"string"}),
        },
    )
    .expect("register v2");
    let cc = run_schema_registry_check_compat_v2(
        &state,
        SchemaCompatReq {
            name: "s".to_string(),
            from_version: "v1".to_string(),
            to_version: "v2".to_string(),
            mode: Some("backward".to_string()),
        },
    )
    .expect("compat");
    assert!(cc.breaking_fields.is_empty());
    let mg = run_schema_registry_suggest_migration_v2(
        &state,
        SchemaMigrationSuggestReq {
            name: "s".to_string(),
            from_version: "v1".to_string(),
            to_version: "v2".to_string(),
        },
    )
    .expect("migration");
    assert!(!mg.steps.is_empty());

    let udf = run_udf_wasm_v1(UdfWasmReq {
        run_id: Some("u1".to_string()),
        rows: vec![json!({"x":3}), json!({"x":7})],
        field: "x".to_string(),
        output_field: "y".to_string(),
        op: Some("double".to_string()),
        wasm_base64: Some("AGFzbQEAAA==".to_string()),
    })
    .expect("udf");
    let out_rows = udf
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(out_rows.len(), 2);
}

#[test]
fn new_ten_ops_basic_work() {
    let ts = crate::run_time_series_v1(crate::TimeSeriesReq {
        run_id: Some("ts1".to_string()),
        rows: vec![
            json!({"month":"2024-01","v":10}),
            json!({"month":"2024-02","v":12}),
        ],
        time_field: "month".to_string(),
        value_field: "v".to_string(),
        group_by: None,
        window: Some(2),
    })
    .expect("ts");
    assert_eq!(
        ts.get("status").and_then(|v| v.as_str()).unwrap_or(""),
        "done"
    );

    let st = crate::run_stats_v1(crate::StatsReq {
        run_id: Some("s1".to_string()),
        rows: vec![
            json!({"x":1,"y":2}),
            json!({"x":2,"y":4}),
            json!({"x":3,"y":6}),
        ],
        x_field: "x".to_string(),
        y_field: "y".to_string(),
    })
    .expect("stats");
    assert!(st.get("metrics").is_some());

    let el = crate::run_entity_linking_v1(crate::EntityLinkReq {
        run_id: None,
        rows: vec![json!({"entity":"Open AI"}), json!({"entity":"Open-AI"})],
        field: "entity".to_string(),
        id_field: Some("eid".to_string()),
    })
    .expect("entity");
    assert_eq!(
        el.get("status").and_then(|v| v.as_str()).unwrap_or(""),
        "done"
    );

    let tr = crate::run_table_reconstruct_v1(crate::TableReconstructReq {
        run_id: None,
        lines: Some(vec!["a  b  c".to_string(), "1  2  3".to_string()]),
        text: None,
        delimiter: None,
    })
    .expect("table");
    assert!(
        tr.get("rows")
            .and_then(|v| v.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false)
    );

    let _ = crate::run_feature_store_upsert_v1(crate::FeatureStoreUpsertReq {
        run_id: None,
        key_field: "id".to_string(),
        rows: vec![json!({"id":"k1","f":1})],
    })
    .expect("fs upsert");
    let fg = crate::run_feature_store_get_v1(crate::FeatureStoreGetReq {
        run_id: None,
        key: "k1".to_string(),
    })
    .expect("fs get");
    assert!(fg.get("value").is_some());

    let lg = crate::run_lineage_v2(crate::LineageV2Req {
        run_id: None,
        rules: Some(json!({"computed_fields":{"total":"mul($price,$qty)"}})),
        computed_fields_v3: None,
    })
    .expect("lineage");
    assert!(lg.get("edges").is_some());

    let rs = crate::run_rule_simulator_v1(crate::RuleSimulatorReq {
        run_id: None,
        rows: vec![json!({"x":"1"}), json!({"x":"2"})],
        rules: json!({"casts":{"x":"int"}}),
        candidate_rules: json!({"casts":{"x":"int"},"filters":[{"field":"x","op":"gt","value":1}]}),
    })
    .expect("sim");
    assert!(rs.get("delta_rows").is_some());

    let cs = crate::run_constraint_solver_v1(crate::ConstraintSolverReq {
        run_id: None,
        rows: vec![json!({"a":1,"b":2,"sum":3}), json!({"a":1,"b":2,"sum":9})],
        constraints: vec![json!({"kind":"sum_equals","left":["a","b"],"right":"sum"})],
    })
    .expect("constraint");
    assert!(!cs.get("passed").and_then(|v| v.as_bool()).unwrap_or(true));

    let cp = crate::run_chart_data_prep_v1(crate::ChartDataPrepReq {
        run_id: None,
        rows: vec![
            json!({"c":"A","s":"S1","v":2}),
            json!({"c":"A","s":"S2","v":3}),
        ],
        category_field: "c".to_string(),
        value_field: "v".to_string(),
        series_field: Some("s".to_string()),
        top_n: Some(10),
    })
    .expect("chart");
    assert!(cp.get("chart").is_some());

    let da = crate::run_diff_audit_v1(crate::DiffAuditReq {
        run_id: None,
        left_rows: vec![json!({"id":"1","v":1}), json!({"id":"2","v":2})],
        right_rows: vec![json!({"id":"2","v":3}), json!({"id":"3","v":9})],
        keys: vec!["id".to_string()],
    })
    .expect("diff");
    assert_eq!(
        da.get("summary")
            .and_then(|s| s.get("added"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        1
    );
}

#[test]
fn additional_ten_ops_basic_work() {
    let vb = run_vector_index_build_v1(VectorIndexBuildReq {
        run_id: Some("vb1".to_string()),
        rows: vec![
            json!({"id":"d1","text":"alpha beta gamma"}),
            json!({"id":"d2","text":"finance ratio cash flow"}),
        ],
        id_field: "id".to_string(),
        text_field: "text".to_string(),
    })
    .expect("vector build");
    assert_eq!(
        vb.get("status").and_then(|v| v.as_str()).unwrap_or(""),
        "done"
    );
    let vs = run_vector_index_search_v1(VectorIndexSearchReq {
        run_id: Some("vs1".to_string()),
        query: "cash flow".to_string(),
        top_k: Some(1),
    })
    .expect("vector search");
    assert_eq!(
        vs.get("hits")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0),
        1
    );

    let er = run_evidence_rank_v1(EvidenceRankReq {
        run_id: None,
        rows: vec![
            json!({"relevance":0.9,"source_score":0.8,"consistency":0.7,"time":"2025-01-01"}),
        ],
        time_field: Some("time".to_string()),
        source_field: Some("source_score".to_string()),
        relevance_field: Some("relevance".to_string()),
        consistency_field: Some("consistency".to_string()),
    })
    .expect("rank");
    assert!(er.get("rows").is_some());

    let fc = run_fact_crosscheck_v1(FactCrosscheckReq {
        run_id: None,
        rows: vec![
            json!({"claim":"GDP grows 5%","source":"a"}),
            json!({"claim":"GDP grows 5 %","source":"b"}),
        ],
        claim_field: "claim".to_string(),
        source_field: Some("source".to_string()),
    })
    .expect("cross");
    assert!(fc.get("results").is_some());

    let tf = run_timeseries_forecast_v1(TimeSeriesForecastReq {
        run_id: None,
        rows: vec![
            json!({"t":"2024-01","v":10}),
            json!({"t":"2024-02","v":12}),
            json!({"t":"2024-03","v":14}),
        ],
        time_field: "t".to_string(),
        value_field: "v".to_string(),
        horizon: Some(2),
        method: Some("naive_drift".to_string()),
    })
    .expect("forecast");
    assert_eq!(
        tf.get("forecast")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0),
        2
    );

    let fr = run_finance_ratio_v1(FinanceRatioReq {
        run_id: None,
        rows: vec![json!({"current_assets":100,"current_liabilities":50,"total_debt":40,"total_equity":20,"revenue":200,"net_income":20,"operating_cash_flow":30})],
    })
    .expect("finance ratio");
    assert!(fr.get("rows").is_some());

    let ax = run_anomaly_explain_v1(AnomalyExplainReq {
        run_id: None,
        rows: vec![
            json!({"score":0.95,"a":10,"b":2}),
            json!({"score":0.2,"a":1,"b":1}),
        ],
        score_field: "score".to_string(),
        threshold: Some(0.9),
    })
    .expect("anomaly explain");
    assert_eq!(
        ax.get("anomalies")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0),
        1
    );

    let tb = run_template_bind_v1(TemplateBindReq {
        run_id: None,
        template_text: "Hello {{user.name}}, score={{score}}".to_string(),
        data: json!({"user":{"name":"AIWF"},"score":99}),
    })
    .expect("bind");
    assert!(
        tb.get("bound_text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("AIWF")
    );

    let ps = run_provenance_sign_v1(ProvenanceSignReq {
        run_id: None,
        payload: json!({"k":"v"}),
        prev_hash: Some("abc".to_string()),
    })
    .expect("sign");
    assert!(ps.get("record").and_then(|v| v.get("hash")).is_some());

    let _ = run_stream_state_save_v1(StreamStateSaveReq {
        run_id: None,
        stream_key: "s1".to_string(),
        state: json!({"x":1}),
        offset: Some(10),
    })
    .expect("stream save");
    let sl = run_stream_state_load_v1(StreamStateLoadReq {
        run_id: None,
        stream_key: "s1".to_string(),
    })
    .expect("stream load");
    assert!(sl.get("value").is_some());

    let ql = run_query_lang_v1(QueryLangReq {
        run_id: None,
        rows: vec![json!({"a":1,"b":2}), json!({"a":3,"b":4})],
        query: "where a > 1".to_string(),
    })
    .expect("query");
    assert_eq!(
        ql.get("rows")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0),
        1
    );
}

#[test]
fn window_optimizer_and_explain_v1_work() {
    let win = run_window_rows_v1(WindowRowsV1Req {
        run_id: Some("w1".to_string()),
        rows: vec![
            json!({"g":"A","t":"2024-01","v":10}),
            json!({"g":"A","t":"2024-02","v":20}),
            json!({"g":"A","t":"2024-03","v":30}),
        ],
        partition_by: Some(vec!["g".to_string()]),
        order_by: "t".to_string(),
        functions: vec![
            json!({"op":"row_number","as":"rn"}),
            json!({"op":"lag","field":"v","as":"prev_v","offset":1}),
            json!({"op":"moving_avg","field":"v","as":"ma2","window":2}),
        ],
    })
    .expect("window");
    assert_eq!(
        win.get("rows")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0),
        3
    );

    let opt = run_optimizer_v1(OptimizerV1Req {
        run_id: Some("o1".to_string()),
        rows: None,
        row_count_hint: Some(150_000),
        prefer_arrow: Some(true),
        join_hint: None,
        aggregate_hint: None,
    })
    .expect("optimizer");
    assert_eq!(
        opt.get("plan")
            .and_then(|p| p.get("execution_engine"))
            .and_then(|v| v.as_str()),
        Some("columnar_arrow_v1")
    );

    let exp = run_explain_plan_v1(ExplainPlanV1Req {
        run_id: Some("e1".to_string()),
        rows: Some(vec![json!({"id":1}), json!({"id":2})]),
        steps: vec![
            json!({"operator":"load_rows_v3"}),
            json!({"operator":"join_rows_v4"}),
            json!({"operator":"aggregate_rows_v4"}),
        ],
        actual_stats: None,
        persist_feedback: None,
    })
    .expect("explain");
    assert!(
        exp.get("estimated_total_cost")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0)
            > 0.0
    );
}
