use super::*;

#[test]
fn join_aggregate_quality_lineage_v4_family_work() {
    let j = run_join_rows_v4(JoinRowsV4Req {
        run_id: Some("j4".to_string()),
        left_rows: vec![
            json!({"id":"1","v":10}),
            json!({"id":"2","v":20}),
            json!({"id":"x","v":999}),
        ],
        right_rows: vec![json!({"id":"1","r":"A"}), json!({"id":"2","r":"B"})],
        left_on: json!(["id"]),
        right_on: json!(["id"]),
        join_type: Some("inner".to_string()),
        strategy: None,
        spill_path: None,
        chunk_size: None,
        enable_bloom: Some(true),
        bloom_field: None,
    })
    .expect("join4");
    assert_eq!(j.operator, "join_rows_v4");
    assert!(j.rows.len() >= 2);

    let a = run_aggregate_rows_v4(AggregateRowsV4Req {
        run_id: Some("a4".to_string()),
        rows: vec![
            json!({"g":"A","x":1}),
            json!({"g":"A","x":2}),
            json!({"g":"B","x":3}),
        ],
        group_by: vec!["g".to_string()],
        aggregates: vec![json!({"op":"count","as":"cnt"})],
        approx_sample_size: Some(128),
        verify_exact: Some(true),
        parallel_workers: None,
    })
    .expect("agg4");
    assert_eq!(a.operator, "aggregate_rows_v4");

    let q = run_quality_check_v4(QualityCheckV4Req {
        run_id: Some("q4".to_string()),
        rows: vec![
            json!({"v":1}),
            json!({"v":2}),
            json!({"v":3}),
            json!({"v":1000}),
        ],
        rules: json!({"anomaly_iqr":{"field":"v","max_ratio":0.20}}),
        rules_dsl: None,
    })
    .expect("qc4");
    assert_eq!(q.operator, "quality_check_v4");
    assert!(q.report.get("anomaly_iqr").is_some());

    let l = run_lineage_v3(LineageV3Req {
        run_id: Some("l3".to_string()),
        rules: Some(json!({})),
        computed_fields_v3: Some(vec![
            json!({"name":"z","expr":{"op":"add","args":[{"field":"a"},{"field":"b"}]}}),
        ]),
        workflow_steps: Some(vec![
            json!({"id":"s2","depends_on":["s1"],"operator":"transform_rows_v3"}),
        ]),
        rows: Some(vec![json!({"a":1,"b":2})]),
    })
    .expect("lineage3");
    assert_eq!(
        l.get("operator").and_then(|v| v.as_str()),
        Some("lineage_v3")
    );
    assert!(l.get("step_lineage").is_some());
}

#[test]
fn parquet_stream_udf_v2_work() {
    let now = utc_now_iso();
    let path = std::env::temp_dir().join(format!("aiwf_parquet_v2_{now}.parquet"));
    let p = path.to_string_lossy().to_string();
    let stream_key = format!("k1_{now}");
    let w = run_parquet_io_v2(ParquetIoV2Req {
        run_id: Some("p4".to_string()),
        op: "write".to_string(),
        path: p.clone(),
        rows: Some(vec![json!({"id":1,"txt":"a"}), json!({"id":2,"txt":"b"})]),
        parquet_mode: Some("typed".to_string()),
        limit: None,
        columns: None,
        predicate_field: None,
        predicate_eq: None,
        partition_by: None,
        compression: Some("gzip".to_string()),
        recursive: None,
        schema_mode: None,
    })
    .expect("parquet write");
    assert_eq!(w.get("ok").and_then(|v| v.as_bool()), Some(true));

    let r = run_parquet_io_v2(ParquetIoV2Req {
        run_id: Some("p4".to_string()),
        op: "read".to_string(),
        path: p.clone(),
        rows: None,
        parquet_mode: None,
        limit: Some(10),
        columns: Some(vec!["id".to_string()]),
        predicate_field: Some("id".to_string()),
        predicate_eq: Some(json!(2)),
        partition_by: None,
        compression: None,
        recursive: None,
        schema_mode: None,
    })
    .expect("parquet read");
    assert_eq!(
        r.get("rows")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0),
        1
    );

    let _ = run_stream_state_v2(StreamStateV2Req {
        run_id: Some("s2".to_string()),
        op: "delete".to_string(),
        stream_key: stream_key.clone(),
        state: None,
        offset: None,
        checkpoint_version: None,
        expected_version: None,
        backend: None,
        db_path: None,
        event_ts_ms: None,
        max_late_ms: None,
    });

    let s1 = run_stream_state_v2(StreamStateV2Req {
        run_id: Some("s2".to_string()),
        op: "save".to_string(),
        stream_key: stream_key.clone(),
        state: Some(json!({"x":1})),
        offset: Some(10),
        checkpoint_version: None,
        expected_version: Some(0),
        backend: None,
        db_path: None,
        event_ts_ms: None,
        max_late_ms: None,
    })
    .expect("stream save");
    assert_eq!(s1.get("version").and_then(|v| v.as_u64()), Some(1));
    let s2 = run_stream_state_v2(StreamStateV2Req {
        run_id: Some("s2".to_string()),
        op: "load".to_string(),
        stream_key: stream_key.clone(),
        state: None,
        offset: None,
        checkpoint_version: None,
        expected_version: None,
        backend: None,
        db_path: None,
        event_ts_ms: None,
        max_late_ms: None,
    })
    .expect("stream load");
    assert!(s2.get("value").is_some());

    let u = run_udf_wasm_v2(UdfWasmV2Req {
        run_id: Some("u2".to_string()),
        rows: vec![json!({"x":2}), json!({"x":3})],
        field: "x".to_string(),
        output_field: "y".to_string(),
        op: Some("double".to_string()),
        wasm_base64: Some("AGFzbQEAAA==".to_string()),
        max_output_bytes: Some(200_000),
        signed_token: None,
        allowed_ops: Some(vec!["double".to_string(), "identity".to_string()]),
    })
    .expect("udf2");
    assert_eq!(
        u.get("operator").and_then(|v| v.as_str()),
        Some("udf_wasm_v2")
    );

    let _ = run_stream_state_v2(StreamStateV2Req {
        run_id: Some("s2".to_string()),
        op: "delete".to_string(),
        stream_key,
        state: None,
        offset: None,
        checkpoint_version: None,
        expected_version: None,
        backend: None,
        db_path: None,
        event_ts_ms: None,
        max_late_ms: None,
    });
    let _ = fs::remove_file(path);
}

#[test]
fn aggregate_v4_parallel_and_approx_ops_work() {
    let rows = vec![
        json!({"g":"A","v":1.0,"k":"x"}),
        json!({"g":"A","v":2.0,"k":"x"}),
        json!({"g":"A","v":3.0,"k":"y"}),
        json!({"g":"B","v":10.0,"k":"m"}),
        json!({"g":"B","v":20.0,"k":"m"}),
    ];
    let out = run_aggregate_rows_v4(AggregateRowsV4Req {
        run_id: Some("agg-par".to_string()),
        rows,
        group_by: vec!["g".to_string()],
        aggregates: vec![
            json!({"op":"count","as":"cnt"}),
            json!({"op":"hll_count","field":"k","as":"hll"}),
            json!({"op":"tdigest_p90","field":"v","as":"p90"}),
            json!({"op":"topk_2","field":"k","as":"topk"}),
        ],
        approx_sample_size: Some(128),
        verify_exact: Some(false),
        parallel_workers: Some(2),
    })
    .expect("aggregate v4 parallel");
    assert_eq!(out.operator, "aggregate_rows_v4");
    assert_eq!(out.rows.len(), 2);
    assert_eq!(
        out.stats
            .get("parallel_workers")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        2
    );
}

#[test]
fn quality_dsl_and_stream_sqlite_and_plugin_registry_work() {
    let q = run_quality_check_v4(QualityCheckV4Req {
        run_id: Some("q-dsl".to_string()),
        rows: vec![
            json!({"id":"1","amount":10.0}),
            json!({"id":"1","amount":-2.0}),
        ],
        rules: json!({}),
        rules_dsl: Some("required:id,amount\nunique:id\nrange: amount >=0<=100".to_string()),
    })
    .expect("quality dsl");
    assert_eq!(q.operator, "quality_check_v4");
    assert!(!q.passed);

    let now = utc_now_iso();
    let sqlite = std::env::temp_dir().join(format!("aiwf_stream_{now}.sqlite"));
    let sqlite_path = sqlite.to_string_lossy().to_string();
    let s1 = run_stream_state_v2(StreamStateV2Req {
        run_id: Some("s-sqlite".to_string()),
        op: "save".to_string(),
        stream_key: "k-sqlite".to_string(),
        state: Some(json!({"x":1})),
        offset: Some(5),
        checkpoint_version: None,
        expected_version: Some(0),
        backend: Some("sqlite".to_string()),
        db_path: Some(sqlite_path.clone()),
        event_ts_ms: None,
        max_late_ms: None,
    })
    .expect("stream sqlite save");
    assert_eq!(s1.get("backend").and_then(|v| v.as_str()), Some("sqlite"));

    let s2 = run_stream_state_v2(StreamStateV2Req {
        run_id: Some("s-sqlite".to_string()),
        op: "load".to_string(),
        stream_key: "k-sqlite".to_string(),
        state: None,
        offset: None,
        checkpoint_version: None,
        expected_version: None,
        backend: Some("sqlite".to_string()),
        db_path: Some(sqlite_path.clone()),
        event_ts_ms: None,
        max_late_ms: None,
    })
    .expect("stream sqlite load");
    assert!(s2.get("value").is_some());
    let _ = fs::remove_file(sqlite);

    let plugin = run_plugin_registry_v1(PluginRegistryV1Req {
        run_id: Some("p-reg".to_string()),
        op: "register".to_string(),
        plugin: Some("demo_reg".to_string()),
        manifest: Some(json!({"name":"demo_reg","api_version":"v1","command":"cmd","args":[],"version":"1.0.0"})),
    })
    .expect("plugin register");
    assert_eq!(plugin.get("ok").and_then(|v| v.as_bool()), Some(true));
}

#[test]
fn columnar_stream_sketch_runtime_and_explain_feedback_work() {
    let c = run_columnar_eval_v1(ColumnarEvalV1Req {
        run_id: Some("c1".to_string()),
        rows: vec![
            json!({"id":"1","k":"A"}),
            json!({"id":"2","k":"B"}),
            json!({"id":"3","k":"A"}),
        ],
        select_fields: Some(vec!["id".to_string()]),
        filter_eq: Some(json!({"k":"A"})),
        limit: Some(10),
    })
    .expect("columnar");
    assert_eq!(
        c.get("operator").and_then(|v| v.as_str()),
        Some("columnar_eval_v1")
    );
    assert_eq!(
        c.get("rows").and_then(|v| v.as_array()).map(|a| a.len()),
        Some(2)
    );

    let now_ms = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()) as i64;
    let sw = run_stream_window_v1(StreamWindowV1Req {
        run_id: Some("sw1".to_string()),
        stream_key: "demo".to_string(),
        rows: vec![
            json!({"ts": now_ms - 1000, "value": 10, "g":"x"}),
            json!({"ts": now_ms - 500, "value": 20, "g":"x"}),
            json!({"ts": now_ms - 120000, "value": 30, "g":"x"}),
        ],
        event_time_field: "ts".to_string(),
        window_ms: 60_000,
        watermark_ms: Some(60_000),
        group_by: Some(vec!["g".to_string()]),
        value_field: Some("value".to_string()),
        trigger: Some("on_watermark".to_string()),
    })
    .expect("stream window");
    assert_eq!(
        sw.get("operator").and_then(|v| v.as_str()),
        Some("stream_window_v1")
    );
    assert_eq!(
        sw.get("stats")
            .and_then(|v| v.get("dropped_late"))
            .and_then(|v| v.as_u64()),
        Some(1)
    );

    let sw2 = run_stream_window_v2(StreamWindowV2Req {
        run_id: Some("sw2".to_string()),
        stream_key: "demo2".to_string(),
        rows: vec![
            json!({"ts": now_ms - 5000, "value": 10, "g":"x"}),
            json!({"ts": now_ms - 3000, "value": 20, "g":"x"}),
            json!({"ts": now_ms - 1000, "value": 30, "g":"x"}),
        ],
        event_time_field: "ts".to_string(),
        window_type: Some("sliding".to_string()),
        window_ms: 4000,
        slide_ms: Some(2000),
        session_gap_ms: None,
        watermark_ms: Some(60000),
        allowed_lateness_ms: Some(60000),
        group_by: Some(vec!["g".to_string()]),
        value_field: Some("value".to_string()),
        trigger: Some("on_watermark".to_string()),
        emit_late_side: Some(true),
    })
    .expect("stream window v2");
    assert_eq!(
        sw2.get("operator").and_then(|v| v.as_str()),
        Some("stream_window_v2")
    );

    let sk = run_sketch_v1(SketchV1Req {
        run_id: Some("sk1".to_string()),
        op: "create".to_string(),
        kind: Some("topk".to_string()),
        state: Some(json!({})),
        rows: Some(vec![json!({"k":"a"}), json!({"k":"a"}), json!({"k":"b"})]),
        field: Some("k".to_string()),
        topk_n: Some(2),
        merge_state: None,
    })
    .expect("sketch");
    assert_eq!(
        sk.get("operator").and_then(|v| v.as_str()),
        Some("sketch_v1")
    );

    let _ = run_runtime_stats_v1(RuntimeStatsV1Req {
        run_id: Some("rs1".to_string()),
        op: "record".to_string(),
        operator: Some("demo_op".to_string()),
        ok: Some(true),
        error_code: None,
        duration_ms: Some(12),
        rows_in: Some(10),
        rows_out: Some(9),
    })
    .expect("stats record");
    let sm = run_runtime_stats_v1(RuntimeStatsV1Req {
        run_id: Some("rs1".to_string()),
        op: "summary".to_string(),
        operator: None,
        ok: None,
        error_code: None,
        duration_ms: None,
        rows_in: None,
        rows_out: None,
    })
    .expect("stats summary");
    assert!(sm.get("items").and_then(|v| v.as_array()).is_some());

    let e = run_explain_plan_v1(ExplainPlanV1Req {
        run_id: Some("exp1".to_string()),
        steps: vec![json!({"operator":"join_rows_v4"})],
        rows: Some(vec![json!({"id":1})]),
        actual_stats: Some(vec![
            json!({"operator":"join_rows_v4","estimated_ms":10,"actual_ms":25}),
        ]),
        persist_feedback: Some(true),
    })
    .expect("explain feedback");
    assert_eq!(
        e.get("operator").and_then(|v| v.as_str()),
        Some("explain_plan_v1")
    );
}

#[test]
fn plugin_operator_v1_is_wired() {
    let res = run_plugin_operator_v1(PluginOperatorV1Req {
        run_id: Some("po1".to_string()),
        tenant_id: Some("default".to_string()),
        plugin: "demo".to_string(),
        op: Some("run".to_string()),
        payload: Some(json!({"x":1})),
    });
    assert!(res.is_err());
}

#[test]
#[ignore]
fn benchmark_new_ops_gate() {
    let loops = 10usize;
    let max_col_ms = std::env::var("AIWF_BENCH_MAX_COLUMNAR_MS")
        .ok()
        .and_then(|v| v.parse::<u128>().ok())
        .unwrap_or(1200);
    let max_stream_ms = std::env::var("AIWF_BENCH_MAX_STREAM_WINDOW_MS")
        .ok()
        .and_then(|v| v.parse::<u128>().ok())
        .unwrap_or(1200);
    let max_sketch_ms = std::env::var("AIWF_BENCH_MAX_SKETCH_MS")
        .ok()
        .and_then(|v| v.parse::<u128>().ok())
        .unwrap_or(1200);

    let rows = (0..20_000)
        .map(|i| json!({"id": i.to_string(), "k": if i % 2 == 0 { "A" } else { "B" }, "v": i as f64, "ts": 1_700_000_000_000i64 + i as i64}))
        .collect::<Vec<_>>();

    let t0 = Instant::now();
    for _ in 0..loops {
        let _ = run_columnar_eval_v1(ColumnarEvalV1Req {
            run_id: Some("bench-col".to_string()),
            rows: rows.clone(),
            select_fields: Some(vec!["id".to_string(), "k".to_string()]),
            filter_eq: Some(json!({"k":"A"})),
            limit: Some(5000),
        })
        .expect("bench columnar");
    }
    let col_ms = t0.elapsed().as_millis();

    let t1 = Instant::now();
    for _ in 0..loops {
        let _ = run_stream_window_v1(StreamWindowV1Req {
            run_id: Some("bench-stream".to_string()),
            stream_key: "bench".to_string(),
            rows: rows.clone(),
            event_time_field: "ts".to_string(),
            window_ms: 60_000,
            watermark_ms: Some(120_000),
            group_by: Some(vec!["k".to_string()]),
            value_field: Some("v".to_string()),
            trigger: Some("on_watermark".to_string()),
        })
        .expect("bench stream");
    }
    let stream_ms = t1.elapsed().as_millis();

    let t2 = Instant::now();
    for _ in 0..loops {
        let _ = run_sketch_v1(SketchV1Req {
            run_id: Some("bench-sk".to_string()),
            op: "update".to_string(),
            kind: Some("topk".to_string()),
            state: Some(json!({})),
            rows: Some(rows.clone()),
            field: Some("k".to_string()),
            topk_n: Some(5),
            merge_state: None,
        })
        .expect("bench sketch");
    }
    let sketch_ms = t2.elapsed().as_millis();

    assert!(
        col_ms <= max_col_ms,
        "columnar benchmark too slow: {col_ms}ms > {max_col_ms}ms"
    );
    assert!(
        stream_ms <= max_stream_ms,
        "stream window benchmark too slow: {stream_ms}ms > {max_stream_ms}ms"
    );
    assert!(
        sketch_ms <= max_sketch_ms,
        "sketch benchmark too slow: {sketch_ms}ms > {max_sketch_ms}ms"
    );
}

#[tokio::test]
async fn http_routes_for_new_ops_work() {
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
    let app = build_router(state);

    let req1 = Request::builder()
        .method("POST")
        .uri("/operators/stats_v1")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"rows":[{"x":1,"y":2},{"x":2,"y":4},{"x":3,"y":6}],"x_field":"x","y_field":"y"})
                .to_string(),
        ))
        .expect("stats req");
    let resp1 = app.clone().oneshot(req1).await.expect("stats resp");
    assert_eq!(resp1.status(), StatusCode::OK);

    let req2 = Request::builder()
        .method("POST")
        .uri("/operators/time_series_v1")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"rows":[{"t":"2024-01","v":1},{"t":"2024-02","v":2}],"time_field":"t","value_field":"v","window":2}).to_string(),
        ))
        .expect("ts req");
    let resp2 = app.clone().oneshot(req2).await.expect("ts resp");
    assert_eq!(resp2.status(), StatusCode::OK);

    let req3 = Request::builder()
        .method("GET")
        .uri("/metrics_v2/prom")
        .body(Body::empty())
        .expect("prom req");
    let resp3 = app.oneshot(req3).await.expect("prom resp");
    assert_eq!(resp3.status(), StatusCode::OK);
}

#[tokio::test]
async fn workflow_incremental_plan_reuses_http_transform_cache() {
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
    let app = build_router(state);

    let transform_input = json!({
        "run_id": "wf-cache-seed",
        "tenant_id": "default",
        "rows": [{"id":"1","amount":"10.1"}],
        "rules": {"casts":{"id":"int","amount":"float"}},
        "quality_gates": {}
    });
    let transform_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/operators/transform_rows_v2")
                .header("content-type", "application/json")
                .body(Body::from(transform_input.to_string()))
                .expect("transform request"),
        )
        .await
        .expect("transform response");
    assert_eq!(transform_resp.status(), StatusCode::OK);

    let workflow_req = json!({
        "run_id": "wf-cache-check",
        "tenant_id": "default",
        "steps": [{
            "id": "plan",
            "operator": "incremental_plan_v1",
            "input": {
                "run_id": "wf-plan",
                "operator": "transform_rows_v2",
                "input": transform_input
            }
        }]
    });
    let workflow_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflow/run")
                .header("content-type", "application/json")
                .body(Body::from(workflow_req.to_string()))
                .expect("workflow request"),
        )
        .await
        .expect("workflow response");
    assert_eq!(workflow_resp.status(), StatusCode::OK);
    let body = to_bytes(workflow_resp.into_body(), 1024 * 1024)
        .await
        .expect("workflow body");
    let v: Value = serde_json::from_slice(&body).expect("workflow json");
    assert_eq!(v.get("ok").and_then(|x| x.as_bool()), Some(true));
    assert_eq!(
        v.pointer("/context/plan/cache_hit")
            .and_then(|x| x.as_bool()),
        Some(true)
    );
    assert_eq!(
        v.pointer("/context/plan/strategy").and_then(|x| x.as_str()),
        Some("cache_reuse")
    );
}
