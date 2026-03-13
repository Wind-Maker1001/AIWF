use super::*;

#[test]
fn writes_valid_parquet_magic_bytes() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("aiwf_cleaned_{now}.parquet"));

    let rows = vec![
        CleanRow {
            id: 1,
            amount: 100.12,
        },
        CleanRow {
            id: 2,
            amount: 200.34,
        },
    ];
    write_cleaned_parquet(&path, &rows).expect("failed to write parquet");
    let bytes = fs::read(&path).expect("failed to read parquet");

    assert!(bytes.len() >= 8, "parquet file too small");
    assert_eq!(&bytes[0..4], b"PAR1", "invalid parquet header");
    assert_eq!(&bytes[bytes.len() - 4..], b"PAR1", "invalid parquet footer");

    let _ = fs::remove_file(path);
}

#[test]
fn load_and_clean_rows_supports_rules_object() {
    let params = json!({
        "rows": [
            {"ID": "1", "AMT": "10.4"},
            {"ID": "1", "AMT": "12.5"},
            {"ID": "2", "AMT": "-3"}
        ],
        "rules": {
            "id_field": "ID",
            "amount_field": "AMT",
            "drop_negative_amount": true,
            "deduplicate_by_id": true,
            "deduplicate_keep": "last",
            "amount_round_digits": 0
        }
    });
    let out = load_and_clean_rows(Some(&params)).expect("clean rows failed");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].id, 1);
    assert_eq!(out[0].amount, 13.0);
}

#[test]
fn load_and_clean_rows_applies_min_max_filters() {
    let params = json!({
        "rows": [
            {"id": 1, "amount": 5},
            {"id": 2, "amount": 50},
            {"id": 3, "amount": 500}
        ],
        "min_amount": 10,
        "max_amount": 100
    });
    let out = load_and_clean_rows(Some(&params)).expect("clean rows failed");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].id, 2);
    assert_eq!(out[0].amount, 50.0);
}

#[test]
fn load_and_clean_rows_rejects_missing_rows() {
    let params = json!({
        "min_amount": 10,
        "max_amount": 100
    });
    let err = load_and_clean_rows(Some(&params)).expect_err("missing rows should fail");
    assert!(err.contains("params.rows is required"));
}

#[test]
fn load_and_clean_rows_rejects_empty_rows() {
    let params = json!({
        "rows": []
    });
    let err = load_and_clean_rows(Some(&params)).expect_err("empty rows should fail");
    assert!(err.contains("params.rows is empty"));
}

#[test]
fn transform_rows_v2_works_for_basic_rules() {
    let req = TransformRowsReq {
        run_id: Some("r1".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![
            json!({"ID":"1","AMT":"10.5","tag":"A"}),
            json!({"ID":"1","AMT":"11.5","tag":"A"}),
            json!({"ID":"2","AMT":"-1","tag":"B"}),
        ]),
        rules: Some(json!({
            "rename_map": {"ID":"id","AMT":"amount"},
            "casts": {"id":"int","amount":"float"},
            "filters": [{"field":"amount","op":"gte","value":0}],
            "deduplicate_by": ["id"],
            "deduplicate_keep": "last",
            "sort_by": [{"field":"id","order":"asc"}]
        })),
        quality_gates: Some(json!({"min_output_rows":1, "max_invalid_rows":0})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("transform rows failed");
    assert_eq!(out.rows.len(), 1);
    assert!(out.rust_v2_used);
}

#[test]
fn transform_rows_v2_supports_in_regex_and_required_missing_gate() {
    let req = TransformRowsReq {
        run_id: Some("r2".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![
            json!({"name":"Alice","city":"beijing","claim_text":"tax policy support","source_url":"https://a"}),
            json!({"name":"Bob","city":"shanghai","claim_text":"tax policy oppose","source_url":""}),
        ]),
        rules: Some(json!({
            "filters": [
                {"field":"city","op":"in","value":["beijing","shanghai"]},
                {"field":"claim_text","op":"regex","value":"tax\\s+policy"}
            ]
        })),
        quality_gates: Some(json!({
            "required_fields": ["claim_text","source_url"],
            "max_required_missing_ratio": 0.5
        })),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("transform rows failed");
    assert_eq!(out.rows.len(), 2);
    assert!(
        out.quality
            .get("required_missing_ratio")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0)
            <= 0.5
    );
}

#[test]
fn router_builds_without_panicking() {
    let state = AppState {
        service: "accel-rust".to_string(),
        tasks: Arc::new(Mutex::new(HashMap::new())),
        metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
        task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
            ttl_sec: 3600,
            max_tasks: 1000,
            store_path: None,
            remote_enabled: false,
            backend: "base_api".to_string(),
            base_api_url: None,
            base_api_key: None,
            sql_host: "127.0.0.1".to_string(),
            sql_port: 1433,
            sql_db: "AIWF".to_string(),
            sql_user: None,
            sql_password: None,
            sql_use_windows_auth: false,
        })),
        cancel_flags: Arc::new(Mutex::new(HashMap::new())),
        tenant_running: Arc::new(Mutex::new(HashMap::new())),
        idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        transform_cache: Arc::new(Mutex::new(HashMap::new())),
        schema_registry: Arc::new(Mutex::new(HashMap::new())),
    };
    let _ = build_router(state);
}

#[test]
fn prune_tasks_respects_ttl_and_max() {
    let now = utc_now_iso().parse::<u64>().unwrap_or(0);
    let mut tasks = HashMap::new();
    tasks.insert(
        "old".to_string(),
        TaskState {
            task_id: "old".to_string(),
            tenant_id: "default".to_string(),
            operator: "transform_rows_v2".to_string(),
            status: "done".to_string(),
            created_at: now.saturating_sub(100).to_string(),
            updated_at: now.saturating_sub(100).to_string(),
            result: None,
            error: None,
            idempotency_key: "".to_string(),
            attempts: 0,
        },
    );
    tasks.insert(
        "new1".to_string(),
        TaskState {
            task_id: "new1".to_string(),
            tenant_id: "default".to_string(),
            operator: "transform_rows_v2".to_string(),
            status: "done".to_string(),
            created_at: now.saturating_sub(2).to_string(),
            updated_at: now.saturating_sub(2).to_string(),
            result: None,
            error: None,
            idempotency_key: "".to_string(),
            attempts: 0,
        },
    );
    tasks.insert(
        "new2".to_string(),
        TaskState {
            task_id: "new2".to_string(),
            tenant_id: "default".to_string(),
            operator: "transform_rows_v2".to_string(),
            status: "done".to_string(),
            created_at: now.saturating_sub(1).to_string(),
            updated_at: now.saturating_sub(1).to_string(),
            result: None,
            error: None,
            idempotency_key: "".to_string(),
            attempts: 0,
        },
    );
    let cfg = TaskStoreConfig {
        ttl_sec: 10,
        max_tasks: 1,
        store_path: None,
        remote_enabled: false,
        backend: "base_api".to_string(),
        base_api_url: None,
        base_api_key: None,
        sql_host: "127.0.0.1".to_string(),
        sql_port: 1433,
        sql_db: "AIWF".to_string(),
        sql_user: None,
        sql_password: None,
        sql_use_windows_auth: false,
    };
    let removed = prune_tasks(&mut tasks, &cfg);
    assert!(removed >= 2);
    assert_eq!(tasks.len(), 1);
    assert!(tasks.contains_key("new2"));
}

#[test]
fn can_cancel_only_for_queued_or_running() {
    assert!(can_cancel_status("queued"));
    assert!(can_cancel_status("running"));
    assert!(!can_cancel_status("done"));
    assert!(!can_cancel_status("failed"));
    assert!(!can_cancel_status("cancelled"));
}

#[tokio::test]
async fn async_submit_and_poll_task() {
    let state = AppState {
        service: "accel-rust".to_string(),
        tasks: Arc::new(Mutex::new(HashMap::new())),
        metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
        task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
            ttl_sec: 3600,
            max_tasks: 1000,
            store_path: None,
            remote_enabled: false,
            backend: "base_api".to_string(),
            base_api_url: None,
            base_api_key: None,
            sql_host: "127.0.0.1".to_string(),
            sql_port: 1433,
            sql_db: "AIWF".to_string(),
            sql_user: None,
            sql_password: None,
            sql_use_windows_auth: false,
        })),
        cancel_flags: Arc::new(Mutex::new(HashMap::new())),
        tenant_running: Arc::new(Mutex::new(HashMap::new())),
        idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        transform_cache: Arc::new(Mutex::new(HashMap::new())),
        schema_registry: Arc::new(Mutex::new(HashMap::new())),
    };
    let app = build_router(state);
    let submit_payload = json!({
        "run_id": "it-1",
        "rows": [
            {"id":"1","amount":"10.1"},
            {"id":"2","amount":"11.2"}
        ],
        "rules": {"casts":{"id":"int","amount":"float"}}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/operators/transform_rows_v2/submit")
                .header("content-type", "application/json")
                .body(Body::from(submit_payload.to_string()))
                .expect("submit request"),
        )
        .await
        .expect("submit response");
    assert_eq!(resp.status(), 200);
    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("submit body");
    let v: serde_json::Value = serde_json::from_slice(&body).expect("submit json");
    let task_id = v
        .get("task_id")
        .and_then(|x| x.as_str())
        .expect("task_id")
        .to_string();

    let mut last = String::new();
    for _ in 0..60 {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/tasks/{task_id}"))
                    .body(Body::empty())
                    .expect("task request"),
            )
            .await
            .expect("task response");
        assert_eq!(resp.status(), 200);
        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("task body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("task json");
        last = v
            .get("status")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        if last == "done" || last == "failed" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(last, "done");
}

#[tokio::test]
async fn metrics_include_transform_success_and_latency_aggregates() {
    let state = AppState {
        service: "accel-rust".to_string(),
        tasks: Arc::new(Mutex::new(HashMap::new())),
        metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
        task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
            ttl_sec: 3600,
            max_tasks: 1000,
            store_path: None,
            remote_enabled: false,
            backend: "base_api".to_string(),
            base_api_url: None,
            base_api_key: None,
            sql_host: "127.0.0.1".to_string(),
            sql_port: 1433,
            sql_db: "AIWF".to_string(),
            sql_user: None,
            sql_password: None,
            sql_use_windows_auth: false,
        })),
        cancel_flags: Arc::new(Mutex::new(HashMap::new())),
        tenant_running: Arc::new(Mutex::new(HashMap::new())),
        idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        transform_cache: Arc::new(Mutex::new(HashMap::new())),
        schema_registry: Arc::new(Mutex::new(HashMap::new())),
    };
    let app = build_router(state);
    let payload = json!({
        "run_id": "m1",
        "rows": [
            {"id":"1","amount":"10.1"},
            {"id":"2","amount":"11.2"}
        ],
        "rules": {"casts":{"id":"int","amount":"float"}}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/operators/transform_rows_v2")
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .expect("transform request"),
        )
        .await
        .expect("transform response");
    assert_eq!(resp.status(), 200);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("metrics response");
    assert_eq!(resp.status(), 200);
    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("metrics body");
    let text = String::from_utf8(body.to_vec()).expect("utf8 body");
    assert!(text.contains("aiwf_transform_rows_v2_success_total 1"));
    assert!(text.contains("aiwf_transform_rows_v2_latency_ms_sum "));
    assert!(text.contains("aiwf_transform_rows_v2_latency_ms_max "));
    assert!(text.contains("aiwf_transform_rows_v2_output_rows_sum 2"));
    assert!(text.contains("aiwf_tasks_active 0"));
}

#[tokio::test]
async fn cancel_task_endpoint_updates_status() {
    let state = AppState {
        service: "accel-rust".to_string(),
        tasks: Arc::new(Mutex::new(HashMap::new())),
        metrics: Arc::new(Mutex::new(ServiceMetrics::default())),
        task_cfg: Arc::new(Mutex::new(TaskStoreConfig {
            ttl_sec: 3600,
            max_tasks: 1000,
            store_path: None,
            remote_enabled: false,
            backend: "base_api".to_string(),
            base_api_url: None,
            base_api_key: None,
            sql_host: "127.0.0.1".to_string(),
            sql_port: 1433,
            sql_db: "AIWF".to_string(),
            sql_user: None,
            sql_password: None,
            sql_use_windows_auth: false,
        })),
        cancel_flags: Arc::new(Mutex::new(HashMap::new())),
        tenant_running: Arc::new(Mutex::new(HashMap::new())),
        idempotency_index: Arc::new(Mutex::new(HashMap::new())),
        transform_cache: Arc::new(Mutex::new(HashMap::new())),
        schema_registry: Arc::new(Mutex::new(HashMap::new())),
    };
    if let Ok(mut t) = state.tasks.lock() {
        t.insert(
            "task-cancel".to_string(),
            TaskState {
                task_id: "task-cancel".to_string(),
                tenant_id: "default".to_string(),
                operator: "transform_rows_v2".to_string(),
                status: "running".to_string(),
                created_at: utc_now_iso(),
                updated_at: utc_now_iso(),
                result: None,
                error: None,
                idempotency_key: "".to_string(),
                attempts: 0,
            },
        );
    }
    let app = build_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tasks/task-cancel/cancel")
                .body(Body::empty())
                .expect("cancel request"),
        )
        .await
        .expect("cancel response");
    assert_eq!(resp.status(), 200);
    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("cancel body");
    let v: serde_json::Value = serde_json::from_slice(&body).expect("cancel json");
    assert_eq!(v.get("cancelled").and_then(|x| x.as_bool()), Some(true));
    assert_eq!(v.get("status").and_then(|x| x.as_str()), Some("cancelled"));
}

#[test]
fn parquet_generic_roundtrip_rows() {
    let now = utc_now_iso();
    let path = std::env::temp_dir().join(format!("aiwf_rows_{now}.parquet"));
    let rows = vec![
        json!({"id":1,"text":"alpha"}),
        json!({"id":2,"text":"beta","score":9.5}),
    ];
    save_rows_parquet(path.to_string_lossy().as_ref(), &rows).expect("save parquet generic");
    let loaded =
        load_parquet_rows(path.to_string_lossy().as_ref(), 100).expect("load parquet generic");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].get("id").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(loaded[1].get("text").and_then(|v| v.as_str()), Some("beta"));
    let _ = fs::remove_file(path);
}

#[test]
fn workflow_run_records_failed_step_replay() {
    let req = WorkflowRunReq {
        run_id: Some("wf-failed".to_string()),
        trace_id: None,
        traceparent: None,
        tenant_id: None,
        context: None,
        steps: vec![json!({
            "id": "bad-op",
            "operator": "missing_operator",
            "input": {}
        })],
    };
    let out = run_workflow(req).expect("workflow response");
    assert!(!out.ok);
    assert_eq!(out.status, "failed");
    assert_eq!(out.failed_step.as_deref(), Some("bad-op"));
    assert_eq!(out.steps.len(), 1);
    assert_eq!(out.steps[0].status, "failed");
    assert!(
        out.steps[0]
            .error
            .as_deref()
            .unwrap_or("")
            .contains("unsupported")
    );
}

#[test]
fn aggregate_rows_v1_groups_and_metrics() {
    let req = AggregateRowsReq {
        run_id: Some("agg-1".to_string()),
        rows: vec![
            json!({"team":"A","amount":10}),
            json!({"team":"A","amount":20}),
            json!({"team":"B","amount":7}),
        ],
        group_by: vec!["team".to_string()],
        aggregates: vec![
            json!({"op":"count","as":"cnt"}),
            json!({"op":"sum","field":"amount","as":"sum_amount"}),
            json!({"op":"avg","field":"amount","as":"avg_amount"}),
        ],
    };
    let out = run_aggregate_rows_v1(req).expect("aggregate rows");
    assert_eq!(out.rows.len(), 2);
    let a = out
        .rows
        .iter()
        .find(|r| r.get("team").and_then(|v| v.as_str()) == Some("A"))
        .expect("team A");
    assert_eq!(a.get("cnt").and_then(|v| v.as_u64()), Some(2));
    assert_eq!(a.get("sum_amount").and_then(|v| v.as_f64()), Some(30.0));
}

#[test]
fn rules_package_publish_and_get_roundtrip() {
    let now = utc_now_iso();
    let name = format!("pkg_{now}");
    let version = "v1.0.0".to_string();
    let published = run_rules_package_publish_v1(RulesPackagePublishReq {
        name: name.clone(),
        version: version.clone(),
        dsl: Some("cast amount:float\nrequired amount".to_string()),
        rules: None,
    })
    .expect("publish package");
    assert!(published.ok);
    assert!(!published.fingerprint.is_empty());

    let fetched = run_rules_package_get_v1(RulesPackageGetReq {
        name: name.clone(),
        version: version.clone(),
    })
    .expect("get package");
    assert!(fetched.ok);
    assert_eq!(fetched.name, name);
    assert_eq!(fetched.version, version);
}
