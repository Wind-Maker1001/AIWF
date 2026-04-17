use super::*;

#[test]
fn quality_check_v1_detects_duplicate_and_null_ratio() {
    let out = run_quality_check_v1(crate::QualityCheckReq {
        run_id: Some("q1".to_string()),
        rows: vec![
            json!({"id":"1","name":"A","score":10}),
            json!({"id":"1","name":null,"score":11}),
        ],
        rules: json!({
            "unique_fields": ["id"],
            "required_fields": ["name"],
            "max_null_ratio": 0.0
        }),
    })
    .expect("quality check");
    assert!(!out.passed);
    assert!(
        out.report
            .get("violations")
            .and_then(|v| v.as_array())
            .map(|v| !v.is_empty())
            .unwrap_or(false)
    );
}

#[test]
fn validate_where_clause_blocks_injection_tokens() {
    assert!(validate_where_clause("amount > 10").is_ok());
    assert!(validate_where_clause("amount >= 10 and city = 'beijing'").is_ok());
    assert!(validate_where_clause("1=1; drop table data").is_err());
    assert!(validate_where_clause("amount > 10 union select 1").is_err());
    assert!(validate_where_clause("amount > 10 and").is_err());
}

#[test]
fn tenant_max_concurrency_falls_back_to_four() {
    let _env_lock = test_env_lock().lock().expect("env lock");
    let _env_guard = TestEnvGuard::set(&[("AIWF_TENANT_MAX_CONCURRENCY", "".to_string())]);
    assert_eq!(crate::tenant_max_concurrency(), 4);
}

#[test]
fn try_acquire_tenant_slot_rejects_when_limit_reached() {
    let _env_lock = test_env_lock().lock().expect("env lock");
    let _env_guard = TestEnvGuard::set(&[("AIWF_TENANT_MAX_CONCURRENCY", "2".to_string())]);
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
    assert!(crate::try_acquire_tenant_slot(&state, "bench_async").is_ok());
    assert!(crate::try_acquire_tenant_slot(&state, "bench_async").is_ok());
    let err = crate::try_acquire_tenant_slot(&state, "bench_async")
        .err()
        .unwrap_or_default();
    assert!(err.contains("tenant concurrency exceeded"));
}

#[test]
fn transform_rows_v2_honors_cancel_flag() {
    let req = TransformRowsReq {
        run_id: Some("cancel-1".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![json!({"id":"1","amount":"10"})]),
        rules: Some(json!({"casts":{"id":"int","amount":"float"}})),
        quality_gates: Some(json!({})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let flag = Arc::new(AtomicBool::new(true));
    let res = run_transform_rows_v2_with_cancel(req, Some(flag));
    assert!(res.is_err());
    let err = res.err().unwrap_or_default();
    assert!(err.to_lowercase().contains("cancel"));
}

#[test]
fn plugin_exec_v1_is_disabled_by_default() {
    let _env_lock = test_env_lock().lock().expect("env lock");
    let _env_guard = TestEnvGuard::set(&[("AIWF_PLUGIN_ENABLE", "false".to_string())]);
    let res = run_plugin_exec_v1(PluginExecReq {
        run_id: Some("plugin-off".to_string()),
        tenant_id: Some("default".to_string()),
        trace_id: None,
        plugin: "demo".to_string(),
        input: json!({"x": 1}),
    });
    assert!(res.is_err());
    assert!(res.err().unwrap_or_default().contains("disabled"));
}

#[test]
fn save_rows_v1_rejects_invalid_table_identifier_for_sql_sinks() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    let sqlite_path = std::env::temp_dir().join(format!("aiwf_invalid_table_{now}.sqlite"));
    let bad_table = "data;drop table steps";

    let sqlite = crate::run_save_rows_v1(crate::SaveRowsReq {
        sink_type: "sqlite".to_string(),
        sink: sqlite_path.to_string_lossy().to_string(),
        table: Some(bad_table.to_string()),
        parquet_mode: None,
        rows: vec![json!({"id": 1})],
    });
    assert!(sqlite.is_err());
    assert!(
        sqlite
            .err()
            .unwrap_or_default()
            .contains("invalid sql identifier")
    );
    assert!(!sqlite_path.exists());

    let sqlserver = crate::run_save_rows_v1(crate::SaveRowsReq {
        sink_type: "sqlserver".to_string(),
        sink: "127.0.0.1/AIWF?windows_auth=true".to_string(),
        table: Some(bad_table.to_string()),
        parquet_mode: None,
        rows: vec![json!({"id": 1})],
    });
    assert!(sqlserver.is_err());
    assert!(
        sqlserver
            .err()
            .unwrap_or_default()
            .contains("invalid sql identifier")
    );
}

#[test]
fn plugin_operator_v1_treats_failed_exec_as_error() {
    let _env_lock = test_env_lock().lock().expect("env lock");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    let plugin = format!("fail_demo_{now}");
    let secret = format!("secret_{now}");
    let registry_path = std::env::temp_dir().join(format!("aiwf_plugin_registry_{now}.json"));
    let runtime_path = std::env::temp_dir().join(format!("aiwf_plugin_runtime_{now}.json"));
    let audit_path = std::env::temp_dir().join(format!("aiwf_plugin_audit_{now}.log"));
    let args = vec!["/C".to_string(), "exit 1".to_string()];
    let signature = plugin_signature(&secret, &plugin, "cmd", &args);
    let mut registry = Map::new();
    registry.insert(
        plugin.clone(),
        json!({
            "name": plugin,
            "version": "1.0.0",
            "api_version": "v1",
            "command": "cmd",
            "args": args,
            "signature": signature
        }),
    );
    fs::write(
        &registry_path,
        serde_json::to_string(&Value::Object(registry)).expect("registry json"),
    )
    .expect("write registry");
    let _env_guard = TestEnvGuard::set(&[
        ("AIWF_PLUGIN_ENABLE", "true".to_string()),
        ("AIWF_PLUGIN_ALLOWLIST", plugin.clone()),
        ("AIWF_PLUGIN_COMMAND_ALLOWLIST", "cmd".to_string()),
        ("AIWF_PLUGIN_SIGNING_SECRET", secret),
        (
            "AIWF_PLUGIN_REGISTRY_PATH",
            registry_path.to_string_lossy().to_string(),
        ),
        (
            "AIWF_PLUGIN_RUNTIME_PATH",
            runtime_path.to_string_lossy().to_string(),
        ),
        (
            "AIWF_PLUGIN_AUDIT_LOG",
            audit_path.to_string_lossy().to_string(),
        ),
        ("AIWF_PLUGIN_OPERATOR_CB_FAIL_THRESHOLD", "1".to_string()),
        ("AIWF_PLUGIN_OPERATOR_CB_OPEN_MS", "5000".to_string()),
    ]);
    if let Ok(mut running) = crate::plugin_tenant_running_map().lock() {
        running.clear();
    }

    let res = run_plugin_operator_v1(PluginOperatorV1Req {
        run_id: Some("po-fail".to_string()),
        tenant_id: Some("default".to_string()),
        plugin: plugin.clone(),
        op: Some("run".to_string()),
        payload: Some(json!({"x": 1})),
    });

    assert!(res.is_err());
    assert!(res.err().unwrap_or_default().contains("failed"));

    let runtime: Value =
        serde_json::from_str(&fs::read_to_string(&runtime_path).expect("read runtime store"))
            .expect("parse runtime store");
    let key = format!("default::{plugin}");
    let item = runtime
        .get(&key)
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert_eq!(item.get("fail_count").and_then(|v| v.as_u64()), Some(1));
    assert!(item.get("open_until").and_then(|v| v.as_u64()).unwrap_or(0) > 0);

    let audit = fs::read_to_string(&audit_path).expect("read audit log");
    assert!(audit.contains("\"status\":\"failed\""));

    if let Ok(mut running) = crate::plugin_tenant_running_map().lock() {
        running.clear();
    }
    let _ = fs::remove_file(registry_path);
    let _ = fs::remove_file(runtime_path);
    let _ = fs::remove_file(audit_path);
}

#[test]
fn load_rows_from_uri_limited_blocks_oversized_jsonl() {
    let now = utc_now_iso().replace(':', "_");
    let path = std::env::temp_dir().join(format!("aiwf_large_{now}.jsonl"));
    let mut f = fs::File::create(&path).expect("create temp jsonl");
    // Two medium lines, then force a very small byte quota to trigger limit rejection.
    writeln!(f, "{{\"id\":1,\"text\":\"abcdefghijk\"}}").expect("write line 1");
    writeln!(f, "{{\"id\":2,\"text\":\"mnopqrstuvw\"}}").expect("write line 2");
    drop(f);

    let err = load_rows_from_uri_limited(path.to_string_lossy().as_ref(), 100, 8)
        .err()
        .unwrap_or_default();
    assert!(err.contains("exceeds byte limit") || err.contains("exceeds"));
    let _ = fs::remove_file(path);
}

#[test]
fn transform_rows_v2_cache_hits_on_same_request() {
    let req = TransformRowsReq {
        run_id: Some("cache-1".to_string()),
        tenant_id: Some("default".to_string()),
        trace_id: None,
        traceparent: None,
        rows: Some(vec![json!({"id":"1","amount":"10.1"})]),
        rules: Some(json!({"casts":{"id":"int","amount":"float"}})),
        quality_gates: Some(json!({})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let cache = Arc::new(Mutex::new(HashMap::<String, TransformCacheEntry>::new()));
    let metrics = Arc::new(Mutex::new(ServiceMetrics::default()));
    let first = run_transform_rows_v2_with_cache(req.clone(), None, Some(&cache), Some(&metrics))
        .expect("first transform");
    let second = run_transform_rows_v2_with_cache(req, None, Some(&cache), Some(&metrics))
        .expect("second transform");
    assert!(first.ok && second.ok);
    let hit = second
        .audit
        .get("cache")
        .and_then(|v| v.get("hit"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(hit);
    assert!(second.audit.get("reason_samples").and_then(|v| v.as_object()).is_some());
}

#[test]
fn transform_rows_v2_audit_samples_respect_sample_limit() {
    let req = TransformRowsReq {
        run_id: Some("audit-samples-1".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![
            json!(1),
            json!({"id":"bad","amount":"oops"}),
            json!({"id":"also-bad","amount":"oops"}),
        ]),
        rules: Some(json!({
            "casts":{"id":"int","amount":"float"}
        })),
        quality_gates: Some(json!({})),
        schema_hint: Some(json!({"audit":{"sample_limit":1}})),
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("audit sample limit");
    let counts = out.audit.get("reason_counts").and_then(|v| v.as_object()).cloned().unwrap_or_default();
    let samples = out.audit.get("reason_samples").and_then(|v| v.as_object()).cloned().unwrap_or_default();
    assert_eq!(counts.get("invalid_object").and_then(|v| v.as_u64()).unwrap_or(0), 1);
    assert_eq!(counts.get("cast_failed").and_then(|v| v.as_u64()).unwrap_or(0), 2);
    assert_eq!(samples.get("invalid_object").and_then(|v| v.as_array()).map(|x| x.len()).unwrap_or(0), 1);
    assert_eq!(samples.get("cast_failed").and_then(|v| v.as_array()).map(|x| x.len()).unwrap_or(0), 1);
    let invalid_sample = samples
        .get("invalid_object")
        .and_then(|v| v.as_array())
        .and_then(|items| items.first())
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert_eq!(invalid_sample.get("reason_code").and_then(|v| v.as_str()).unwrap_or(""), "invalid_object");
    assert!(invalid_sample.get("raw_row").is_some());
}

#[test]
fn transform_rows_v2_audit_samples_cover_required_filter_and_dedup() {
    let req = TransformRowsReq {
        run_id: Some("audit-samples-2".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![
            json!({"id":"1","amount":"10","tag":"keep"}),
            json!({"id":"2","tag":"keep"}),
            json!({"id":"3","amount":"5","tag":"drop"}),
            json!({"id":"1","amount":"11","tag":"keep"}),
        ]),
        rules: Some(json!({
            "casts":{"id":"int","amount":"float","tag":"string"},
            "required_fields":["id","amount"],
            "filters":[{"field":"tag","op":"eq","value":"keep"}],
            "deduplicate_by":["id"],
            "deduplicate_keep":"first"
        })),
        quality_gates: Some(json!({"min_output_rows":1})),
        schema_hint: Some(json!({"audit":{"sample_limit":2}})),
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("audit reason coverage");
    let counts = out.audit.get("reason_counts").and_then(|v| v.as_object()).cloned().unwrap_or_default();
    let samples = out.audit.get("reason_samples").and_then(|v| v.as_object()).cloned().unwrap_or_default();
    assert_eq!(counts.get("required_missing").and_then(|v| v.as_u64()).unwrap_or(0), 1);
    assert_eq!(counts.get("filter_rejected").and_then(|v| v.as_u64()).unwrap_or(0), 1);
    assert_eq!(counts.get("duplicate_removed").and_then(|v| v.as_u64()).unwrap_or(0), 1);
    assert_eq!(samples.get("required_missing").and_then(|v| v.as_array()).map(|x| x.len()).unwrap_or(0), 1);
    assert_eq!(samples.get("filter_rejected").and_then(|v| v.as_array()).map(|x| x.len()).unwrap_or(0), 1);
    assert_eq!(samples.get("duplicate_removed").and_then(|v| v.as_array()).map(|x| x.len()).unwrap_or(0), 1);
    let required_sample = samples
        .get("required_missing")
        .and_then(|v| v.as_array())
        .and_then(|items| items.first())
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert_eq!(required_sample.get("reason_code").and_then(|v| v.as_str()).unwrap_or(""), "required_missing");
    assert!(required_sample.get("detail").and_then(|v| v.as_str()).unwrap_or("").contains("missing required fields"));
}

#[test]
fn transform_rows_v2_supports_columnar_engine_flag() {
    let req = TransformRowsReq {
        run_id: Some("col-1".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![
            json!({"id":"1","amount":"10.0","currency":"cny"}),
            json!({"id":"2","amount":"-1","currency":"cny"}),
        ]),
        rules: Some(json!({
            "execution_engine":"columnar_v1",
            "casts":{"id":"int","amount":"float","currency":"string"},
            "filters":[{"field":"amount","op":"gte","value":0}]
        })),
        quality_gates: Some(json!({"min_output_rows":1})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("columnar run");
    let engine = out
        .audit
        .get("engine")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(engine, "columnar_v1");
    assert_eq!(out.stats.output_rows, 1);
}

#[test]
fn columnar_dedup_and_sort_match_expectation() {
    let req = TransformRowsReq {
        run_id: Some("col-dedup-sort".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![
            json!({"id":"2","amount":"20","city":"bj"}),
            json!({"id":"1","amount":"10","city":"sh"}),
            json!({"id":"2","amount":"25","city":"bj"}),
        ]),
        rules: Some(json!({
            "execution_engine":"columnar_v1",
            "casts":{"id":"int","amount":"float"},
            "deduplicate_by":["id"],
            "deduplicate_keep":"last",
            "sort_by":[{"field":"id","order":"asc"}]
        })),
        quality_gates: Some(json!({"min_output_rows":1})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("columnar dedup sort");
    assert_eq!(out.stats.output_rows, 2);
    let rows = out.rows;
    assert_eq!(
        rows[0]
            .get("id")
            .and_then(|v| v.as_i64())
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        rows[1]
            .get("id")
            .and_then(|v| v.as_i64())
            .unwrap_or_default(),
        2
    );
    assert_eq!(
        rows[1]
            .get("amount")
            .and_then(|v| v.as_f64())
            .unwrap_or_default(),
        25.0
    );
}

#[test]
fn auto_engine_selects_columnar_for_medium_payload() {
    let mut rows = Vec::new();
    for i in 0..25000 {
        rows.push(json!({"id": i, "amount": "10.5"}));
    }
    let req = TransformRowsReq {
        run_id: Some("auto-eng-1".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(rows),
        rules: Some(json!({
            "execution_engine":"auto_v1",
            "casts":{"id":"int","amount":"float"},
            "filters":[{"field":"amount","op":"gte","value":0}]
        })),
        quality_gates: Some(json!({"min_output_rows":1})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("auto engine run");
    let eng = out
        .audit
        .get("engine")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(eng == "columnar_v1" || eng == "columnar_arrow_v1");
    let reason = out
        .audit
        .get("engine_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(reason.contains("auto:"));
}

#[test]
fn quality_gates_support_filtered_and_empty_constraints() {
    let quality = json!({
        "input_rows": 10,
        "output_rows": 0,
        "invalid_rows": 1,
        "filtered_rows": 6,
        "duplicate_rows_removed": 2,
        "required_missing_ratio": 0.0
    });
    let gates = json!({
        "max_filtered_rows": 5,
        "allow_empty_output": false
    });
    let out = evaluate_quality_gates(&quality, &gates);
    let passed = out.get("passed").and_then(|v| v.as_bool()).unwrap_or(true);
    assert!(!passed);
    let errors = out
        .get("errors")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(value_to_string)
        .collect::<Vec<String>>()
        .join(";");
    assert!(errors.contains("max_filtered_rows"));
    assert!(errors.contains("allow_empty_output"));
}

#[test]
fn transform_rows_v2_reports_extended_quality_metrics_and_gates() {
    let req = TransformRowsReq {
        run_id: Some("quality-metrics-1".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![
            json!({"id":"1","amount":"10.5","biz_date":"2026-03-01"}),
            json!({"id":"1","amount":"bad","biz_date":"bad-date"}),
            json!({}),
        ]),
        rules: Some(json!({
            "casts":{"id":"int","amount":"float"},
            "deduplicate_by":["id"],
            "deduplicate_keep":"last",
            "date_ops":[{"field":"biz_date","op":"parse_ymd","as":"biz_date_norm"}]
        })),
        quality_gates: Some(json!({
            "numeric_parse_rate_min": 0.5,
            "date_parse_rate_min": 0.5,
            "duplicate_key_ratio_max": 0.6,
            "blank_row_ratio_max": 0.6
        })),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("transform rows with extended quality metrics");
    assert!(out
        .quality
        .get("numeric_parse_rate")
        .and_then(|v| v.as_f64())
        .unwrap_or(-1.0)
        >= 0.0);
    assert!(out
        .quality
        .get("date_parse_rate")
        .and_then(|v| v.as_f64())
        .unwrap_or(-1.0)
        >= 0.0);
    assert!(out
        .quality
        .get("duplicate_key_ratio")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0) >= 0.0);
    assert!(out.quality.get("blank_row_ratio").and_then(|v| v.as_f64()).unwrap_or(1.0) <= 1.0);

    let req_fail = TransformRowsReq {
        run_id: Some("quality-metrics-fail".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![
            json!({"id":"1","amount":"10.5","biz_date":"2026-03-01"}),
            json!({"id":"2","amount":"bad","biz_date":"bad-date"}),
        ]),
        rules: Some(json!({
            "casts":{"id":"int","amount":"float"},
            "date_ops":[{"field":"biz_date","op":"parse_ymd","as":"biz_date_norm"}]
        })),
        quality_gates: Some(json!({
            "numeric_parse_rate_min": 0.9,
            "date_parse_rate_min": 0.9
        })),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let err = run_transform_rows_v2(req_fail).err().unwrap_or_default();
    assert!(err.contains("numeric_parse_rate") || err.contains("date_parse_rate"));
}

#[test]
fn join_rows_v2_supports_multi_key_and_full_join() {
    let out = run_join_rows_v2(JoinRowsV2Req {
        run_id: Some("jv2-1".to_string()),
        left_rows: vec![
            json!({"id":1,"k":"a","lv":10}),
            json!({"id":2,"k":"b","lv":20}),
        ],
        right_rows: vec![
            json!({"rid":9,"k":"a","rv":99}),
            json!({"rid":8,"k":"c","rv":88}),
        ],
        left_on: json!(["k"]),
        right_on: json!(["k"]),
        join_type: Some("full".to_string()),
    })
    .expect("join v2");
    assert!(out.rows.len() >= 3);
}

#[test]
fn aggregate_rows_v2_supports_stddev_and_percentile() {
    let out = run_aggregate_rows_v2(AggregateRowsV2Req {
        run_id: Some("agv2-1".to_string()),
        rows: vec![
            json!({"g":"x","amount":10.0}),
            json!({"g":"x","amount":20.0}),
            json!({"g":"x","amount":30.0}),
        ],
        group_by: vec!["g".to_string()],
        aggregates: vec![
            json!({"op":"stddev","field":"amount","as":"std"}),
            json!({"op":"percentile_p50","field":"amount","as":"p50"}),
        ],
    })
    .expect("agg v2");
    let row: Map<String, Value> = out
        .rows
        .first()
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert!(row.get("std").is_some());
    assert_eq!(
        row.get("p50").and_then(|v| v.as_f64()).unwrap_or_default(),
        20.0
    );
}

#[test]
fn schema_registry_infer_and_get_work() {
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
    let infer = run_schema_registry_infer_v1(
        &state,
        SchemaInferReq {
            name: Some("orders".to_string()),
            version: Some("v1".to_string()),
            rows: vec![json!({"id":"1","amount":"12.3","active":"true"})],
        },
    )
    .expect("infer");
    assert_eq!(infer.status, "done");
    let got = run_schema_registry_get_v1(
        &state,
        SchemaGetReq {
            name: "orders".to_string(),
            version: "v1".to_string(),
        },
    )
    .expect("get schema");
    assert!(got.schema.get("id").is_some());
}

#[test]
fn transform_rows_v2_supports_computed_fields() {
    let req = TransformRowsReq {
        run_id: Some("expr-1".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![json!({"price":"2","qty":"3","name":" A "})]),
        rules: Some(json!({
            "casts":{"price":"float","qty":"int","name":"string"},
            "computed_fields":{"total":"mul($price,$qty)"},
            "string_ops":[{"field":"name","op":"trim"},{"field":"name","op":"upper"}]
        })),
        quality_gates: Some(json!({"min_output_rows":1})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("expr transform");
    let row = out
        .rows
        .first()
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        row.get("total")
            .and_then(|v| v.as_f64())
            .unwrap_or_default(),
        6.0
    );
    assert_eq!(row.get("name").and_then(|v| v.as_str()).unwrap_or(""), "A");
}

#[test]
fn transform_rows_v2_supports_field_ops() {
    let req = TransformRowsReq {
        run_id: Some("field-op-1".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![json!({
            "speaker":" Alice  ",
            "biz_date":"20260301",
            "note":"visit https://example.com",
            "score":"9.66"
        })]),
        rules: Some(json!({
            "field_ops":[
                {"field":"speaker","op":"trim"},
                {"field":"speaker","op":"lower"},
                {"field":"biz_date","op":"parse_date"},
                {"field":"note","op":"remove_urls"},
                {"field":"score","op":"parse_number"},
                {"field":"score","op":"round_number","digits":1}
            ]
        })),
        quality_gates: Some(json!({"min_output_rows":1})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("field ops transform");
    let row = out
        .rows
        .first()
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert_eq!(row.get("speaker").and_then(|v| v.as_str()).unwrap_or(""), "alice");
    assert_eq!(row.get("biz_date").and_then(|v| v.as_str()).unwrap_or(""), "2026-03-01");
    assert_eq!(row.get("note").and_then(|v| v.as_str()).unwrap_or(""), "visit");
    assert_eq!(row.get("score").and_then(|v| v.as_f64()).unwrap_or_default(), 9.7);
    assert_eq!(
        out.audit.get("schema").and_then(|v| v.as_str()).unwrap_or(""),
        "transform_rows_v2.audit.v1"
    );
    assert!(out.audit.get("reason_counts").and_then(|v| v.as_object()).is_some());
    assert!(out.audit.get("reason_samples").and_then(|v| v.as_object()).is_some());
}

#[test]
fn transform_rows_v2_supports_table_cleaning_field_ops_and_row_filters() {
    let req = TransformRowsReq {
        run_id: Some("field-op-table-1".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![
            json!({
                "amount":"¥ 12.5",
                "phone":"138 0013 8000",
                "account_no":" 6222-0011 8899 "
            }),
            json!({"c1":"ID","c2":"Amount","c3":"Txn Date"}),
            json!({"note":"note: imported from OCR"}),
            json!({"amount":"Subtotal","note":"subtotal line"}),
            json!({"amount":"", "note":"   "}),
        ]),
        rules: Some(json!({
            "execution_engine":"columnar_v1",
            "filters":[
                {"op":"header_repeat_row","header_values":["id","amount","txn_date"],"min_matches":2},
                {"op":"note_row","keywords":["note"]},
                {"op":"subtotal_row","keywords":["subtotal"]},
                {"op":"blank_row"}
            ],
            "field_ops":[
                {"field":"amount","op":"strip_currency_symbol"},
                {"field":"amount","op":"parse_number"},
                {"field":"amount","op":"scale_by_header_unit","unit":"万元"},
                {"field":"phone","op":"normalize_phone_cn"},
                {"field":"account_no","op":"normalize_account_no"}
            ]
        })),
        quality_gates: Some(json!({"min_output_rows":1})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("table cleaning transform");
    let row = out
        .rows
        .first()
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(row.get("phone").and_then(|v| v.as_str()).unwrap_or(""), "+8613800138000");
    assert_eq!(row.get("account_no").and_then(|v| v.as_str()).unwrap_or(""), "622200118899");
    assert_eq!(row.get("amount").and_then(|v| v.as_f64()).unwrap_or_default(), 125000.0);
    assert_eq!(out.stats.filtered_rows, 4);
    assert_eq!(out.audit.get("engine").and_then(|v| v.as_str()).unwrap_or(""), "row_v1");
}

#[test]
fn transform_rows_v2_parse_number_keeps_full_width_comma_normalization() {
    let req = TransformRowsReq {
        run_id: Some("field-op-full-width-comma".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![json!({"amount":"1，234.56"})]),
        rules: Some(json!({
            "execution_engine":"columnar_v1",
            "field_ops":[
                {"field":"amount","op":"parse_number"}
            ]
        })),
        quality_gates: Some(json!({"min_output_rows":1})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("full-width comma parse_number transform");
    let row = out
        .rows
        .first()
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(row.get("amount").and_then(|v| v.as_f64()).unwrap_or_default(), 1234.56);
}

#[test]
fn transform_rows_v2_supports_survivorship_and_duplicate_explain() {
    let req = TransformRowsReq {
        run_id: Some("survivorship-1".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![
            json!({
                "account_no":"6222-0011",
                "phone":"",
                "biz_date":"2026-03-01"
            }),
            json!({
                "account_no":"62220011",
                "phone":"13800138000",
                "biz_date":"2026-03-02"
            }),
        ]),
        rules: Some(json!({
            "execution_engine":"columnar_v1",
            "deduplicate_by":["account_no"],
            "field_ops":[
                {"field":"account_no","op":"normalize_account_no"},
                {"field":"phone","op":"normalize_phone_cn"},
                {"field":"biz_date","op":"parse_date"}
            ],
            "survivorship":{
                "keys":["account_no"],
                "prefer_non_null_fields":["phone"],
                "prefer_latest_fields":["biz_date"],
                "tie_breaker":"last"
            }
        })),
        quality_gates: Some(json!({"min_output_rows":1})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("survivorship transform");
    let row = out
        .rows
        .first()
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let samples = out
        .audit
        .get("reason_samples")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let duplicate_sample = samples
        .get("duplicate_removed")
        .and_then(|v| v.as_array())
        .and_then(|items| items.first())
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(row.get("phone").and_then(|v| v.as_str()).unwrap_or(""), "+8613800138000");
    assert_eq!(row.get("biz_date").and_then(|v| v.as_str()).unwrap_or(""), "2026-03-02");
    assert!(out.quality.get("survivorship_applied").and_then(|v| v.as_bool()).unwrap_or(false));
    assert_eq!(out.audit.get("engine").and_then(|v| v.as_str()).unwrap_or(""), "row_v1");
    assert_eq!(duplicate_sample.get("winner_row_id").and_then(|v| v.as_u64()).unwrap_or(0), 2);
    assert_eq!(duplicate_sample.get("loser_row_id").and_then(|v| v.as_u64()).unwrap_or(0), 1);
    assert!(
        duplicate_sample
            .get("decision_basis")
            .and_then(|v| v.as_array())
            .map(|items| items.iter().any(|item| item.as_str() == Some("prefer_non_null_fields:phone")))
            .unwrap_or(false)
    );
}

#[test]
fn transform_rows_v2_applies_survivorship_keys_without_deduplicate_by() {
    let req = TransformRowsReq {
        run_id: Some("survivorship-keys-only".to_string()),
        tenant_id: None,
        trace_id: None,
        traceparent: None,
        rows: Some(vec![
            json!({
                "account_no":"6222-0011",
                "phone":"",
                "biz_date":"2026-03-01"
            }),
            json!({
                "account_no":"62220011",
                "phone":"13800138000",
                "biz_date":"2026-03-02"
            }),
        ]),
        rules: Some(json!({
            "execution_engine":"columnar_v1",
            "field_ops":[
                {"field":"account_no","op":"normalize_account_no"},
                {"field":"phone","op":"normalize_phone_cn"},
                {"field":"biz_date","op":"parse_date"}
            ],
            "survivorship":{
                "keys":["account_no"],
                "prefer_non_null_fields":["phone"],
                "prefer_latest_fields":["biz_date"],
                "tie_breaker":"last"
            }
        })),
        quality_gates: Some(json!({"min_output_rows":1})),
        schema_hint: None,
        rules_dsl: None,
        input_uri: None,
        output_uri: None,
        request_signature: None,
        idempotency_key: None,
    };
    let out = run_transform_rows_v2(req).expect("survivorship keys only transform");
    assert_eq!(out.rows.len(), 1);
    assert_eq!(
        out.quality.get("duplicate_rows_removed").and_then(|v| v.as_u64()).unwrap_or(0),
        1
    );
    assert!(out.quality.get("survivorship_applied").and_then(|v| v.as_bool()).unwrap_or(false));
}

#[test]
fn workflow_supports_schema_registry_ops() {
    let run_id = format!(
        "wf-schema-{}",
        utc_now_iso().replace(":", "").replace("-", "")
    );
    let wf = WorkflowRunReq {
        run_id: Some(run_id),
        trace_id: None,
        traceparent: None,
        tenant_id: Some("local".to_string()),
        context: Some(json!({})),
        steps: vec![
            json!({
                "id":"infer",
                "operator":"schema_registry_v1_infer",
                "input":{"name":"wf_orders","version":"v1","rows":[{"id":"1","amount":"12.3"}]}
            }),
            json!({
                "id":"get",
                "operator":"schema_registry_v1_get",
                "input":{"name":"wf_orders","version":"v1"}
            }),
        ],
    };
    let out = run_workflow(wf).expect("workflow schema ops");
    assert!(out.ok);
    let get_step = out
        .context
        .as_object()
        .and_then(|m| m.get("get"))
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        get_step
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        "done"
    );
}
