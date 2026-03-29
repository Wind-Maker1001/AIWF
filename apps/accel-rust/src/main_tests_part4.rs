use super::*;

#[tokio::test]
async fn workflow_contract_validation_endpoint_returns_invalid_with_error_items() {
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
    let request = json!({
        "workflow_definition": {
            "workflow_id": "wf_invalid",
            "version": "1.0.0",
            "nodes": [{ "id": "n1", "type": "unknown_future_node" }],
            "edges": [],
        },
        "allow_version_migration": false,
        "require_non_empty_nodes": true,
        "validation_scope": "run",
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/operators/workflow_contract_v1/validate")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .expect("workflow contract validation request"),
        )
        .await
        .expect("workflow contract validation response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("workflow contract validation body");
    let payload: Value = serde_json::from_slice(&body).expect("workflow contract validation json");
    assert_eq!(payload.get("ok").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(payload.get("valid").and_then(|v| v.as_bool()), Some(false));
    assert_eq!(payload.get("status").and_then(|v| v.as_str()), Some("invalid"));
    assert_eq!(
        payload.pointer("/error_items/0/path").and_then(|v| v.as_str()),
        Some("workflow.nodes"),
    );
    assert_eq!(
        payload.pointer("/error_items/0/code").and_then(|v| v.as_str()),
        Some("unknown_node_type"),
    );
}

#[tokio::test]
async fn workflow_reference_run_endpoint_executes_cleaning_reference() {
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
    let bus_root = std::env::temp_dir().join(format!(
        "aiwf-accel-ref-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis()
    ));
    let _env = TestEnvGuard::set(&[("AIWF_BUS", bus_root.to_string_lossy().to_string())]);
    let request = json!({
        "job_id": "job_ref_1",
        "version_id": "ver_cleaning_compat_001",
        "published_version_id": "ver_cleaning_compat_001",
        "trace_id": "trace-ref",
        "job_context": {
            "job_root": bus_root.join("jobs").join("job_ref_1").to_string_lossy().to_string()
        },
        "params": {
            "rows": [{ "id": 1, "amount": 10.0 }],
            "office_outputs_enabled": false
        },
        "workflow_definition": {
            "workflow_id": "cleaning",
            "version": "workflow.v1",
            "nodes": [{ "id": "cleaning", "type": "cleaning", "config": {} }],
            "edges": []
        }
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/operators/workflow_reference_run_v1")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .expect("workflow reference run request"),
        )
        .await
        .expect("workflow reference run response");
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("workflow reference run body");
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let payload: Value = serde_json::from_slice(&body).expect("workflow reference run json");
    assert_eq!(payload.get("ok").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(payload.get("workflow_id").and_then(|v| v.as_str()), Some("cleaning"));
    assert_eq!(payload.get("version_id").and_then(|v| v.as_str()), Some("ver_cleaning_compat_001"));
    assert_eq!(
        payload.pointer("/execution/operator").and_then(|v| v.as_str()),
        Some("workflow_run"),
    );
    assert_eq!(
        payload.pointer("/final_output/operator").and_then(|v| v.as_str()),
        Some("cleaning"),
    );
    assert_eq!(
        payload.pointer("/final_output/outputs/cleaned_parquet/path").and_then(|v| v.as_str()).is_some(),
        true,
    );
}

#[tokio::test]
async fn workflow_reference_run_endpoint_returns_structured_invalid_contract() {
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
    let request = json!({
        "job_id": "job_ref",
        "version_id": "ver_invalid",
        "workflow_definition": {
            "workflow_id": "wf_invalid",
            "version": "1.0.0",
            "nodes": [{ "id": "n1", "type": "unknown_future_node" }],
            "edges": []
        }
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/operators/workflow_reference_run_v1")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .expect("workflow reference invalid request"),
        )
        .await
        .expect("workflow reference invalid response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("workflow reference invalid body");
    let payload: Value = serde_json::from_slice(&body).expect("workflow reference invalid json");
    assert_eq!(payload.get("ok").and_then(|v| v.as_bool()), Some(false));
    assert_eq!(payload.get("status").and_then(|v| v.as_str()), Some("invalid"));
    assert_eq!(
        payload.pointer("/error_items/0/code").and_then(|v| v.as_str()),
        Some("unknown_node_type"),
    );
}

#[tokio::test]
async fn workflow_reference_run_endpoint_executes_generic_workflow_step_plan() {
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
    let request = json!({
        "job_id": "job_caps_1",
        "version_id": "ver_caps_001",
        "workflow_definition": {
            "workflow_id": "wf_caps",
            "version": "1.0.0",
            "nodes": [{ "id": "caps", "type": "capabilities_v1", "config": {} }],
            "edges": []
        }
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/operators/workflow_reference_run_v1")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .expect("workflow reference generic request"),
        )
        .await
        .expect("workflow reference generic response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("workflow reference generic body");
    let payload: Value = serde_json::from_slice(&body).expect("workflow reference generic json");
    assert_eq!(payload.get("ok").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        payload.pointer("/compiled_plan/steps/0/operator").and_then(|v| v.as_str()),
        Some("capabilities_v1"),
    );
}

#[tokio::test]
async fn workflow_draft_run_endpoint_executes_desktop_linear_graph() {
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
    let request = json!({
        "job_id": "draft_run_1",
        "run_id": "draft_run_1",
        "job_context": { "job_root": "" },
        "params": { "input_files": "D:/docs/a.pdf" },
        "workflow_definition": {
            "workflow_id": "wf_draft",
            "version": "1.0.0",
            "nodes": [
                { "id": "n1", "type": "ingest_files", "config": {} },
                { "id": "n2", "type": "clean_md", "config": {} },
                { "id": "n3", "type": "compute_rust", "config": {} },
                { "id": "n4", "type": "ai_refine", "config": {} },
                { "id": "n5", "type": "ai_audit", "config": {} },
                { "id": "n6", "type": "md_output", "config": {} }
            ],
            "edges": [
                { "from": "n1", "to": "n2" },
                { "from": "n2", "to": "n3" },
                { "from": "n3", "to": "n4" },
                { "from": "n4", "to": "n5" },
                { "from": "n5", "to": "n6" }
            ]
        }
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/operators/workflow_draft_run_v1")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .expect("workflow draft run request"),
        )
        .await
        .expect("workflow draft run response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("workflow draft run body");
    let payload: Value = serde_json::from_slice(&body).expect("workflow draft run json");
    assert_eq!(payload.get("ok").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(payload.get("workflow_definition_source").and_then(|v| v.as_str()), Some("draft_inline"));
    assert_eq!(
        payload.pointer("/compiled_plan/ordered_node_ids/0").and_then(|v| v.as_str()),
        Some("n1"),
    );
    assert_eq!(
        payload.pointer("/final_output/operator").and_then(|v| v.as_str()),
        Some("md_output"),
    );
}

#[tokio::test]
async fn workflow_reference_run_endpoint_rejects_edge_missing_node() {
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
    let request = json!({
        "job_id": "job_edge_1",
        "version_id": "ver_edge_001",
        "workflow_definition": {
            "workflow_id": "wf_edge",
            "version": "1.0.0",
            "nodes": [{ "id": "caps", "type": "capabilities_v1", "config": {} }],
            "edges": [{ "from": "caps", "to": "missing" }]
        }
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/operators/workflow_reference_run_v1")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .expect("workflow reference edge invalid request"),
        )
        .await
        .expect("workflow reference edge invalid response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("workflow reference edge invalid body");
    let payload: Value = serde_json::from_slice(&body).expect("workflow reference edge invalid json");
    assert_eq!(payload.get("error").and_then(|v| v.as_str()), Some("edge to does not exist: missing"));
}

#[tokio::test]
async fn workflow_reference_run_endpoint_rejects_unsupported_when_semantics() {
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
    let request = json!({
        "job_id": "job_when_1",
        "version_id": "ver_when_001",
        "workflow_definition": {
            "workflow_id": "wf_when",
            "version": "1.0.0",
            "nodes": [
              { "id": "caps", "type": "capabilities_v1", "config": {} },
              { "id": "next", "type": "capabilities_v1", "config": {} }
            ],
            "edges": [{ "from": "caps", "to": "next", "when": { "field": "approved" } }]
        }
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/operators/workflow_reference_run_v1")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .expect("workflow reference when invalid request"),
        )
        .await
        .expect("workflow reference when invalid response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("workflow reference when invalid body");
    let payload: Value = serde_json::from_slice(&body).expect("workflow reference when invalid json");
    assert_eq!(
        payload.get("error").and_then(|v| v.as_str()),
        Some("edge.when type unsupported: caps->next"),
    );
}
