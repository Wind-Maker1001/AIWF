const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadWorkflowContractModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/workflow-contract.js")).href;
  return import(file);
}

test("workflow contract helper migrates missing version and rejects missing workflow id", async () => {
  const {
    NODE_CONFIG_SCHEMA_IDS,
    NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE,
    WORKFLOW_SCHEMA_VERSION,
    assertWorkflowContract,
    normalizeWorkflowContract,
  } = await loadWorkflowContractModule();

  const migrated = normalizeWorkflowContract({
    workflow_id: "wf_contract",
    nodes: [{ id: "n1", type: "ingest_files" }],
    edges: [],
  }, { allowVersionMigration: true });
  assert.equal(migrated.ok, true);
  assert.equal(migrated.graph.version, WORKFLOW_SCHEMA_VERSION);
  assert.equal(migrated.migrated, true);

  assert.throws(
    () => assertWorkflowContract({ version: WORKFLOW_SCHEMA_VERSION, nodes: [], edges: [] }),
    /workflow\.workflow_id is required/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_unknown_type",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "unknown_future_node" }],
      edges: [],
    }),
    /workflow contains unregistered node types/i,
  );
  try {
    assertWorkflowContract({
      workflow_id: "wf_unknown_type",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "unknown_future_node" }],
      edges: [],
    });
  } catch (error) {
    assert.ok(Array.isArray(error?.details?.error_items));
    assert.ok(error.details.error_items.some((item) => item.path === "workflow.nodes" && item.code === "unknown_node_type"));
  }

  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("manual_review"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("ai_refine"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("quality_check_v3"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("ai_strategy_v1"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("ai_audit"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("load_rows_v3"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("load_rows_v2"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("join_rows_v2"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("join_rows_v3"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("aggregate_rows_v2"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("aggregate_rows_v4"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("window_rows_v1"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("plugin_registry_v1"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("optimizer_v1"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("transform_rows_v3"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("lineage_v2"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("lineage_v3"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("rule_simulator_v1"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("constraint_solver_v1"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("parquet_io_v2"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("udf_wasm_v2"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("sql_chart_v1"));
  assert.ok(NODE_CONFIG_SCHEMA_IDS.includes("office_slot_fill_v1"));
  assert.equal(NODE_CONFIG_SCHEMA_IDS.length, 30);
  assert.equal(Object.keys(NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE).length, 30);
  assert.equal(NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE.ingest_files, "typed");
  assert.equal(NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE.load_rows_v3, "nested_shape_constrained");
  assert.equal(NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE.office_slot_fill_v1, "nested_shape_constrained");
  assert.equal(NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE.plugin_registry_v1, "nested_shape_constrained");
  assert.equal(NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE.optimizer_v1, "nested_shape_constrained");
  assert.equal(NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE.parquet_io_v2, "nested_shape_constrained");
  assert.equal(NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE.udf_wasm_v2, "nested_shape_constrained");
  assert.equal(NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE.lineage_v3, "nested_shape_constrained");
});

test("workflow contract helper validates high-frequency node config schema", async () => {
  const {
    WORKFLOW_SCHEMA_VERSION,
    assertWorkflowContract,
  } = await loadWorkflowContractModule();

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_manual_review_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "manual_review", config: { review_key: "" } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.review_key must not be empty/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_ai_refine_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "ai_refine", config: { reuse_existing: "yes" } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.reuse_existing must be a boolean/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_quality_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "quality_check_v4", config: { rules: [], rules_dsl: 1 } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.rules must be an object/i,
  );

  const accepted = assertWorkflowContract({
    workflow_id: "wf_schema_ok",
    version: WORKFLOW_SCHEMA_VERSION,
    nodes: [
      { id: "n1", type: "ingest_files", config: {} },
      { id: "n2", type: "manual_review", config: { review_key: "gate_1", default_approve: false } },
      { id: "n3", type: "ai_refine", config: { reuse_existing: true, allow_ai_on_data: false } },
      { id: "n4", type: "quality_check_v3", config: { rows: [], rules: {} } },
    ],
    edges: [],
  });
  assert.equal(accepted.ok, true);
});

test("workflow contract helper validates legacy rust data node config and array item schema", async () => {
  const {
    WORKFLOW_SCHEMA_VERSION,
    assertWorkflowContract,
  } = await loadWorkflowContractModule();

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_join_v2_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "join_rows_v2", config: { left_on: [""], right_on: ["id"] } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.left_on\[0\] must not be empty/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_agg_v2_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "aggregate_rows_v2", config: { aggregates: [{ op: "", as: "x" }] } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.aggregates\[0\]\.op is required/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_window_item_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "window_rows_v1", config: { order_by: "time", functions: [{ op: "row_number", as: "" }] } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.functions\[0\]\.as is required/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_plugin_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "plugin_registry_v1", config: { op: "", manifest: [] } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.op must not be empty/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_optimizer_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "optimizer_v1", config: { row_count_hint: -1, prefer_arrow: "yes" } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.row_count_hint must be >= 0/i,
  );

  const accepted = assertWorkflowContract({
    workflow_id: "wf_legacy_rust_ok",
    version: WORKFLOW_SCHEMA_VERSION,
    nodes: [
      { id: "n1", type: "load_rows_v2", config: { source_type: "csv", source: "demo.csv", limit: 100 } },
      { id: "n2", type: "join_rows_v2", config: { left_rows: [], right_rows: [], left_on: ["id"], right_on: ["id"], join_type: "inner" } },
      { id: "n3", type: "aggregate_rows_v2", config: { rows: [], group_by: ["region"], aggregates: [{ op: "count", as: "row_count" }] } },
      { id: "n4", type: "plugin_registry_v1", config: { op: "list", plugin: "", manifest: {} } },
      { id: "n5", type: "optimizer_v1", config: { rows: [], row_count_hint: 1000, prefer_arrow: true } },
    ],
    edges: [],
  });
  assert.equal(accepted.ok, true);
});

test("workflow contract helper validates nested object shape for covered node configs", async () => {
  const {
    WORKFLOW_SCHEMA_VERSION,
    assertWorkflowContract,
  } = await loadWorkflowContractModule();

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_rules_shape_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "quality_check_v3", config: { rows: [], rules: { required_columns: [1] } } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.rules\.required_columns\[0\] must be a string/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_slots_shape_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "office_slot_fill_v1", config: { template_kind: "pptx", template_version: "v1", required_slots: ["title", ""], slots: { chart_main: { categories: {}, series: [] } }, chart_source_node: "sql_chart_v1" } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.required_slots\[1\] must not be empty/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_manifest_shape_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "plugin_registry_v1", config: { op: "upsert", manifest: { capabilities: [1] } } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.manifest\.capabilities\[0\] must be a string/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_enum_shape_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "join_rows_v3", config: { left_rows: [], right_rows: [], left_on: ["id"], right_on: ["id"], join_type: "outer", strategy: "merge", chunk_size: 1000 } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.join_type must be one of: inner, left, right, full, semi, anti/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_source_type_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "load_rows_v2", config: { source_type: "xml", source: "demo.xml", limit: 10 } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.source_type must be one of: jsonl, csv, sqlite, sqlserver, parquet, txt, pdf, docx, xlsx, image/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_template_kind_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "office_slot_fill_v1", config: { template_kind: "pdf", template_version: "v1", required_slots: ["title"], slots: { title: "Demo" }, chart_source_node: "sql_chart_v1" } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.template_kind must be one of: docx, pptx, xlsx/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_manifest_command_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "plugin_registry_v1", config: { op: "register", plugin: "demo", manifest: { command: "", capabilities: ["run"] } } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.manifest\.command is required when workflow\.nodes\[0\]\.config\.op is register/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_transform_shape_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "transform_rows_v3", config: { rows: [], rules: {}, computed_fields_v3: [{ expr: "a+b" }] } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.computed_fields_v3\[0\] requires one of as\/name\/field/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_ai_strategy_shape_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "ai_strategy_v1", config: { providers: [{}], allow_ai_on_data: false } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.providers\[0\] requires one of name\/model\/endpoint/i,
  );

  const accepted = assertWorkflowContract({
    workflow_id: "wf_nested_shape_ok",
    version: WORKFLOW_SCHEMA_VERSION,
    nodes: [
      { id: "n1", type: "quality_check_v3", config: { rows: [], rules: { required_columns: ["amount"], unique_columns: ["id"] } } },
      { id: "n2", type: "office_slot_fill_v1", config: { template_kind: "pptx", template_version: "v1", required_slots: ["title", "chart_main"], slots: { title: "Demo", chart_main: { categories: [], series: [] } }, chart_source_node: "sql_chart_v1" } },
      { id: "n3", type: "plugin_registry_v1", config: { op: "upsert", plugin: "demo", manifest: { name: "demo", version: "v1", api_version: "v1", command: "demo.exe", capabilities: ["run"] } } },
      { id: "n4", type: "transform_rows_v3", config: { rows: [], rules: {}, computed_fields_v3: [{ as: "amount_double", expr: "amount*2" }] } },
      { id: "n5", type: "ai_strategy_v1", config: { providers: [{ name: "primary", endpoint: "https://example.invalid", model: "demo" }], allow_ai_on_data: false } },
    ],
    edges: [],
  });
  assert.equal(accepted.ok, true);
});

test("workflow contract helper validates analytics and runtime extension node config schema", async () => {
  const {
    WORKFLOW_SCHEMA_VERSION,
    assertWorkflowContract,
  } = await loadWorkflowContractModule();

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_lineage_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "lineage_v3", config: { rules: {}, computed_fields_v3: [], workflow_steps: [{ id: "", depends_on: ["s1"] }], rows: [] } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.workflow_steps\[0\]\.id is required/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_constraint_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "constraint_solver_v1", config: { rows: [], constraints: [{ kind: "sum_equals", left: ["a"], right: "", tolerance: -1 }] } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.constraints\[0\]\.right must not be empty/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_parquet_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "parquet_io_v2", config: { op: "compact", compression: "brotli", schema_mode: "replace" } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.op must be one of: write, save, read, load, inspect, inspect_schema, merge_small/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_udf_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "udf_wasm_v2", config: { rows: [], field: "", output_field: "", allowed_ops: [1] } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.allowed_ops\[0\] must be a string/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_optimizer_hint_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "optimizer_v1", config: { rows: [1], join_hint: [], aggregate_hint: { mode: "exact" } } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.rows\[0\] must be an object/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_load_connector_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "load_rows_v3", config: { source_type: "csv", source: "demo.csv", limit: 100, connector_options: [] } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.connector_options must be an object/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_parquet_predicate_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "parquet_io_v2", config: { op: "read", path: "demo.parquet", predicate_eq: 1 } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.predicate_field is required when workflow\.nodes\[0\]\.config\.predicate_eq is provided/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_udf_allowed_ops_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "udf_wasm_v2", config: { rows: [], op: "trim", allowed_ops: ["upper"], signed_token: "token" } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.op must be included in workflow\.nodes\[0\]\.config\.allowed_ops when both are provided/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_rule_sim_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "rule_simulator_v1", config: { rows: [], rules: [], candidate_rules: {} } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.rules must be an object/i,
  );

  const accepted = assertWorkflowContract({
    workflow_id: "wf_extension_ok",
    version: WORKFLOW_SCHEMA_VERSION,
    nodes: [
      { id: "n1", type: "lineage_v2", config: { rules: {}, computed_fields_v3: [{ as: "x2", expr: "x*2" }] } },
      { id: "n2", type: "lineage_v3", config: { rules: {}, computed_fields_v3: [{ as: "x2", expr: "x*2" }], workflow_steps: [{ id: "s2", depends_on: ["s1"], operator: "transform_rows_v3" }], rows: [] } },
      { id: "n3", type: "rule_simulator_v1", config: { rows: [], rules: {}, candidate_rules: {} } },
      { id: "n4", type: "constraint_solver_v1", config: { rows: [], constraints: [{ kind: "non_negative", field: "amount" }, { kind: "sum_equals", left: ["a", "b"], right: "total", tolerance: 0 }] } },
      { id: "n5", type: "parquet_io_v2", config: { op: "read", path: "demo.parquet", rows: [], parquet_mode: "typed", limit: 100, columns: ["id", "amount"], predicate_field: "id", predicate_eq: 2, compression: "snappy", schema_mode: "additive", recursive: true } },
      { id: "n6", type: "udf_wasm_v2", config: { rows: [{ name: "Alice" }], field: "name", output_field: "name_upper", op: "upper", wasm_base64: "", max_output_bytes: 1000000, signed_token: "token", allowed_ops: ["identity", "upper"] } },
    ],
    edges: [],
  });
  assert.equal(accepted.ok, true);
});

test("workflow contract helper validates rust data pipeline node config schema", async () => {
  const {
    WORKFLOW_SCHEMA_VERSION,
    assertWorkflowContract,
  } = await loadWorkflowContractModule();

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_join_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "join_rows_v4", config: { left_rows: {}, chunk_size: 0, enable_bloom: "yes" } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.left_rows must be an array/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_agg_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "aggregate_rows_v4", config: { rows: [], group_by: [], aggregates: {}, parallel_workers: 0 } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.aggregates must be an array/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_window_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "window_rows_v1", config: { order_by: "", functions: {} } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.order_by must not be empty/i,
  );

  const accepted = assertWorkflowContract({
    workflow_id: "wf_rust_chain_ok",
    version: WORKFLOW_SCHEMA_VERSION,
    nodes: [
      { id: "n1", type: "join_rows_v3", config: { left_rows: [], right_rows: [], left_on: ["id"], right_on: ["id"], join_type: "inner", strategy: "auto", chunk_size: 1000 } },
      { id: "n2", type: "join_rows_v4", config: { left_rows: [], right_rows: [], left_on: ["id"], right_on: ["id"], join_type: "left", strategy: "hash", chunk_size: 2000, enable_bloom: true } },
      { id: "n3", type: "aggregate_rows_v4", config: { rows: [], group_by: [], aggregates: [{ op: "count", as: "row_count" }], approx_sample_size: 1024, verify_exact: false, parallel_workers: 1 } },
      { id: "n4", type: "window_rows_v1", config: { rows: [], partition_by: ["region"], order_by: "time", functions: [{ op: "row_number", as: "row_no" }] } },
    ],
    edges: [],
  });
  assert.equal(accepted.ok, true);
});

test("workflow contract helper validates output-chain and loader node config schema", async () => {
  const {
    WORKFLOW_SCHEMA_VERSION,
    assertWorkflowContract,
  } = await loadWorkflowContractModule();

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_audit_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "ai_audit", config: { max_new_numbers: -1 } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.max_new_numbers must be >= 0/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_load_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "load_rows_v3", config: { source_type: "", limit: 0 } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.source_type must not be empty/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_load_resume_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "load_rows_v3", config: { source_type: "csv", source: "demo.csv", limit: 100, resume_token: "" } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.resume_token must not be empty/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_chart_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "sql_chart_v1", config: { rows: {}, top_n: 0 } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.rows must be an array/i,
  );

  assert.throws(
    () => assertWorkflowContract({
      workflow_id: "wf_slot_bad",
      version: WORKFLOW_SCHEMA_VERSION,
      nodes: [{ id: "n1", type: "office_slot_fill_v1", config: { template_kind: "", required_slots: {}, slots: [] } }],
      edges: [],
    }),
    /workflow\.nodes\[0\]\.config\.template_kind must not be empty/i,
  );

  const accepted = assertWorkflowContract({
    workflow_id: "wf_output_chain_ok",
    version: WORKFLOW_SCHEMA_VERSION,
    nodes: [
      { id: "n1", type: "load_rows_v3", config: { source_type: "csv", source: "demo.csv", query: "select * from data", limit: 100, max_retries: 1, retry_backoff_ms: 100, resume_token: "resume_1", connector_options: { connector: "local", credentials: { profile: "dev" } } } },
      { id: "n2", type: "sql_chart_v1", config: { rows: [], chart_type: "bar", category_field: "category", value_field: "value", series_field: "series", top_n: 5 } },
      { id: "n3", type: "office_slot_fill_v1", config: { template_kind: "pptx", template_version: "v1", required_slots: ["title"], slots: { title: "Demo" }, chart_source_node: "sql_chart_v1" } },
      { id: "n4", type: "ai_audit", config: { numeric_lock: true, citation_required: true, recalc_verify: true, max_new_numbers: 0, max_metric_delta: 0 } },
    ],
    edges: [],
  });
  assert.equal(accepted.ok, true);
});
