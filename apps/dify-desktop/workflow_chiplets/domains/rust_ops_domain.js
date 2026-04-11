const {
  DESKTOP_RUST_OPERATOR_TYPES,
  assertDesktopRustOperator,
} = require("./rust_operator_manifest.generated");

function registerRustOpsDomainChiplets(registry, deps, helpers) {
  const { runIsolatedTask } = deps;
  const {
    rustBase,
    rustRequired,
    resolveIsolationLevel,
    resolveSandboxLimits,
  } = helpers;
  const registeredTypes = [];
  const registerChiplet = registry.register.bind(registry);

  function isTruthy(v) {
    const s = String(v || "").trim().toLowerCase();
    return s === "1" || s === "true" || s === "yes" || s === "on";
  }

  function isLocalEndpoint(raw) {
    const s = String(raw || "").trim();
    if (!s) return true;
    try {
      const u = new URL(s);
      const host = String(u.hostname || "").toLowerCase();
      return host === "127.0.0.1" || host === "localhost" || host === "::1";
    } catch {
      return false;
    }
  }

  function canUseNetworkEgress() {
    return isTruthy(process.env.AIWF_ALLOW_EGRESS) || isTruthy(process.env.AIWF_ALLOW_CLOUD_LLM);
  }

  async function callRustOperator(base, operatorPath, body, required, timeoutMs, operatorName) {
    if (!isLocalEndpoint(base) && !canUseNetworkEgress()) {
      throw new Error("rust_egress_blocked");
    }
    const resp = await fetch(`${base}${operatorPath}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const txt = await resp.text();
    if (!resp.ok) throw new Error(`rust_http_${resp.status}:${txt.slice(0, 240)}`);
    const out = txt ? JSON.parse(txt) : {};
    return {
      ok: !!out.ok,
      operator: out.operator || operatorName,
      status: out.status || "done",
      detail: out,
      isolated: false,
      isolation_error: "",
    };
  }

  function makeRustNodeChiplet(type, operatorPath, defaults = {}, priority = 85) {
    return {
      id: `chiplet.${type}.v1`,
      priority,
      timeout_ms: Number(process.env.AIWF_CHIPLET_RUST_NODE_TIMEOUT_MS || 180000),
      retries: Number(process.env.AIWF_CHIPLET_RUST_NODE_RETRIES || 1),
      async run(ctx, node) {
        const base = rustBase(ctx);
        const required = rustRequired(ctx);
        const cfg = node?.config && typeof node.config === "object" ? node.config : {};
        const body = {
          run_id: ctx.runId,
          ...(defaults || {}),
          ...cfg,
        };
        if (type === "plugin_operator_v1") {
          const allow = String(process.env.AIWF_PLUGIN_ALLOWLIST || "")
            .split(",")
            .map((x) => x.trim())
            .filter(Boolean);
          if (allow.length > 0) {
            const pluginName = String(body.plugin || "").trim();
            if (!allow.includes(pluginName)) {
              throw new Error(`plugin_not_allowed:${pluginName}`);
            }
          }
        }
        try {
          const isolationLevel = resolveIsolationLevel(ctx, type, true, node);
          if (isolationLevel !== "none") {
            try {
              const isolatedOut = await runIsolatedTask("rust_operator_http", {
                base,
                operator_path: operatorPath,
                operator_name: type,
                body,
                required,
                isolation_level: isolationLevel,
                sandbox_limits: resolveSandboxLimits(ctx, node),
              }, Number(process.env.AIWF_CHIPLET_RUST_NODE_TIMEOUT_MS || 180000));
              return {
                ...isolatedOut,
                isolated: true,
                isolation_level: isolationLevel,
                isolation_error: "",
              };
            } catch (isolationErr) {
              if (isolationLevel === "sandbox") throw isolationErr;
              const out = await callRustOperator(base, operatorPath, body, required, Number(process.env.AIWF_CHIPLET_RUST_NODE_TIMEOUT_MS || 180000), type);
              return {
                ...out,
                isolated: false,
                isolation_level: "none",
                isolation_error: String(isolationErr),
              };
            }
          }
          return await callRustOperator(base, operatorPath, body, required, Number(process.env.AIWF_CHIPLET_RUST_NODE_TIMEOUT_MS || 180000), type);
        } catch (e) {
          if (required) throw e;
          return {
            ok: false,
            operator: type,
            status: "fallback",
            detail: String(e),
            isolated: false,
            isolation_level: "none",
            isolation_error: "",
          };
        }
      },
    };
  }

  function registerManifestRustChiplet(type, operatorPathOrChiplet, defaults = {}, priority = 85) {
    assertDesktopRustOperator(type);
    registeredTypes.push(type);
    if (typeof operatorPathOrChiplet === "string") {
      registerChiplet(type, makeRustNodeChiplet(type, operatorPathOrChiplet, defaults, priority));
      return;
    }
    registerChiplet(type, operatorPathOrChiplet);
  }

  function assertRegistrationSurface() {
    const actual = Array.from(new Set(registeredTypes)).sort();
    const missing = DESKTOP_RUST_OPERATOR_TYPES.filter((type) => !actual.includes(type));
    const stale = actual.filter((type) => !DESKTOP_RUST_OPERATOR_TYPES.includes(type));
    if (missing.length > 0 || stale.length > 0) {
      const fragments = [];
      if (missing.length > 0) {
        fragments.push(`missing manifest-authorized rust operators: ${missing.join(", ")}`);
      }
      if (stale.length > 0) {
        fragments.push(`registered rust operators outside manifest: ${stale.join(", ")}`);
      }
      throw new Error(fragments.join("; "));
    }
  }

  registerManifestRustChiplet(
    "transform_rows_v3",
    "/operators/transform_rows_v3",
    {
      rows: [],
      rules: {},
      computed_fields_v3: [],
    },
    89,
  );
  registerManifestRustChiplet(
    "postprocess_rows_v1",
    "/operators/postprocess_rows_v1",
    {
      rows: [],
      standardize_evidence: false,
      evidence_schema: {},
      chunk_mode: "none",
      chunk_field: "",
      chunk_max_chars: 500,
      detect_conflicts: false,
      conflict_topic_field: "",
      conflict_stance_field: "",
      conflict_text_field: "",
      conflict_positive_words: [],
      conflict_negative_words: [],
      schema_hint: {},
    },
    88,
  );
  registerManifestRustChiplet(
    "join_rows_v2",
    "/operators/join_rows_v2",
    {
      left_rows: [],
      right_rows: [],
      left_on: ["id"],
      right_on: ["id"],
      join_type: "inner",
    },
    87,
  );
  registerManifestRustChiplet(
    "join_rows_v3",
    "/operators/join_rows_v3",
    {
      left_rows: [],
      right_rows: [],
      left_on: ["id"],
      right_on: ["id"],
      join_type: "inner",
      strategy: "auto",
      chunk_size: 50000,
    },
    86,
  );
  registerManifestRustChiplet(
    "join_rows_v4",
    "/operators/join_rows_v4",
    {
      left_rows: [],
      right_rows: [],
      left_on: ["id"],
      right_on: ["id"],
      join_type: "inner",
      strategy: "auto",
      chunk_size: 50000,
      enable_bloom: true,
    },
    85,
  );
  registerManifestRustChiplet(
    "aggregate_rows_v2",
    "/operators/aggregate_rows_v2",
    {
      rows: [],
      group_by: [],
      aggregates: [{ op: "count", as: "row_count" }],
    },
    84,
  );
  registerManifestRustChiplet(
    "aggregate_rows_v3",
    "/operators/aggregate_rows_v3",
    {
      rows: [],
      group_by: [],
      aggregates: [{ op: "count", as: "row_count" }],
      approx_sample_size: 1024,
    },
    83,
  );
  registerManifestRustChiplet(
    "aggregate_rows_v4",
    "/operators/aggregate_rows_v4",
    {
      rows: [],
      group_by: [],
      aggregates: [{ op: "count", as: "row_count" }],
      approx_sample_size: 1024,
      verify_exact: false,
      parallel_workers: 1,
    },
    82,
  );
  registerManifestRustChiplet(
    "quality_check_v2",
    "/operators/quality_check_v2",
    {
      rows: [],
      rules: {},
    },
    81,
  );
  registerManifestRustChiplet(
    "quality_check_v3",
    "/operators/quality_check_v3",
    {
      rows: [],
      rules: {},
    },
    80,
  );
  registerManifestRustChiplet(
    "quality_check_v4",
    "/operators/quality_check_v4",
    {
      rows: [],
      rules: {},
      rules_dsl: "",
    },
    79,
  );
  registerManifestRustChiplet(
    "plugin_registry_v1",
    makeRustNodeChiplet("plugin_registry_v1", "/operators/plugin_registry_v1", {
      op: "list",
      plugin: "",
      manifest: {},
    }, 79),
  );
  registerManifestRustChiplet(
    "window_rows_v1",
    makeRustNodeChiplet("window_rows_v1", "/operators/window_rows_v1", {
      rows: [],
      partition_by: [],
      order_by: "time",
      functions: [{ op: "row_number", as: "row_no" }],
    }, 79),
  );
  registerManifestRustChiplet(
    "optimizer_v1",
    makeRustNodeChiplet("optimizer_v1", "/operators/optimizer_v1", {
      rows: [],
      row_count_hint: 10000,
      prefer_arrow: true,
    }, 78),
  );
  registerManifestRustChiplet(
    "load_rows_v2",
    makeRustNodeChiplet("load_rows_v2", "/operators/load_rows_v2", {
      source_type: "csv",
      source: "",
      limit: 10000,
    }, 77),
  );
  registerManifestRustChiplet(
    "load_rows_v3",
    makeRustNodeChiplet("load_rows_v3", "/operators/load_rows_v3", {
      source_type: "csv",
      source: "",
      limit: 10000,
      max_retries: 2,
      retry_backoff_ms: 150,
    }, 76),
  );
  registerManifestRustChiplet(
    "schema_registry_v1_register",
    makeRustNodeChiplet("schema_registry_v1_register", "/operators/schema_registry_v1/register", {
      name: "default_schema",
      version: "v1",
      schema: {},
    }, 80),
  );
  registerManifestRustChiplet(
    "schema_registry_v1_get",
    makeRustNodeChiplet("schema_registry_v1_get", "/operators/schema_registry_v1/get", {
      name: "default_schema",
      version: "v1",
    }, 79),
  );
  registerManifestRustChiplet(
    "schema_registry_v1_infer",
    makeRustNodeChiplet("schema_registry_v1_infer", "/operators/schema_registry_v1/infer", {
      name: "default_schema",
      version: "v1",
      rows: [],
    }, 78),
  );
  registerManifestRustChiplet(
    "schema_registry_v2_check_compat",
    makeRustNodeChiplet("schema_registry_v2_check_compat", "/operators/schema_registry_v2/check_compat", {
      name: "default_schema",
      from_version: "v1",
      to_version: "v2",
      mode: "backward",
    }, 77),
  );
  registerManifestRustChiplet(
    "schema_registry_v2_suggest_migration",
    makeRustNodeChiplet("schema_registry_v2_suggest_migration", "/operators/schema_registry_v2/suggest_migration", {
      name: "default_schema",
      from_version: "v1",
      to_version: "v2",
    }, 76),
  );
  registerManifestRustChiplet(
    "udf_wasm_v1",
    makeRustNodeChiplet("udf_wasm_v1", "/operators/udf_wasm_v1/apply", {
      rows: [],
      field: "",
      output_field: "",
      op: "identity",
    }, 75),
  );
  registerManifestRustChiplet(
    "udf_wasm_v2",
    makeRustNodeChiplet("udf_wasm_v2", "/operators/udf_wasm_v2/apply", {
      rows: [],
      field: "",
      output_field: "",
      op: "identity",
      wasm_base64: "",
      max_output_bytes: 1000000,
      allowed_ops: ["identity", "double", "negate", "trim", "upper"],
    }, 74),
  );
  registerManifestRustChiplet(
    "time_series_v1",
    makeRustNodeChiplet("time_series_v1", "/operators/time_series_v1", {
      rows: [],
      time_field: "month",
      value_field: "value",
      group_by: [],
      window: 3,
    }, 73),
  );
  registerManifestRustChiplet(
    "stats_v1",
    makeRustNodeChiplet("stats_v1", "/operators/stats_v1", {
      rows: [],
      x_field: "x",
      y_field: "y",
    }, 72),
  );
  registerManifestRustChiplet(
    "entity_linking_v1",
    makeRustNodeChiplet("entity_linking_v1", "/operators/entity_linking_v1", {
      rows: [],
      field: "entity",
      id_field: "entity_id",
    }, 72),
  );
  registerManifestRustChiplet(
    "table_reconstruct_v1",
    makeRustNodeChiplet("table_reconstruct_v1", "/operators/table_reconstruct_v1", {
      lines: [],
      delimiter: "\\s{2,}|\\t",
    }, 71),
  );
  registerManifestRustChiplet(
    "feature_store_v1_upsert",
    makeRustNodeChiplet("feature_store_v1_upsert", "/operators/feature_store_v1/upsert", {
      key_field: "id",
      rows: [],
    }, 70),
  );
  registerManifestRustChiplet(
    "feature_store_v1_get",
    makeRustNodeChiplet("feature_store_v1_get", "/operators/feature_store_v1/get", {
      key: "",
    }, 69),
  );
  registerManifestRustChiplet(
    "lineage_v2",
    makeRustNodeChiplet("lineage_v2", "/operators/lineage_v2", {
      rules: {},
      computed_fields_v3: [],
    }, 68),
  );
  registerManifestRustChiplet(
    "lineage_v3",
    makeRustNodeChiplet("lineage_v3", "/operators/lineage_v3", {
      rules: {},
      computed_fields_v3: [],
      workflow_steps: [],
      rows: [],
    }, 67),
  );
  registerManifestRustChiplet(
    "rule_simulator_v1",
    makeRustNodeChiplet("rule_simulator_v1", "/operators/rule_simulator_v1", {
      rows: [],
      rules: {},
      candidate_rules: {},
    }, 67),
  );
  registerManifestRustChiplet(
    "constraint_solver_v1",
    makeRustNodeChiplet("constraint_solver_v1", "/operators/constraint_solver_v1", {
      rows: [],
      constraints: [],
    }, 66),
  );
  registerManifestRustChiplet(
    "chart_data_prep_v1",
    makeRustNodeChiplet("chart_data_prep_v1", "/operators/chart_data_prep_v1", {
      rows: [],
      category_field: "category",
      value_field: "value",
      series_field: "series",
      top_n: 100,
    }, 65),
  );
  registerManifestRustChiplet(
    "diff_audit_v1",
    makeRustNodeChiplet("diff_audit_v1", "/operators/diff_audit_v1", {
      left_rows: [],
      right_rows: [],
      keys: ["id"],
    }, 64),
  );
  registerManifestRustChiplet(
    "vector_index_v1_build",
    makeRustNodeChiplet("vector_index_v1_build", "/operators/vector_index_v1/build", {
      rows: [],
      id_field: "id",
      text_field: "text",
    }, 63),
  );
  registerManifestRustChiplet(
    "vector_index_v1_search",
    makeRustNodeChiplet("vector_index_v1_search", "/operators/vector_index_v1/search", {
      query: "",
      top_k: 5,
    }, 62),
  );
  registerManifestRustChiplet(
    "evidence_rank_v1",
    makeRustNodeChiplet("evidence_rank_v1", "/operators/evidence_rank_v1", {
      rows: [],
      time_field: "time",
      source_field: "source_score",
      relevance_field: "relevance",
      consistency_field: "consistency",
    }, 61),
  );
  registerManifestRustChiplet(
    "fact_crosscheck_v1",
    makeRustNodeChiplet("fact_crosscheck_v1", "/operators/fact_crosscheck_v1", {
      rows: [],
      claim_field: "claim",
      source_field: "source",
    }, 60),
  );
  registerManifestRustChiplet(
    "timeseries_forecast_v1",
    makeRustNodeChiplet("timeseries_forecast_v1", "/operators/timeseries_forecast_v1", {
      rows: [],
      time_field: "time",
      value_field: "value",
      horizon: 3,
      method: "naive_drift",
    }, 59),
  );
  registerManifestRustChiplet(
    "finance_ratio_v1",
    makeRustNodeChiplet("finance_ratio_v1", "/operators/finance_ratio_v1", {
      rows: [],
    }, 58),
  );
  registerManifestRustChiplet(
    "anomaly_explain_v1",
    makeRustNodeChiplet("anomaly_explain_v1", "/operators/anomaly_explain_v1", {
      rows: [],
      score_field: "score",
      threshold: 0.8,
    }, 57),
  );
  registerManifestRustChiplet(
    "template_bind_v1",
    makeRustNodeChiplet("template_bind_v1", "/operators/template_bind_v1", {
      template_text: "",
      data: {},
    }, 56),
  );
  registerManifestRustChiplet(
    "provenance_sign_v1",
    makeRustNodeChiplet("provenance_sign_v1", "/operators/provenance_sign_v1", {
      payload: {},
      prev_hash: "",
    }, 55),
  );
  registerManifestRustChiplet(
    "stream_state_v1_save",
    makeRustNodeChiplet("stream_state_v1_save", "/operators/stream_state_v1/save", {
      stream_key: "default",
      state: {},
      offset: 0,
    }, 54),
  );
  registerManifestRustChiplet(
    "stream_state_v1_load",
    makeRustNodeChiplet("stream_state_v1_load", "/operators/stream_state_v1/load", {
      stream_key: "default",
    }, 53),
  );
  registerManifestRustChiplet(
    "stream_state_v2",
    makeRustNodeChiplet("stream_state_v2", "/operators/stream_state_v2", {
      op: "save",
      stream_key: "default",
      state: {},
      offset: 0,
      expected_version: 0,
      backend: "file",
      db_path: "",
      event_ts_ms: null,
      max_late_ms: null,
    }, 52),
  );
  registerManifestRustChiplet(
    "parquet_io_v2",
    makeRustNodeChiplet("parquet_io_v2", "/operators/parquet_io_v2", {
      op: "inspect",
      path: "",
      rows: [],
      parquet_mode: "typed",
      limit: 1000,
      partition_by: [],
      compression: "snappy",
      recursive: true,
    }, 51),
  );
  registerManifestRustChiplet(
    "query_lang_v1",
    makeRustNodeChiplet("query_lang_v1", "/operators/query_lang_v1", {
      rows: [],
      query: "limit 100",
    }, 50),
  );
  registerManifestRustChiplet(
    "columnar_eval_v1",
    makeRustNodeChiplet("columnar_eval_v1", "/operators/columnar_eval_v1", {
      rows: [],
      select_fields: [],
      filter_eq: {},
      limit: 10000,
    }, 50),
  );
  registerManifestRustChiplet(
    "stream_window_v1",
    makeRustNodeChiplet("stream_window_v1", "/operators/stream_window_v1", {
      stream_key: "default_stream",
      rows: [],
      event_time_field: "ts",
      window_ms: 60000,
      watermark_ms: 60000,
      group_by: [],
      value_field: "value",
      trigger: "on_watermark",
    }, 49),
  );
  registerManifestRustChiplet(
    "stream_window_v2",
    makeRustNodeChiplet("stream_window_v2", "/operators/stream_window_v2", {
      stream_key: "default_stream",
      rows: [],
      event_time_field: "ts",
      window_type: "tumbling",
      window_ms: 60000,
      slide_ms: 10000,
      session_gap_ms: 30000,
      watermark_ms: 60000,
      allowed_lateness_ms: 60000,
      group_by: [],
      value_field: "value",
      trigger: "on_watermark",
      emit_late_side: true,
    }, 49),
  );
  registerManifestRustChiplet(
    "sketch_v1",
    makeRustNodeChiplet("sketch_v1", "/operators/sketch_v1", {
      op: "create",
      kind: "hll",
      state: {},
      rows: [],
      field: "value",
      topk_n: 5,
      merge_state: {},
    }, 49),
  );
  registerManifestRustChiplet(
    "runtime_stats_v1",
    makeRustNodeChiplet("runtime_stats_v1", "/operators/runtime_stats_v1", {
      op: "summary",
      operator: "",
      ok: true,
      error_code: "",
      duration_ms: 0,
      rows_in: 0,
      rows_out: 0,
    }, 48),
  );
  registerManifestRustChiplet(
    "capabilities_v1",
    makeRustNodeChiplet("capabilities_v1", "/operators/capabilities_v1", {
      include_ops: [],
    }, 48),
  );
  registerManifestRustChiplet(
    "io_contract_v1",
    makeRustNodeChiplet("io_contract_v1", "/operators/io_contract_v1/validate", {
      operator: "transform_rows_v3",
      input: {},
      strict: false,
    }, 48),
  );
  registerManifestRustChiplet(
    "failure_policy_v1",
    makeRustNodeChiplet("failure_policy_v1", "/operators/failure_policy_v1", {
      operator: "",
      error: "",
      status_code: 0,
      attempts: 0,
      max_retries: 2,
    }, 48),
  );
  registerManifestRustChiplet(
    "incremental_plan_v1",
    makeRustNodeChiplet("incremental_plan_v1", "/operators/incremental_plan_v1", {
      operator: "transform_rows_v2",
      input: {},
      checkpoint_key: "",
    }, 48),
  );
  registerManifestRustChiplet(
    "tenant_isolation_v1",
    makeRustNodeChiplet("tenant_isolation_v1", "/operators/tenant_isolation_v1", {
      op: "get",
      tenant_id: "default",
    }, 48),
  );
  registerManifestRustChiplet(
    "operator_policy_v1",
    makeRustNodeChiplet("operator_policy_v1", "/operators/operator_policy_v1", {
      op: "get",
      tenant_id: "default",
      allow: [],
      deny: [],
    }, 48),
  );
  registerManifestRustChiplet(
    "optimizer_adaptive_v2",
    makeRustNodeChiplet("optimizer_adaptive_v2", "/operators/optimizer_adaptive_v2", {
      operator: "transform_rows_v3",
      row_count_hint: 0,
      prefer_arrow: false,
    }, 48),
  );
  registerManifestRustChiplet(
    "vector_index_v2_build",
    makeRustNodeChiplet("vector_index_v2_build", "/operators/vector_index_v2/build", {
      shard: "default",
      rows: [],
      id_field: "id",
      text_field: "text",
      metadata_fields: [],
      replace: false,
    }, 48),
  );
  registerManifestRustChiplet(
    "vector_index_v2_search",
    makeRustNodeChiplet("vector_index_v2_search", "/operators/vector_index_v2/search", {
      query: "",
      top_k: 5,
      shard: "",
      filter_eq: {},
      rerank_meta_field: "",
      rerank_meta_weight: 0,
    }, 48),
  );
  registerManifestRustChiplet(
    "vector_index_v2_eval",
    makeRustNodeChiplet("vector_index_v2_eval", "/operators/vector_index_v2/eval", {
      run_id: "",
      shard: "",
      top_k: 5,
      cases: [],
    }, 48),
  );
  registerManifestRustChiplet(
    "stream_reliability_v1",
    makeRustNodeChiplet("stream_reliability_v1", "/operators/stream_reliability_v1", {
      op: "stats",
      stream_key: "default",
      msg_id: "",
      row: {},
      error: "",
      checkpoint: 0,
    }, 48),
  );
  registerManifestRustChiplet(
    "lineage_provenance_v1",
    makeRustNodeChiplet("lineage_provenance_v1", "/operators/lineage_provenance_v1", {
      rules: {},
      computed_fields_v3: [],
      workflow_steps: [],
      rows: [],
      payload: {},
      prev_hash: "",
    }, 48),
  );
  registerManifestRustChiplet(
    "contract_regression_v1",
    makeRustNodeChiplet("contract_regression_v1", "/operators/contract_regression_v1", {
      operators: [],
    }, 48),
  );
  registerManifestRustChiplet(
    "perf_baseline_v1",
    "/operators/perf_baseline_v1",
    {
      op: "get",
      operator: "transform_rows_v3",
      p95_ms: 500,
      max_p95_ms: 500,
    },
    48,
  );
  registerManifestRustChiplet(
    "plugin_operator_v1",
    "/operators/plugin_operator_v1",
    {
      plugin: "",
      op: "run",
      payload: {},
    },
    48,
  );
  registerManifestRustChiplet(
    "explain_plan_v1",
    "/operators/explain_plan_v1",
    {
      steps: [],
      rows: [],
    },
    49,
  );
  registerManifestRustChiplet(
    "explain_plan_v2",
    "/operators/explain_plan_v2",
    {
      steps: [],
      rows: [],
      actual_stats: [],
      persist_feedback: true,
      include_runtime_stats: false,
    },
    49,
  );
  assertRegistrationSurface();
}

module.exports = {
  registerRustOpsDomainChiplets,
};

