function registerRustOpsDomainChiplets(registry, deps, helpers) {
  const { runIsolatedTask } = deps;
  const {
    rustBase,
    rustRequired,
    resolveIsolationLevel,
    resolveSandboxLimits,
  } = helpers;

  async function callRustOperator(base, operatorPath, body, required, timeoutMs, operatorName) {
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

  registry.register(
    "transform_rows_v3",
    makeRustNodeChiplet("transform_rows_v3", "/operators/transform_rows_v3", {
      rows: [],
      rules: {},
      computed_fields_v3: [],
    }, 89),
  );
  registry.register(
    "join_rows_v2",
    makeRustNodeChiplet("join_rows_v2", "/operators/join_rows_v2", {
      left_rows: [],
      right_rows: [],
      left_on: ["id"],
      right_on: ["id"],
      join_type: "inner",
    }, 88),
  );
  registry.register(
    "join_rows_v3",
    makeRustNodeChiplet("join_rows_v3", "/operators/join_rows_v3", {
      left_rows: [],
      right_rows: [],
      left_on: ["id"],
      right_on: ["id"],
      join_type: "inner",
      strategy: "auto",
      chunk_size: 50000,
    }, 87),
  );
  registry.register(
    "join_rows_v4",
    makeRustNodeChiplet("join_rows_v4", "/operators/join_rows_v4", {
      left_rows: [],
      right_rows: [],
      left_on: ["id"],
      right_on: ["id"],
      join_type: "inner",
      strategy: "auto",
      chunk_size: 50000,
      enable_bloom: true,
    }, 86),
  );
  registry.register(
    "aggregate_rows_v2",
    makeRustNodeChiplet("aggregate_rows_v2", "/operators/aggregate_rows_v2", {
      rows: [],
      group_by: [],
      aggregates: [{ op: "count", as: "row_count" }],
    }, 85),
  );
  registry.register(
    "aggregate_rows_v3",
    makeRustNodeChiplet("aggregate_rows_v3", "/operators/aggregate_rows_v3", {
      rows: [],
      group_by: [],
      aggregates: [{ op: "count", as: "row_count" }],
      approx_sample_size: 1024,
    }, 84),
  );
  registry.register(
    "aggregate_rows_v4",
    makeRustNodeChiplet("aggregate_rows_v4", "/operators/aggregate_rows_v4", {
      rows: [],
      group_by: [],
      aggregates: [{ op: "count", as: "row_count" }],
      approx_sample_size: 1024,
      verify_exact: false,
      parallel_workers: 1,
    }, 83),
  );
  registry.register(
    "quality_check_v2",
    makeRustNodeChiplet("quality_check_v2", "/operators/quality_check_v2", {
      rows: [],
      rules: {},
    }, 82),
  );
  registry.register(
    "quality_check_v3",
    makeRustNodeChiplet("quality_check_v3", "/operators/quality_check_v3", {
      rows: [],
      rules: {},
    }, 81),
  );
  registry.register(
    "quality_check_v4",
    makeRustNodeChiplet("quality_check_v4", "/operators/quality_check_v4", {
      rows: [],
      rules: {},
      rules_dsl: "",
    }, 80),
  );
  registry.register(
    "plugin_registry_v1",
    makeRustNodeChiplet("plugin_registry_v1", "/operators/plugin_registry_v1", {
      op: "list",
      plugin: "",
      manifest: {},
    }, 79),
  );
  registry.register(
    "window_rows_v1",
    makeRustNodeChiplet("window_rows_v1", "/operators/window_rows_v1", {
      rows: [],
      partition_by: [],
      order_by: "time",
      functions: [{ op: "row_number", as: "row_no" }],
    }, 79),
  );
  registry.register(
    "optimizer_v1",
    makeRustNodeChiplet("optimizer_v1", "/operators/optimizer_v1", {
      rows: [],
      row_count_hint: 10000,
      prefer_arrow: true,
    }, 78),
  );
  registry.register(
    "load_rows_v2",
    makeRustNodeChiplet("load_rows_v2", "/operators/load_rows_v2", {
      source_type: "csv",
      source: "",
      limit: 10000,
    }, 77),
  );
  registry.register(
    "load_rows_v3",
    makeRustNodeChiplet("load_rows_v3", "/operators/load_rows_v3", {
      source_type: "csv",
      source: "",
      limit: 10000,
      max_retries: 2,
      retry_backoff_ms: 150,
    }, 76),
  );
  registry.register(
    "schema_registry_v1_register",
    makeRustNodeChiplet("schema_registry_v1_register", "/operators/schema_registry_v1/register", {
      name: "default_schema",
      version: "v1",
      schema: {},
    }, 80),
  );
  registry.register(
    "schema_registry_v1_get",
    makeRustNodeChiplet("schema_registry_v1_get", "/operators/schema_registry_v1/get", {
      name: "default_schema",
      version: "v1",
    }, 79),
  );
  registry.register(
    "schema_registry_v1_infer",
    makeRustNodeChiplet("schema_registry_v1_infer", "/operators/schema_registry_v1/infer", {
      name: "default_schema",
      version: "v1",
      rows: [],
    }, 78),
  );
  registry.register(
    "schema_registry_v2_check_compat",
    makeRustNodeChiplet("schema_registry_v2_check_compat", "/operators/schema_registry_v2/check_compat", {
      name: "default_schema",
      from_version: "v1",
      to_version: "v2",
      mode: "backward",
    }, 77),
  );
  registry.register(
    "schema_registry_v2_suggest_migration",
    makeRustNodeChiplet("schema_registry_v2_suggest_migration", "/operators/schema_registry_v2/suggest_migration", {
      name: "default_schema",
      from_version: "v1",
      to_version: "v2",
    }, 76),
  );
  registry.register(
    "udf_wasm_v1",
    makeRustNodeChiplet("udf_wasm_v1", "/operators/udf_wasm_v1/apply", {
      rows: [],
      field: "",
      output_field: "",
      op: "identity",
    }, 75),
  );
  registry.register(
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
  registry.register(
    "time_series_v1",
    makeRustNodeChiplet("time_series_v1", "/operators/time_series_v1", {
      rows: [],
      time_field: "month",
      value_field: "value",
      group_by: [],
      window: 3,
    }, 73),
  );
  registry.register(
    "stats_v1",
    makeRustNodeChiplet("stats_v1", "/operators/stats_v1", {
      rows: [],
      x_field: "x",
      y_field: "y",
    }, 72),
  );
  registry.register(
    "entity_linking_v1",
    makeRustNodeChiplet("entity_linking_v1", "/operators/entity_linking_v1", {
      rows: [],
      field: "entity",
      id_field: "entity_id",
    }, 72),
  );
  registry.register(
    "table_reconstruct_v1",
    makeRustNodeChiplet("table_reconstruct_v1", "/operators/table_reconstruct_v1", {
      lines: [],
      delimiter: "\\s{2,}|\\t",
    }, 71),
  );
  registry.register(
    "feature_store_v1_upsert",
    makeRustNodeChiplet("feature_store_v1_upsert", "/operators/feature_store_v1/upsert", {
      key_field: "id",
      rows: [],
    }, 70),
  );
  registry.register(
    "feature_store_v1_get",
    makeRustNodeChiplet("feature_store_v1_get", "/operators/feature_store_v1/get", {
      key: "",
    }, 69),
  );
  registry.register(
    "lineage_v2",
    makeRustNodeChiplet("lineage_v2", "/operators/lineage_v2", {
      rules: {},
      computed_fields_v3: [],
    }, 68),
  );
  registry.register(
    "lineage_v3",
    makeRustNodeChiplet("lineage_v3", "/operators/lineage_v3", {
      rules: {},
      computed_fields_v3: [],
      workflow_steps: [],
      rows: [],
    }, 67),
  );
  registry.register(
    "rule_simulator_v1",
    makeRustNodeChiplet("rule_simulator_v1", "/operators/rule_simulator_v1", {
      rows: [],
      rules: {},
      candidate_rules: {},
    }, 67),
  );
  registry.register(
    "constraint_solver_v1",
    makeRustNodeChiplet("constraint_solver_v1", "/operators/constraint_solver_v1", {
      rows: [],
      constraints: [],
    }, 66),
  );
  registry.register(
    "chart_data_prep_v1",
    makeRustNodeChiplet("chart_data_prep_v1", "/operators/chart_data_prep_v1", {
      rows: [],
      category_field: "category",
      value_field: "value",
      series_field: "series",
      top_n: 100,
    }, 65),
  );
  registry.register(
    "diff_audit_v1",
    makeRustNodeChiplet("diff_audit_v1", "/operators/diff_audit_v1", {
      left_rows: [],
      right_rows: [],
      keys: ["id"],
    }, 64),
  );
  registry.register(
    "vector_index_v1_build",
    makeRustNodeChiplet("vector_index_v1_build", "/operators/vector_index_v1/build", {
      rows: [],
      id_field: "id",
      text_field: "text",
    }, 63),
  );
  registry.register(
    "vector_index_v1_search",
    makeRustNodeChiplet("vector_index_v1_search", "/operators/vector_index_v1/search", {
      query: "",
      top_k: 5,
    }, 62),
  );
  registry.register(
    "evidence_rank_v1",
    makeRustNodeChiplet("evidence_rank_v1", "/operators/evidence_rank_v1", {
      rows: [],
      time_field: "time",
      source_field: "source_score",
      relevance_field: "relevance",
      consistency_field: "consistency",
    }, 61),
  );
  registry.register(
    "fact_crosscheck_v1",
    makeRustNodeChiplet("fact_crosscheck_v1", "/operators/fact_crosscheck_v1", {
      rows: [],
      claim_field: "claim",
      source_field: "source",
    }, 60),
  );
  registry.register(
    "timeseries_forecast_v1",
    makeRustNodeChiplet("timeseries_forecast_v1", "/operators/timeseries_forecast_v1", {
      rows: [],
      time_field: "time",
      value_field: "value",
      horizon: 3,
      method: "naive_drift",
    }, 59),
  );
  registry.register(
    "finance_ratio_v1",
    makeRustNodeChiplet("finance_ratio_v1", "/operators/finance_ratio_v1", {
      rows: [],
    }, 58),
  );
  registry.register(
    "anomaly_explain_v1",
    makeRustNodeChiplet("anomaly_explain_v1", "/operators/anomaly_explain_v1", {
      rows: [],
      score_field: "score",
      threshold: 0.8,
    }, 57),
  );
  registry.register("evidence_conflict_v1", {
    id: "chiplet.evidence_conflict_v1",
    priority: 56,
    timeout_ms: 60000,
    retries: 0,
    async run(_ctx, node) {
      const cfg = node?.config && typeof node.config === "object" ? node.config : {};
      const rows = Array.isArray(cfg.rows) ? cfg.rows : [];
      const claimField = String(cfg.claim_field || "claim");
      const stanceField = String(cfg.stance_field || "stance");
      const sourceField = String(cfg.source_field || "source");
      const byClaim = new Map();
      rows.forEach((r) => {
        const claim = String(r?.[claimField] || "").trim().toLowerCase();
        if (!claim) return;
        const stance = String(r?.[stanceField] || "").trim().toLowerCase();
        const source = String(r?.[sourceField] || "").trim();
        if (!byClaim.has(claim)) byClaim.set(claim, { support: 0, oppose: 0, neutral: 0, sources: new Set() });
        const it = byClaim.get(claim);
        if (/(support|璧炴垚|鏀寔|yes|true|pro)/i.test(stance)) it.support += 1;
        else if (/(oppose|鍙嶅|璐ㄧ枒|no|false|con)/i.test(stance)) it.oppose += 1;
        else it.neutral += 1;
        if (source) it.sources.add(source);
      });
      const conflicts = [];
      for (const [claim, v] of byClaim.entries()) {
        if (v.support > 0 && v.oppose > 0) {
          conflicts.push({
            claim,
            support: v.support,
            oppose: v.oppose,
            neutral: v.neutral,
            source_count: v.sources.size,
            conflict_score: Number((Math.min(v.support, v.oppose) / Math.max(1, v.support + v.oppose)).toFixed(4)),
          });
        }
      }
      conflicts.sort((a, b) => b.conflict_score - a.conflict_score);
      return {
        ok: true,
        status: "done",
        operator: "evidence_conflict_v1",
        conflict_count: conflicts.length,
        total_claims: byClaim.size,
        conflicts,
      };
    },
  });
  registry.register(
    "template_bind_v1",
    makeRustNodeChiplet("template_bind_v1", "/operators/template_bind_v1", {
      template_text: "",
      data: {},
    }, 56),
  );
  registry.register(
    "provenance_sign_v1",
    makeRustNodeChiplet("provenance_sign_v1", "/operators/provenance_sign_v1", {
      payload: {},
      prev_hash: "",
    }, 55),
  );
  registry.register(
    "stream_state_v1_save",
    makeRustNodeChiplet("stream_state_v1_save", "/operators/stream_state_v1/save", {
      stream_key: "default",
      state: {},
      offset: 0,
    }, 54),
  );
  registry.register(
    "stream_state_v1_load",
    makeRustNodeChiplet("stream_state_v1_load", "/operators/stream_state_v1/load", {
      stream_key: "default",
    }, 53),
  );
  registry.register(
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
  registry.register(
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
  registry.register(
    "query_lang_v1",
    makeRustNodeChiplet("query_lang_v1", "/operators/query_lang_v1", {
      rows: [],
      query: "limit 100",
    }, 50),
  );
  registry.register(
    "columnar_eval_v1",
    makeRustNodeChiplet("columnar_eval_v1", "/operators/columnar_eval_v1", {
      rows: [],
      select_fields: [],
      filter_eq: {},
      limit: 10000,
    }, 50),
  );
  registry.register(
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
  registry.register(
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
  registry.register(
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
  registry.register(
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
  registry.register(
    "capabilities_v1",
    makeRustNodeChiplet("capabilities_v1", "/operators/capabilities_v1", {
      include_ops: [],
    }, 48),
  );
  registry.register(
    "io_contract_v1",
    makeRustNodeChiplet("io_contract_v1", "/operators/io_contract_v1/validate", {
      operator: "transform_rows_v3",
      input: {},
      strict: false,
    }, 48),
  );
  registry.register(
    "failure_policy_v1",
    makeRustNodeChiplet("failure_policy_v1", "/operators/failure_policy_v1", {
      operator: "",
      error: "",
      status_code: 0,
      attempts: 0,
      max_retries: 2,
    }, 48),
  );
  registry.register(
    "incremental_plan_v1",
    makeRustNodeChiplet("incremental_plan_v1", "/operators/incremental_plan_v1", {
      operator: "transform_rows_v2",
      input: {},
      checkpoint_key: "",
    }, 48),
  );
  registry.register(
    "tenant_isolation_v1",
    makeRustNodeChiplet("tenant_isolation_v1", "/operators/tenant_isolation_v1", {
      op: "get",
      tenant_id: "default",
    }, 48),
  );
  registry.register(
    "operator_policy_v1",
    makeRustNodeChiplet("operator_policy_v1", "/operators/operator_policy_v1", {
      op: "get",
      tenant_id: "default",
      allow: [],
      deny: [],
    }, 48),
  );
  registry.register(
    "optimizer_adaptive_v2",
    makeRustNodeChiplet("optimizer_adaptive_v2", "/operators/optimizer_adaptive_v2", {
      operator: "transform_rows_v3",
      row_count_hint: 0,
      prefer_arrow: false,
    }, 48),
  );
  registry.register(
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
  registry.register(
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
  registry.register(
    "vector_index_v2_eval",
    makeRustNodeChiplet("vector_index_v2_eval", "/operators/vector_index_v2/eval", {
      run_id: "",
      shard: "",
      top_k: 5,
      cases: [],
    }, 48),
  );
  registry.register(
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
  registry.register(
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
  registry.register(
    "contract_regression_v1",
    makeRustNodeChiplet("contract_regression_v1", "/operators/contract_regression_v1", {
      operators: [],
    }, 48),
  );
  registry.register(
    "perf_baseline_v1",
    makeRustNodeChiplet("perf_baseline_v1", "/operators/perf_baseline_v1", {
      op: "get",
      operator: "transform_rows_v3",
      p95_ms: 500,
      max_p95_ms: 500,
    }, 48),
  );
  registry.register(
    "plugin_operator_v1",
    makeRustNodeChiplet("plugin_operator_v1", "/operators/plugin_operator_v1", {
      plugin: "",
      op: "run",
      payload: {},
    }, 48),
  );
  registry.register(
    "explain_plan_v1",
    makeRustNodeChiplet("explain_plan_v1", "/operators/explain_plan_v1", {
      steps: [],
      rows: [],
    }, 49),
  );
  registry.register(
    "explain_plan_v2",
    makeRustNodeChiplet("explain_plan_v2", "/operators/explain_plan_v2", {
      steps: [],
      rows: [],
      actual_stats: [],
      persist_feedback: true,
      include_runtime_stats: false,
    }, 49),
  );
}

module.exports = {
  registerRustOpsDomainChiplets,
};
