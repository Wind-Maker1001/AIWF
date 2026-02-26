function registerBuiltinWorkflowChiplets(registry, deps) {
  const {
    fs,
    path,
    runOfflineCleaning,
    collectFiles,
    readArtifactById,
    summarizeCorpus,
    computeViaRust,
    callExternalAi,
    auditAiText,
    writeWorkflowSummary,
    sha256Text,
    nodeOutputByType,
    runIsolatedTask,
  } = deps;

  function rustBase(ctx) {
    return String(ctx?.payload?.rust?.endpoint || "http://127.0.0.1:18082").replace(/\/$/, "");
  }

  function rustRequired(ctx) {
    return ctx?.payload?.rust?.required !== false;
  }

  function parseIsolationTypeList(ctx) {
    const fromPayload = Array.isArray(ctx?.payload?.chiplet_isolated_types)
      ? ctx.payload.chiplet_isolated_types
      : [];
    const fromCfg = Array.isArray(ctx?.config?.chiplet_isolated_types)
      ? ctx.config.chiplet_isolated_types
      : [];
    const fromEnv = String(process.env.AIWF_CHIPLET_ISOLATED_TYPES || "")
      .split(/[;,]/)
      .map((x) => String(x || "").trim())
      .filter(Boolean);
    return Array.from(new Set([...fromPayload, ...fromCfg, ...fromEnv].map((x) => String(x || "").trim()).filter(Boolean)));
  }

  function resolveSandboxLimits(ctx, node = null) {
    const env = {
      max_duration_ms: Number(process.env.AIWF_SANDBOX_MAX_DURATION_MS || 180000),
      max_cpu_ms: Number(process.env.AIWF_SANDBOX_MAX_CPU_MS || 120000),
      max_rss_mb: Number(process.env.AIWF_SANDBOX_MAX_RSS_MB || 512),
      max_output_bytes: Number(process.env.AIWF_SANDBOX_MAX_OUTPUT_BYTES || 2000000),
    };
    const cfg = ctx?.config?.sandbox_limits && typeof ctx.config.sandbox_limits === "object" ? ctx.config.sandbox_limits : {};
    const payload = ctx?.payload?.sandbox_limits && typeof ctx.payload.sandbox_limits === "object" ? ctx.payload.sandbox_limits : {};
    const nodeCfg = node?.config?.sandbox_limits && typeof node.config.sandbox_limits === "object" ? node.config.sandbox_limits : {};
    function pick(name, def) {
      const n = Number(nodeCfg[name] ?? payload[name] ?? cfg[name] ?? def);
      return Number.isFinite(n) && n > 0 ? Math.floor(n) : def;
    }
    return {
      max_duration_ms: pick("max_duration_ms", env.max_duration_ms),
      max_cpu_ms: pick("max_cpu_ms", env.max_cpu_ms),
      max_rss_mb: pick("max_rss_mb", env.max_rss_mb),
      max_output_bytes: pick("max_output_bytes", env.max_output_bytes),
    };
  }

  function resolveIsolationLevel(ctx, nodeType, defaultOn = false, node = null) {
    if (typeof runIsolatedTask !== "function") return "none";
    const nodeCfg = node?.config && typeof node.config === "object" ? node.config : {};
    const nodeLevel = String(nodeCfg.isolation_level || "").trim().toLowerCase();
    if (nodeLevel === "none" || nodeLevel === "off" || nodeLevel === "disabled") return "none";
    if (nodeLevel === "process" || nodeLevel === "sandbox") return nodeLevel;
    if (ctx?.payload?.chiplet_isolation_enabled === false) return "none";
    if (ctx?.config?.chiplet_isolation_enabled === false) return "none";
    const mode = String(
      ctx?.payload?.chiplet_isolation_mode
      || ctx?.config?.chiplet_isolation_mode
      || process.env.AIWF_CHIPLET_ISOLATION_MODE
      || "high_risk"
    ).trim().toLowerCase();
    if (mode === "off" || mode === "none" || mode === "disabled") return "none";
    if (mode === "all") return "process";
    const t = String(nodeType || "").trim();
    const list = parseIsolationTypeList(ctx);
    if (list.includes("*") || list.includes(t)) return "process";
    return defaultOn ? "process" : "none";
  }

  function extractNumericTokens(text) {
    const src = String(text || "");
    const out = [];
    const rx = /[-+]?\d+(?:[.,]\d+)?%?/g;
    let m = null;
    while ((m = rx.exec(src))) {
      const raw = String(m[0] || "");
      const norm = raw.replace(/,/g, "");
      if (!norm) continue;
      out.push(norm);
    }
    return out;
  }

  function hasCitationMarkers(text) {
    const s = String(text || "");
    if (!s.trim()) return false;
    if (/\[[0-9]{1,3}\]/.test(s)) return true;
    if (/(来源|出处|source|reference)[:：]/i.test(s)) return true;
    if (/https?:\/\/\S+/i.test(s)) return true;
    if (/（[^）]{0,28}(来源|出处)[^）]{0,28}）/.test(s)) return true;
    return false;
  }

  function compareMetricCore(base = {}, now = {}) {
    const keys = ["sections", "bullets", "chars", "cjk", "latin"];
    const diffs = [];
    for (const k of keys) {
      const a = Number(base?.[k] || 0);
      const b = Number(now?.[k] || 0);
      const d = Math.abs(a - b);
      if (d > 0) diffs.push({ key: k, base: a, now: b, delta: d });
    }
    return diffs;
  }

  function looksLikeDataFile(p) {
    const s = String(p || "").trim().toLowerCase();
    if (!s) return false;
    return /\.(csv|tsv|xlsx|xls|json|jsonl|parquet|feather|orc|db|sqlite|sql)$/i.test(s);
  }

  function hasRowsLikeOutput(nodeOutputs) {
    if (!nodeOutputs || typeof nodeOutputs !== "object") return false;
    const values = Array.isArray(nodeOutputs) ? nodeOutputs : Object.values(nodeOutputs);
    for (const v of values) {
      if (!v || typeof v !== "object") continue;
      if (Array.isArray(v.rows) && v.rows.length > 0) return true;
      if (v.detail && typeof v.detail === "object" && Array.isArray(v.detail.rows) && v.detail.rows.length > 0) return true;
      if (Array.isArray(v.left_rows) && v.left_rows.length > 0) return true;
      if (Array.isArray(v.right_rows) && v.right_rows.length > 0) return true;
    }
    return false;
  }

  function shouldBlockAiOnData(ctx, node) {
    const payloadAi = ctx?.payload?.ai && typeof ctx.payload.ai === "object" ? ctx.payload.ai : {};
    const cfg = node?.config && typeof node.config === "object" ? node.config : {};
    const allowOnData = cfg.allow_ai_on_data === true || payloadAi.allow_on_data === true;
    if (allowOnData) return { block: false, reason: "" };
    const strict = payloadAi.no_hallucination_data !== false;
    if (!strict) return { block: false, reason: "" };
    const filesFromCtx = Array.isArray(ctx?.files) ? ctx.files : [];
    const filesFromPayload = collectFiles(ctx?.payload || {});
    const fileList = Array.from(new Set([...filesFromCtx, ...filesFromPayload]));
    const dataFileHit = fileList.find((f) => looksLikeDataFile(f));
    const dataRowsHit = hasRowsLikeOutput(ctx?.nodeOutputs);
    if (dataFileHit || dataRowsHit) {
      return {
        block: true,
        reason: dataFileHit ? `data_file_detected:${String(dataFileHit)}` : "rows_detected_in_upstream_outputs",
      };
    }
    return { block: false, reason: "" };
  }

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

  registry.register("ingest_files", {
    id: "chiplet.ingest_files.v1",
    priority: 120,
    timeout_ms: 60000,
    retries: 0,
    async run(ctx, node) {
      const files = collectFiles(ctx.payload);
      ctx.files = files;
      return { input_files: files, count: files.length };
    },
  });

  registry.register("clean_md", {
    id: "chiplet.clean_md.v1",
    priority: 110,
    timeout_ms: Number(process.env.AIWF_CHIPLET_CLEAN_TIMEOUT_MS || 900000),
    retries: 0,
    async run(ctx, node) {
      const params = ctx.payload.params || {};
      const cfg = node?.config && typeof node.config === "object" ? node.config : {};
      ctx.cleanResult = await runOfflineCleaning({
        params: {
          ...params,
          ...cfg,
          report_title: params.report_title || "Workflow 清洗结果",
          input_files: JSON.stringify(ctx.files || []),
          md_only: true,
          paper_markdown_enabled: true,
        },
        output_root: ctx.outputRoot,
      });
      const aiCorpusPath = readArtifactById(ctx.cleanResult.artifacts, "md_ai_corpus_001");
      ctx.aiCorpusPath = aiCorpusPath;
      ctx.corpusText = fs.existsSync(aiCorpusPath) ? fs.readFileSync(aiCorpusPath, "utf8") : "";
      return {
        job_id: ctx.cleanResult.job_id,
        ai_corpus_path: aiCorpusPath,
        warnings: ctx.cleanResult.warnings || [],
        rust_v2_used: !!ctx.cleanResult?.quality?.rust_v2_used,
      };
    },
  });

  const computeChiplet = {
    id: "chiplet.compute_rust.v1",
    priority: 90,
    timeout_ms: Number(process.env.AIWF_CHIPLET_COMPUTE_TIMEOUT_MS || 180000),
    retries: Number(process.env.AIWF_CHIPLET_COMPUTE_RETRIES || 1),
    async run(ctx, node) {
      let computed = null;
      const options = {
        run_id: ctx.runId,
        rust_endpoint: ctx.payload.rust?.endpoint,
        rust_required: ctx.payload.rust?.required !== false,
      };
      const isolationLevel = resolveIsolationLevel(ctx, "compute_rust", true, node);
      if (isolationLevel !== "none") {
        try {
          computed = await runIsolatedTask("compute_rust", {
            corpusText: ctx.corpusText || "",
            options,
            isolation_level: isolationLevel,
            sandbox_limits: resolveSandboxLimits(ctx, node),
          }, Number(process.env.AIWF_CHIPLET_COMPUTE_TIMEOUT_MS || 120000));
          computed.isolated = true;
          computed.isolation_level = isolationLevel;
        } catch (e) {
          if (isolationLevel === "sandbox") throw e;
          computed = await computeViaRust(ctx.corpusText || "", options);
          computed.isolated = false;
          computed.isolation_level = "none";
          computed.isolation_error = String(e);
        }
      } else {
        computed = await computeViaRust(ctx.corpusText || "", options);
        computed.isolated = false;
        computed.isolation_level = "none";
      }
      ctx.metrics = computed.metrics;
      return {
        engine: computed.mode,
        metrics: computed.metrics,
        rust_started: computed.started || false,
        rust_path: computed.rust_path || "",
        isolated: !!computed.isolated,
        isolation_level: computed.isolation_level || "none",
        isolation_error: computed.isolation_error || "",
      };
    },
  };
  registry.register("compute_rust", computeChiplet);
  registry.register("compute_rust_placeholder", computeChiplet);
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
        if (/(support|赞成|支持|yes|true|pro)/i.test(stance)) it.support += 1;
        else if (/(oppose|反对|质疑|no|false|con)/i.test(stance)) it.oppose += 1;
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

  registry.register("manual_review", {
    id: "chiplet.manual_review.v1",
    priority: 75,
    timeout_ms: Number(process.env.AIWF_CHIPLET_MANUAL_REVIEW_TIMEOUT_MS || 60000),
    retries: 0,
    async run(ctx, node) {
      const cfg = node?.config && typeof node.config === "object" ? node.config : {};
      const bag = ctx?.payload?.manual_review && typeof ctx.payload.manual_review === "object"
        ? ctx.payload.manual_review
        : {};
      const key = String(cfg.review_key || node?.id || "manual_review");
      const picked = bag[key] && typeof bag[key] === "object" ? bag[key] : {};
      const reviewer = String(
        picked.reviewer
          || cfg.default_reviewer
          || ctx?.payload?.reviewer
          || ctx?.payload?.actor
          || "unassigned",
      ).trim();
      const comment = String(picked.comment || cfg.default_comment || "").trim();
      if (typeof picked.approved !== "boolean") {
        const req = {
          run_id: String(ctx?.runId || ""),
          workflow_id: String(ctx?.workflowId || ""),
          node_id: String(node?.id || ""),
          review_key: key,
          reviewer,
          comment,
          created_at: new Date().toISOString(),
          status: "pending",
        };
        if (!Array.isArray(ctx.manualReviewRequests)) ctx.manualReviewRequests = [];
        ctx.manualReviewRequests.push(req);
        throw new Error(`manual_review_pending:${key}`);
      }
      const approved = picked.approved;
      return {
        ok: approved,
        status: approved ? "approved" : "rejected",
        approved,
        review_key: key,
        reviewer,
        comment,
      };
    },
  });

  registry.register("sql_chart_v1", {
    id: "chiplet.sql_chart_v1",
    priority: 71,
    timeout_ms: 60000,
    retries: 0,
    async run(ctx, node) {
      const cfg = node?.config && typeof node.config === "object" ? node.config : {};
      const inRows = Array.isArray(cfg.rows) ? cfg.rows : [];
      const rows = inRows.length ? inRows : (nodeOutputByType(ctx, "load_rows_v3")?.detail?.rows || []);
      const categoryField = String(cfg.category_field || "category");
      const valueField = String(cfg.value_field || "value");
      const seriesField = String(cfg.series_field || "series");
      const topN = Number.isFinite(Number(cfg.top_n)) ? Math.max(1, Math.floor(Number(cfg.top_n))) : 100;
      const grouped = new Map();
      for (const r of rows) {
        const c = String((r && r[categoryField]) ?? "");
        const s = String((r && r[seriesField]) ?? "default");
        const v = Number((r && r[valueField]) ?? 0);
        if (!grouped.has(c)) grouped.set(c, {});
        const cur = grouped.get(c);
        cur[s] = Number(cur[s] || 0) + (Number.isFinite(v) ? v : 0);
      }
      const cats = Array.from(grouped.keys()).slice(0, topN);
      const seriesKeys = new Set();
      cats.forEach((c) => Object.keys(grouped.get(c) || {}).forEach((k) => seriesKeys.add(k)));
      const series = Array.from(seriesKeys).map((k) => ({
        name: k,
        data: cats.map((c) => Number((grouped.get(c) || {})[k] || 0)),
      }));
      return {
        ok: true,
        chart_type: String(cfg.chart_type || "bar"),
        categories: cats,
        series,
        rows_in: rows.length,
      };
    },
  });

  registry.register("office_slot_fill_v1", {
    id: "chiplet.office_slot_fill_v1",
    priority: 70,
    timeout_ms: 60000,
    retries: 0,
    async run(ctx, node) {
      const cfg = node?.config && typeof node.config === "object" ? node.config : {};
      const sourceType = String(cfg.chart_source_node || "sql_chart_v1").trim() || "sql_chart_v1";
      const chart = nodeOutputByType(ctx, sourceType) || {};
      const slots = cfg.slots && typeof cfg.slots === "object" && !Array.isArray(cfg.slots) ? { ...cfg.slots } : {};
      const templateVersion = String(cfg.template_version || "v1");
      const requiredSlots = Array.isArray(cfg.required_slots) ? cfg.required_slots.map((x) => String(x || "").trim()).filter(Boolean) : ["chart_main"];
      if (!slots.chart_main) {
        slots.chart_main = {
          categories: Array.isArray(chart?.categories) ? chart.categories : [],
          series: Array.isArray(chart?.series) ? chart.series : [],
        };
      }
      const artifactRoot = path.join(ctx.outputRoot, ctx.runId, "artifacts");
      fs.mkdirSync(artifactRoot, { recursive: true });
      const bindingPath = path.join(artifactRoot, "office_slot_binding.json");
      const validationPath = path.join(artifactRoot, "office_template_validation.json");
      const missingSlots = requiredSlots.filter((k) => !(k in slots));
      const emptySlots = requiredSlots.filter((k) => {
        const v = slots[k];
        if (v === null || v === undefined) return true;
        if (Array.isArray(v)) return v.length === 0;
        if (typeof v === "object") return Object.keys(v).length === 0;
        return String(v).trim() === "";
      });
      const payload = {
        run_id: String(ctx.runId || ""),
        workflow_id: String(ctx.workflowId || ""),
        template_kind: String(cfg.template_kind || "pptx"),
        template_version: templateVersion,
        required_slots: requiredSlots,
        slots,
      };
      fs.writeFileSync(bindingPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      const validation = {
        ok: missingSlots.length === 0,
        run_id: String(ctx.runId || ""),
        workflow_id: String(ctx.workflowId || ""),
        template_kind: payload.template_kind,
        template_version: templateVersion,
        required_slots: requiredSlots,
        missing_slots: missingSlots,
        empty_slots: emptySlots,
        checked_at: new Date().toISOString(),
      };
      fs.writeFileSync(validationPath, `${JSON.stringify(validation, null, 2)}\n`, "utf8");
      const warnings = [];
      if (missingSlots.length > 0) warnings.push(`missing_slots:${missingSlots.join(",")}`);
      if (emptySlots.length > 0) warnings.push(`empty_slots:${emptySlots.join(",")}`);
      return {
        ok: true,
        template_kind: payload.template_kind,
        template_version: templateVersion,
        slots,
        binding_path: bindingPath,
        validation_path: validationPath,
        warnings,
      };
    },
  });

  registry.register("ai_strategy_v1", {
    id: "chiplet.ai_strategy_v1",
    priority: 70,
    timeout_ms: Number(process.env.AIWF_CHIPLET_AI_TIMEOUT_MS || 240000),
    retries: 0,
    async run(ctx, node) {
      const dataGuard = shouldBlockAiOnData(ctx, node);
      if (dataGuard.block) {
        throw new Error(`ai_for_data_blocked:${dataGuard.reason}`);
      }
      const cfg = node?.config && typeof node.config === "object" ? node.config : {};
      const providers = Array.isArray(cfg.providers) && cfg.providers.length
        ? cfg.providers
        : (Array.isArray(ctx?.payload?.ai?.providers) ? ctx.payload.ai.providers : []);
      const corpusText = ctx.corpusText || "";
      const metrics = ctx.metrics || summarizeCorpus(corpusText);
      const attempts = [];
      const candidates = providers.length ? providers : [ctx?.payload?.ai || {}];
      let lastErr = null;
      for (const p of candidates) {
        try {
          const nextPayload = {
            ...ctx.payload,
            ai: {
              ...(ctx.payload?.ai && typeof ctx.payload.ai === "object" ? ctx.payload.ai : {}),
              ...(p && typeof p === "object" ? p : {}),
            },
          };
          let out = null;
          const isolationLevel = resolveIsolationLevel(ctx, "ai_strategy_v1", true, node);
          if (isolationLevel !== "none") {
            try {
              out = await runIsolatedTask("ai_call", {
                workflowPayload: nextPayload,
                corpusText,
                metrics,
                isolation_level: isolationLevel,
                sandbox_limits: resolveSandboxLimits(ctx, node),
              }, Number(process.env.AIWF_CHIPLET_AI_TIMEOUT_MS || 180000));
              out.isolated = true;
              out.isolation_level = isolationLevel;
            } catch (isolationErr) {
              if (isolationLevel === "sandbox") throw isolationErr;
              out = await callExternalAi(nextPayload, corpusText, metrics);
              out.isolated = false;
              out.isolation_level = "none";
              out.isolation_error = String(isolationErr);
            }
          } else {
            out = await callExternalAi(nextPayload, corpusText, metrics);
            out.isolated = false;
            out.isolation_level = "none";
          }
          ctx.aiText = out.text || "";
          ctx.aiProvider = String((p && (p.name || p.model || p.endpoint)) || "default");
          return {
            ok: true,
            selected_provider: ctx.aiProvider,
            attempts,
            ai_mode: out.reason,
            ai_text_chars: ctx.aiText.length,
            detail: out.detail || "",
            isolated: !!out.isolated,
            isolation_level: out.isolation_level || "none",
            isolation_error: out.isolation_error || "",
          };
        } catch (e) {
          attempts.push({
            provider: String((p && (p.name || p.model || p.endpoint)) || "default"),
            error: String(e),
          });
          lastErr = e;
        }
      }
      throw (lastErr || new Error("no_ai_provider_available"));
    },
  });

  registry.register("ai_refine", {
    id: "chiplet.ai_refine.v1",
    priority: 70,
    timeout_ms: Number(process.env.AIWF_CHIPLET_AI_TIMEOUT_MS || 240000),
    retries: Number(process.env.AIWF_CHIPLET_AI_RETRIES || 1),
    async run(ctx, node) {
      const dataGuard = shouldBlockAiOnData(ctx, node);
      if (dataGuard.block) {
        throw new Error(`ai_for_data_blocked:${dataGuard.reason}`);
      }
      const cfg = node?.config && typeof node.config === "object" ? node.config : {};
      const payloadAi = ctx?.payload?.ai && typeof ctx.payload.ai === "object" ? { ...ctx.payload.ai } : {};
      if (cfg.ai_endpoint) payloadAi.endpoint = String(cfg.ai_endpoint);
      if (cfg.ai_api_key) payloadAi.api_key = String(cfg.ai_api_key);
      if (cfg.ai_model) payloadAi.model = String(cfg.ai_model);
      if (cfg.provider_name) payloadAi.name = String(cfg.provider_name);
      const refinePayload = { ...(ctx?.payload || {}), ai: payloadAi };
      if (cfg.reuse_existing !== false && ctx.aiText) {
        return {
          ai_mode: "reuse_existing",
          ai_text_chars: ctx.aiText.length,
          detail: "reuse ai text from previous strategy node",
          isolated: false,
          isolation_level: "none",
          isolation_error: "",
        };
      }
      let refined = null;
      const corpusText = ctx.corpusText || "";
      const metrics = ctx.metrics || summarizeCorpus(corpusText);
      const isolationLevel = resolveIsolationLevel(ctx, "ai_refine", true, node);
      if (isolationLevel !== "none") {
        try {
          refined = await runIsolatedTask("ai_refine", {
            workflowPayload: refinePayload,
            corpusText,
            metrics,
            isolation_level: isolationLevel,
            sandbox_limits: resolveSandboxLimits(ctx, node),
          }, Number(process.env.AIWF_CHIPLET_AI_TIMEOUT_MS || 180000));
          refined.isolated = true;
          refined.isolation_level = isolationLevel;
        } catch (e) {
          if (isolationLevel === "sandbox") throw e;
          refined = await callExternalAi(refinePayload, corpusText, metrics);
          refined.isolated = false;
          refined.isolation_level = "none";
          refined.isolation_error = String(e);
        }
      } else {
        refined = await callExternalAi(refinePayload, corpusText, metrics);
        refined.isolated = false;
        refined.isolation_level = "none";
      }
      ctx.aiText = refined.text || "";
      ctx.aiTextSource = "ai_refine";
      return {
        ai_mode: refined.reason,
        ai_text_chars: ctx.aiText.length,
        detail: refined.detail || "",
        isolated: !!refined.isolated,
        isolation_level: refined.isolation_level || "none",
        isolation_error: refined.isolation_error || "",
      };
    },
  });

  registry.register("ai_audit", {
    id: "chiplet.ai_audit.v1",
    priority: 60,
    timeout_ms: 60000,
    retries: 0,
    async run(ctx, node) {
      const cfg = node?.config && typeof node.config === "object" ? node.config : {};
      const numericLock = cfg.numeric_lock !== false;
      const citationRequired = cfg.citation_required !== false;
      const recalcVerify = cfg.recalc_verify !== false;
      const allowedNewNumbers = Number.isFinite(Number(cfg.max_new_numbers))
        ? Math.max(0, Math.floor(Number(cfg.max_new_numbers)))
        : 0;

      const aiText = String(ctx.aiText || "");
      const corpusText = String(ctx.corpusText || "");
      const metrics = ctx.metrics || summarizeCorpus(corpusText);
      const reasonsExtra = [];

      if (numericLock) {
        const baseNums = new Set(extractNumericTokens(corpusText));
        const aiNums = extractNumericTokens(aiText);
        const newNums = aiNums.filter((x) => !baseNums.has(x));
        if (newNums.length > allowedNewNumbers) {
          reasonsExtra.push(`numeric_lock_failed:new_numbers=${newNums.slice(0, 10).join(",")}`);
        }
      }

      if (citationRequired && !hasCitationMarkers(aiText)) {
        reasonsExtra.push("citation_required_failed:no_citation_markers");
      }

      let recalc = null;
      if (recalcVerify) {
        const options = {
          run_id: ctx.runId,
          rust_endpoint: ctx?.payload?.rust?.endpoint,
          rust_required: ctx?.payload?.rust?.required !== false,
        };
        try {
          recalc = await computeViaRust(corpusText, options);
          const recalcMetrics = recalc?.metrics || summarizeCorpus(corpusText);
          const metricDiffs = compareMetricCore(metrics, recalcMetrics);
          const maxAllowedMetricDelta = Number.isFinite(Number(cfg.max_metric_delta))
            ? Math.max(0, Math.floor(Number(cfg.max_metric_delta)))
            : 0;
          const hardDiff = metricDiffs.filter((d) => Number(d.delta) > maxAllowedMetricDelta);
          if (hardDiff.length > 0) {
            reasonsExtra.push(`recalc_verify_failed:${hardDiff.map((d) => `${d.key}:${d.base}->${d.now}`).join("|")}`);
          }
          ctx.metrics = recalcMetrics;
        } catch (e) {
          reasonsExtra.push(`recalc_verify_error:${String(e)}`);
        }
      }

      ctx.audit = auditAiText(aiText, ctx.metrics || metrics);
      if (reasonsExtra.length) {
        ctx.audit.passed = false;
        ctx.audit.reasons = Array.isArray(ctx.audit.reasons) ? [...ctx.audit.reasons, ...reasonsExtra] : reasonsExtra;
      }
      ctx.audit.constraints = {
        numeric_lock: numericLock,
        citation_required: citationRequired,
        recalc_verify: recalcVerify,
      };
      ctx.audit.recalc = recalc && typeof recalc === "object"
        ? {
            mode: recalc.mode || "",
            started: !!recalc.started,
            metrics: recalc.metrics || null,
          }
        : null;
      if (!ctx.audit.passed) throw new Error(ctx.audit.reasons.join("; "));
      return ctx.audit;
    },
  });

  registry.register("md_output", {
    id: "chiplet.md_output.v1",
    priority: 50,
    timeout_ms: 60000,
    retries: 0,
    async run(ctx) {
      const source = nodeOutputByType(ctx, "clean_md");
      const artDir = source?.ai_corpus_path
        ? path.dirname(source.ai_corpus_path)
        : path.join(ctx.outputRoot, ctx.runId, "artifacts");
      fs.mkdirSync(artDir, { recursive: true });
      const summaryPath = path.join(artDir, "workflow_summary.md");
      writeWorkflowSummary(summaryPath, {
        run_id: ctx.runId,
        workflow_id: ctx.workflowId,
        clean_job_id: ctx.cleanResult?.job_id || "",
        metrics: ctx.metrics || summarizeCorpus(ctx.corpusText || ""),
        audit: ctx.audit || { passed: false, reasons: ["未执行审核节点"] },
        ai_text: ctx.aiText || "",
      });
      const summaryArtifact = {
        artifact_id: "md_workflow_summary_001",
        kind: "md",
        path: summaryPath,
        sha256: sha256Text(fs.readFileSync(summaryPath, "utf8")),
      };
      ctx.workflowSummaryArtifact = summaryArtifact;
      return summaryArtifact;
    },
  });

  return registry;
}

module.exports = {
  registerBuiltinWorkflowChiplets,
};
