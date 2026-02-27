function registerCoreDomainChiplets(registry, deps = {}, helpers = {}) {
  const {
    fs,
    runOfflineCleaning,
    collectFiles,
    readArtifactById,
    computeViaRust,
    runIsolatedTask,
  } = deps;
  const {
    resolveIsolationLevel,
    resolveSandboxLimits,
  } = helpers;

  registry.register("ingest_files", {
    id: "chiplet.ingest_files.v1",
    priority: 120,
    timeout_ms: 60000,
    retries: 0,
    async run(ctx) {
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
          || "unassigned"
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
}

module.exports = {
  registerCoreDomainChiplets,
};
