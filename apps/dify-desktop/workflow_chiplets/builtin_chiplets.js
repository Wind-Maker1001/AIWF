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
    async run(ctx) {
      const params = ctx.payload.params || {};
      ctx.cleanResult = await runOfflineCleaning({
        params: {
          ...params,
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
    async run(ctx) {
      let computed = null;
      const options = {
        run_id: ctx.runId,
        rust_endpoint: ctx.payload.rust?.endpoint,
        rust_required: ctx.payload.rust?.required !== false,
      };
      if (typeof runIsolatedTask === "function") {
        try {
          computed = await runIsolatedTask("compute_rust", {
            corpusText: ctx.corpusText || "",
            options,
          }, Number(process.env.AIWF_CHIPLET_COMPUTE_TIMEOUT_MS || 120000));
          computed.isolated = true;
        } catch (e) {
          computed = await computeViaRust(ctx.corpusText || "", options);
          computed.isolated = false;
          computed.isolation_error = String(e);
        }
      } else {
        computed = await computeViaRust(ctx.corpusText || "", options);
        computed.isolated = false;
      }
      ctx.metrics = computed.metrics;
      return {
        engine: computed.mode,
        metrics: computed.metrics,
        rust_started: computed.started || false,
        rust_path: computed.rust_path || "",
        isolated: !!computed.isolated,
        isolation_error: computed.isolation_error || "",
      };
    },
  };
  registry.register("compute_rust", computeChiplet);
  registry.register("compute_rust_placeholder", computeChiplet);

  registry.register("ai_refine", {
    id: "chiplet.ai_refine.v1",
    priority: 70,
    timeout_ms: Number(process.env.AIWF_CHIPLET_AI_TIMEOUT_MS || 240000),
    retries: Number(process.env.AIWF_CHIPLET_AI_RETRIES || 1),
    async run(ctx) {
      let refined = null;
      const corpusText = ctx.corpusText || "";
      const metrics = ctx.metrics || summarizeCorpus(corpusText);
      if (typeof runIsolatedTask === "function") {
        try {
          refined = await runIsolatedTask("ai_refine", {
            workflowPayload: ctx.payload,
            corpusText,
            metrics,
          }, Number(process.env.AIWF_CHIPLET_AI_TIMEOUT_MS || 180000));
          refined.isolated = true;
        } catch (e) {
          refined = await callExternalAi(ctx.payload, corpusText, metrics);
          refined.isolated = false;
          refined.isolation_error = String(e);
        }
      } else {
        refined = await callExternalAi(ctx.payload, corpusText, metrics);
        refined.isolated = false;
      }
      ctx.aiText = refined.text || "";
      return {
        ai_mode: refined.reason,
        ai_text_chars: ctx.aiText.length,
        detail: refined.detail || "",
        isolated: !!refined.isolated,
        isolation_error: refined.isolation_error || "",
      };
    },
  });

  registry.register("ai_audit", {
    id: "chiplet.ai_audit.v1",
    priority: 60,
    timeout_ms: 60000,
    retries: 0,
    async run(ctx) {
      ctx.audit = auditAiText(ctx.aiText || "", ctx.metrics || summarizeCorpus(ctx.corpusText || ""));
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
