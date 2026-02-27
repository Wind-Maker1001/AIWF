function registerAiDomainChiplets(registry, deps = {}, helpers = {}) {
  const {
    callExternalAi,
    auditAiText,
    summarizeCorpus,
    computeViaRust,
    runIsolatedTask,
  } = deps;
  const {
    shouldBlockAiOnData,
    enforceAiBudgetBeforeCall,
    recordAiBudgetAfterCall,
    resolveIsolationLevel,
    resolveSandboxLimits,
    extractNumericTokens,
    hasCitationMarkers,
    compareMetricCore,
  } = helpers;

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
          enforceAiBudgetBeforeCall(ctx, corpusText);
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
          recordAiBudgetAfterCall(ctx, corpusText, ctx.aiText);
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
      enforceAiBudgetBeforeCall(ctx, corpusText);
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
      recordAiBudgetAfterCall(ctx, corpusText, ctx.aiText);
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
}

module.exports = {
  registerAiDomainChiplets,
};
