function registerWorkflowRunIpc(ctx, deps) {
  const {
    ipcMain,
    createWorkflowWindow,
    loadConfig,
    runMinimalWorkflow,
  } = ctx;
  const {
    normalizeWorkflowConfig,
    resolveOutputRoot,
    createNodeCacheApi,
    appendDiagnostics,
    appendRunHistory,
    extractSandboxViolations,
    appendAudit,
    getRun,
    enqueueReviews,
    reportSupport,
    sandboxSupport,
    sandboxRuleStore,
    sandboxAutoFixStore,
  } = deps;

  ipcMain.handle("aiwf:openWorkflowStudio", async () => {
    createWorkflowWindow();
    return { ok: true };
  });

  ipcMain.handle("aiwf:runWorkflow", async (_evt, payload, cfg) => {
    const merged = normalizeWorkflowConfig({ ...loadConfig(), ...(cfg || {}) });
    const rulesOut = await sandboxRuleStore.getRuntimeRules(merged);
    if (!rulesOut?.ok) return rulesOut;
    const effectivePayload = await reportSupport.applyQualityRuleSetToPayload(
      await sandboxAutoFixStore.applyPayload(payload || {}, merged),
      merged
    );
    const out = sandboxSupport.attachQualityGate(await runMinimalWorkflow({
      payload: effectivePayload,
      config: merged,
      outputRoot: resolveOutputRoot(merged),
      nodeCache: createNodeCacheApi(),
    }), effectivePayload || {});
    appendDiagnostics(out);
    appendRunHistory(out, effectivePayload, merged);
    extractSandboxViolations(out).forEach((item) => sandboxSupport.appendSandboxViolationAudit(item, effectivePayload || {}, rulesOut.rules || {}));
    await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, merged);
    appendAudit("run_workflow", {
      run_id: String(out?.run_id || ""),
      workflow_id: String(out?.workflow_id || ""),
      ok: !!out?.ok,
      status: String(out?.status || ""),
    });
    if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) {
      await enqueueReviews(out.pending_reviews, merged);
    }
    return out;
  });

  ipcMain.handle("aiwf:replayWorkflowRun", async (_evt, req, cfg) => {
    try {
      const runId = String(req?.run_id || "").trim();
      const nodeId = String(req?.node_id || "").trim();
      const merged = normalizeWorkflowConfig({ ...loadConfig(), ...(cfg || {}) });
      const found = await getRun(runId, merged);
      if (!found) return { ok: false, error: `run not found: ${runId}` };
      const basePayload = found.payload && typeof found.payload === "object" ? found.payload : {};
      const replayPayload = {
        ...basePayload,
        manual_review: req?.manual_review && typeof req.manual_review === "object"
          ? req.manual_review
          : (basePayload.manual_review || {}),
        resume: {
          run_id: runId,
          node_id: nodeId,
          outputs: found?.result?.node_outputs || {},
        },
      };
      const replayMerged = normalizeWorkflowConfig({ ...loadConfig(), ...(found.config || {}), ...(cfg || {}) });
      const rulesOut = await sandboxRuleStore.getRuntimeRules(replayMerged);
      if (!rulesOut?.ok) return rulesOut;
      const effectivePayload = await reportSupport.applyQualityRuleSetToPayload(
        await sandboxAutoFixStore.applyPayload(replayPayload, replayMerged),
        replayMerged
      );
      const out = sandboxSupport.attachQualityGate(await runMinimalWorkflow({
        payload: effectivePayload,
        config: replayMerged,
        outputRoot: resolveOutputRoot(replayMerged),
        nodeCache: createNodeCacheApi(),
      }), effectivePayload || {});
      appendDiagnostics(out);
      appendRunHistory(out, effectivePayload, replayMerged);
      extractSandboxViolations(out).forEach((item) => sandboxSupport.appendSandboxViolationAudit(item, effectivePayload || {}, rulesOut.rules || {}));
      await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, replayMerged);
      if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) {
        await enqueueReviews(out.pending_reviews, replayMerged);
      }
      return { ok: true, replay_of: runId, resumed_from: nodeId || null, result: out };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });
}

module.exports = {
  registerWorkflowRunIpc,
};
