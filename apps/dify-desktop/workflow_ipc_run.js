function registerWorkflowRunIpc(ctx, deps) {
  const {
    ipcMain,
    createWorkflowWindow,
    loadConfig,
    runMinimalWorkflow,
  } = ctx;
  const { workflowStoreRemoteErrorResult } = require("./workflow_store_remote_error");
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

  function applyPendingReviewEnqueueResult(out, enqueueOut) {
    if (!enqueueOut?.ok) {
      const reviewEnqueue = {
        ok: false,
        error: String(enqueueOut?.error || "manual review enqueue failed"),
        error_code: String(enqueueOut?.error_code || "manual_review_enqueue_failed"),
        error_item_contract: String(enqueueOut?.error_item_contract || ""),
        graph_contract: String(enqueueOut?.graph_contract || ""),
        error_items: Array.isArray(enqueueOut?.error_items) ? enqueueOut.error_items : [],
      };
      return {
        ...(out && typeof out === "object" ? out : {}),
        review_enqueue_failed: true,
        review_enqueue: reviewEnqueue,
      };
    }
    return out;
  }

  function normalizePendingReviews(items, out, payload = null) {
    const workflow = payload?.workflow && typeof payload.workflow === "object" ? payload.workflow : {};
    const fallbackRunId = String(out?.run_id || "").trim();
    const fallbackWorkflowId = String(out?.workflow_id || workflow.workflow_id || "").trim();
    return (Array.isArray(items) ? items : []).map((item) => {
      const source = item && typeof item === "object" ? item : {};
      const reviewKey = String(source.review_key || source.node_id || "").trim();
      return {
        ...source,
        run_id: String(source.run_id || fallbackRunId).trim(),
        workflow_id: String(source.workflow_id || fallbackWorkflowId).trim(),
        node_id: String(source.node_id || reviewKey).trim(),
        review_key: reviewKey,
        status: String(source.status || "pending").trim().toLowerCase() || "pending",
      };
    });
  }

  ipcMain.handle("aiwf:openWorkflowStudio", async () => {
    createWorkflowWindow();
    return { ok: true };
  });

  ipcMain.handle("aiwf:runWorkflow", async (_evt, payload, cfg) => {
    try {
      const merged = normalizeWorkflowConfig({ ...loadConfig(), ...(cfg || {}) });
      const rulesOut = await sandboxRuleStore.getRuntimeRules(merged);
      if (!rulesOut?.ok) return rulesOut;
      const effectivePayload = await reportSupport.applyQualityRuleSetToPayload(
        await sandboxAutoFixStore.applyPayload(payload || {}, merged),
        merged
      );
      let out = sandboxSupport.attachQualityGate(await runMinimalWorkflow({
        payload: effectivePayload,
        config: merged,
        outputRoot: resolveOutputRoot(merged),
        nodeCache: createNodeCacheApi(),
      }), effectivePayload || {});
      if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) {
        const enqueueOut = await enqueueReviews(normalizePendingReviews(out.pending_reviews, out, effectivePayload), merged);
        out = applyPendingReviewEnqueueResult(out, enqueueOut);
      }
      appendDiagnostics(out);
      appendRunHistory(out, effectivePayload, merged);
      extractSandboxViolations(out).forEach((item) => sandboxSupport.appendSandboxViolationAudit(item, effectivePayload || {}, rulesOut.rules || {}));
      if (out?.review_enqueue_failed) {
        appendAudit("run_workflow", {
          run_id: String(out?.run_id || ""),
          workflow_id: String(out?.workflow_id || ""),
          ok: !!out?.ok,
          status: String(out?.status || ""),
        });
        return out;
      }
      await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, merged);
      appendAudit("run_workflow", {
        run_id: String(out?.run_id || ""),
        workflow_id: String(out?.workflow_id || ""),
        ok: !!out?.ok,
        status: String(out?.status || ""),
      });
      return out;
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
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
      let out = sandboxSupport.attachQualityGate(await runMinimalWorkflow({
        payload: effectivePayload,
        config: replayMerged,
        outputRoot: resolveOutputRoot(replayMerged),
        nodeCache: createNodeCacheApi(),
      }), effectivePayload || {});
      if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) {
        const enqueueOut = await enqueueReviews(normalizePendingReviews(out.pending_reviews, out, effectivePayload), replayMerged);
        out = applyPendingReviewEnqueueResult(out, enqueueOut);
      }
      appendDiagnostics(out);
      appendRunHistory(out, effectivePayload, replayMerged);
      extractSandboxViolations(out).forEach((item) => sandboxSupport.appendSandboxViolationAudit(item, effectivePayload || {}, rulesOut.rules || {}));
      if (out?.review_enqueue_failed) {
        return {
          ok: false,
          replay_of: runId,
          resumed_from: nodeId || null,
          result: out,
          run_id: String(out?.run_id || ""),
          status: String(out?.status || ""),
          error: String(out?.error || ""),
          error_code: String(out?.error_code || ""),
          error_item_contract: String(out?.error_item_contract || ""),
          graph_contract: String(out?.graph_contract || ""),
          error_items: Array.isArray(out?.error_items) ? out.error_items : [],
          review_enqueue_failed: !!out?.review_enqueue_failed,
          review_enqueue: out?.review_enqueue && typeof out.review_enqueue === "object" ? out.review_enqueue : null,
        };
      }
      await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, replayMerged);
      return {
        ok: !!out?.ok,
        replay_of: runId,
        resumed_from: nodeId || null,
        result: out,
        run_id: String(out?.run_id || ""),
        status: String(out?.status || ""),
        error: String(out?.error || ""),
        error_code: String(out?.error_code || ""),
        error_item_contract: String(out?.error_item_contract || ""),
        graph_contract: String(out?.graph_contract || ""),
        error_items: Array.isArray(out?.error_items) ? out.error_items : [],
        review_enqueue_failed: !!out?.review_enqueue_failed,
        review_enqueue: out?.review_enqueue && typeof out.review_enqueue === "object" ? out.review_enqueue : null,
      };
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  });
}

module.exports = {
  registerWorkflowRunIpc,
};
