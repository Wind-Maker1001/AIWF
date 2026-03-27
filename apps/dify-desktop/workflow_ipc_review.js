function registerWorkflowReviewIpc(ctx, deps) {
  const {
    ipcMain,
    dialog,
    app,
    fs,
    path,
    loadConfig,
    runMinimalWorkflow,
  } = ctx;
  const { workflowStoreRemoteErrorResult } = require("./workflow_store_remote_error");
  const {
    isMockIoAllowed,
    resolveMockFilePath,
    getRun,
    normalizeWorkflowConfig,
    applyQualityRuleSetToPayload,
    applySandboxAutoFixPayload,
    attachQualityGate,
    resolveOutputRoot,
    createNodeCacheApi,
    appendDiagnostics,
    appendRunHistory,
    extractSandboxViolations,
    appendSandboxViolationAudit,
    maybeApplySandboxAutoFix,
    enqueueReviews,
    sandboxRuleStore,
    sandboxAutoFixStore,
    workflowManualReviewStore,
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

  ipcMain.handle("aiwf:listManualReviews", async () => {
    return await workflowManualReviewStore.listQueue();
  });

  ipcMain.handle("aiwf:listManualReviewHistory", async (_evt, req) => {
    return await workflowManualReviewStore.listHistory(req);
  });

  ipcMain.handle("aiwf:exportManualReviewHistory", async (_evt, req) => {
    try {
      const listed = await workflowManualReviewStore.listHistory({
        limit: 5000,
        filter: req?.filter || {},
      });
      if (!listed?.ok) return listed;
      const items = Array.isArray(listed.items) ? listed.items : [];
      const allowMockIo = isMockIoAllowed();
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        const safe = resolveMockFilePath(req.path);
        if (!safe.ok) return safe;
        filePath = safe.path;
      } else {
        const out = await dialog.showSaveDialog({
          title: "导出审核历史",
          defaultPath: path.join(app.getPath("documents"), "aiwf_manual_review_history.json"),
          filters: [{ name: "JSON", extensions: ["json"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (out.canceled || !out.filePath) return { ok: false, canceled: true };
        filePath = out.filePath;
      }
      const payload = {
        exported_at: new Date().toISOString(),
        total: items.length,
        items,
      };
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      return { ok: true, path: filePath, total: items.length };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:submitManualReview", async (_evt, req) => {
    try {
      const runId = String(req?.run_id || "").trim();
      const reviewKey = String(req?.review_key || "").trim();
      const approved = !!req?.approved;
      const reviewer = String(req?.reviewer || "reviewer").trim();
      const comment = String(req?.comment || "").trim();
      const mergedCfg = normalizeWorkflowConfig({ ...loadConfig(), ...(req?.cfg || {}) });
      const submitted = await workflowManualReviewStore.submit({
        run_id: runId,
        review_key: reviewKey,
        approved,
        reviewer,
        comment,
      }, mergedCfg);
      if (!submitted?.ok) return submitted;
      const hist = submitted.item;
      let resumed = null;
      const autoResume = req?.auto_resume !== false;
      if (autoResume) {
        const found = await getRun(runId, mergedCfg);
        if (found) {
          const basePayload = found.payload && typeof found.payload === "object" ? found.payload : {};
          const replayPayload = {
            ...basePayload,
            manual_review: {
              ...(basePayload.manual_review && typeof basePayload.manual_review === "object" ? basePayload.manual_review : {}),
              [reviewKey]: {
                approved: !!hist?.approved,
                reviewer: String(hist?.reviewer || reviewer),
                comment: String(hist?.comment || comment),
              },
            },
            resume: {
              run_id: runId,
              node_id: String(hist?.node_id || reviewKey || ""),
              outputs: found?.result?.node_outputs || {},
            },
          };
          const merged = normalizeWorkflowConfig({ ...loadConfig(), ...(found.config || {}), ...(req?.cfg || {}) });
          const rulesOut = await sandboxRuleStore.getRuntimeRules(merged);
          if (!rulesOut?.ok) return rulesOut;
          const effectivePayload = await applyQualityRuleSetToPayload(
            await sandboxAutoFixStore.applyPayload(replayPayload, merged),
            merged
          );
          let out = attachQualityGate(await runMinimalWorkflow({
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
          extractSandboxViolations(out).forEach((item) => appendSandboxViolationAudit(item, effectivePayload || {}, rulesOut.rules || {}));
          if (!out?.review_enqueue_failed) {
            await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, merged);
          }
          resumed = out;
        }
      }
      const resumeOk = !resumed || !!resumed?.ok;
      return {
        ok: resumeOk,
        review_saved: true,
        provider: String(submitted?.provider || ""),
        item: hist,
        remaining: Number(submitted?.remaining || 0),
        resumed,
        run_id: String(resumed?.run_id || ""),
        status: String(resumed?.status || ""),
        error: String(resumed?.error || ""),
        error_code: String(resumed?.error_code || ""),
        error_item_contract: String(resumed?.error_item_contract || ""),
        graph_contract: String(resumed?.graph_contract || ""),
        error_items: Array.isArray(resumed?.error_items) ? resumed.error_items : [],
        review_enqueue_failed: !!resumed?.review_enqueue_failed,
        review_enqueue: resumed?.review_enqueue && typeof resumed.review_enqueue === "object" ? resumed.review_enqueue : null,
      };
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  });
}

module.exports = {
  registerWorkflowReviewIpc,
};
