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
          const out = attachQualityGate(await runMinimalWorkflow({
            payload: effectivePayload,
            config: merged,
            outputRoot: resolveOutputRoot(merged),
            nodeCache: createNodeCacheApi(),
          }), effectivePayload || {});
          appendDiagnostics(out);
          appendRunHistory(out, effectivePayload, merged);
          extractSandboxViolations(out).forEach((item) => appendSandboxViolationAudit(item, effectivePayload || {}, rulesOut.rules || {}));
          await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, merged);
          if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) {
            await enqueueReviews(out.pending_reviews, merged);
          }
          resumed = out;
        }
      }
      return { ok: true, provider: String(submitted?.provider || ""), item: hist, remaining: Number(submitted?.remaining || 0), resumed };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });
}

module.exports = {
  registerWorkflowReviewIpc,
};
