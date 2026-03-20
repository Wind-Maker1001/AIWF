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
    loadReviewQueue,
    saveReviewQueue,
    listReviewHistory,
    filterReviewHistory,
    isMockIoAllowed,
    resolveMockFilePath,
    findRunById,
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
  } = deps;

  ipcMain.handle("aiwf:listManualReviews", async () => {
    return { ok: true, items: loadReviewQueue() };
  });

  ipcMain.handle("aiwf:listManualReviewHistory", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    const safe = Number.isFinite(limit) ? Math.max(1, Math.min(2000, Math.floor(limit))) : 200;
    const items = listReviewHistory(safe);
    return { ok: true, items: filterReviewHistory(items, req?.filter || {}) };
  });

  ipcMain.handle("aiwf:exportManualReviewHistory", async (_evt, req) => {
    try {
      const items = filterReviewHistory(listReviewHistory(5000), req?.filter || {});
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
      const queue = loadReviewQueue();
      const index = queue.findIndex((item) => String(item.run_id || "") === runId && String(item.review_key || "") === reviewKey);
      if (index < 0) return { ok: false, error: "review task not found" };
      const task = queue[index];
      queue.splice(index, 1);
      saveReviewQueue(queue);
      const hist = {
        ...task,
        approved,
        reviewer,
        comment,
        status: approved ? "approved" : "rejected",
        decided_at: new Date().toISOString(),
      };
      appendReviewHistory(hist);
      let resumed = null;
      const autoResume = req?.auto_resume !== false;
      if (autoResume) {
        const found = findRunById(runId);
        if (found) {
          const basePayload = found.payload && typeof found.payload === "object" ? found.payload : {};
          const replayPayload = {
            ...basePayload,
            manual_review: {
              ...(basePayload.manual_review && typeof basePayload.manual_review === "object" ? basePayload.manual_review : {}),
              [reviewKey]: {
                approved,
                reviewer,
                comment,
              },
            },
            resume: {
              run_id: runId,
              node_id: String(task.node_id || reviewKey || ""),
              outputs: found?.result?.node_outputs || {},
            },
          };
          const merged = normalizeWorkflowConfig({ ...loadConfig(), ...(found.config || {}) });
          const effectivePayload = applyQualityRuleSetToPayload(applySandboxAutoFixPayload(replayPayload));
          const out = attachQualityGate(await runMinimalWorkflow({
            payload: effectivePayload,
            config: merged,
            outputRoot: resolveOutputRoot(merged),
            nodeCache: createNodeCacheApi(),
          }), effectivePayload || {});
          appendDiagnostics(out);
          appendRunHistory(out, effectivePayload, merged);
          extractSandboxViolations(out).forEach((item) => appendSandboxViolationAudit(item, effectivePayload || {}));
          maybeApplySandboxAutoFix(out, effectivePayload || {});
          if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) enqueueReviews(out.pending_reviews);
          resumed = out;
        }
      }
      return { ok: true, item: hist, remaining: queue.length, resumed };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  function appendReviewHistory(item) {
    const filePath = deps.reviewHistoryPath();
    try {
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.appendFileSync(filePath, `${JSON.stringify(item)}\n`, "utf8");
    } catch {}
  }
}

module.exports = {
  registerWorkflowReviewIpc,
};
