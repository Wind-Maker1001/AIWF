function bindIfPresent(target, eventName, handler) {
  if (target?.addEventListener) target.addEventListener(eventName, handler);
}

function bindWorkflowCoreToolbarActions(ctx = {}) {
  const {
    els,
    setStatus = () => {},
    handleAddNode = () => {},
    resetWorkflow = () => {},
    clearWorkflow = () => {},
    runWorkflowPreflight = async () => ({ ok: true, issues: [] }),
    exportPreflightReport = () => {},
    autoFixGraphStructure = () => ({ changed: false }),
    setLastAutoFixSummary = () => {},
    renderAutoFixDiff = () => {},
    runWorkflow = () => {},
    enqueueWorkflowRun = () => {},
    refreshQueue = () => {},
    pauseQueue = () => {},
    resumeQueue = () => {},
    refreshVersions = () => {},
    compareVersions = () => {},
    refreshCacheStats = () => {},
    clearCache = () => {},
    exportJson = () => {},
    saveFlow = () => {},
    loadFlow = () => {},
  } = ctx;

  els.btnAdd.addEventListener("click", handleAddNode);
  els.btnReset.addEventListener("click", resetWorkflow);
  els.btnClear.addEventListener("click", clearWorkflow);

  bindIfPresent(els.btnPreflight, "click", async () => {
    const out = await runWorkflowPreflight();
    if (out.ok) {
      const warns = (out.issues || []).filter((item) => String(item.level || "") === "warning").length;
      setStatus(warns > 0 ? `预检通过（${warns} 条警告）` : "预检通过", true);
    } else {
      const errs = (out.issues || []).filter((item) => String(item.level || "") === "error").length;
      setStatus(`预检失败（${errs} 条错误）`, false);
    }
  });
  bindIfPresent(els.btnPreflightExport, "click", exportPreflightReport);
  bindIfPresent(els.btnAutoFixGraph, "click", async () => {
    const out = autoFixGraphStructure();
    setLastAutoFixSummary(out);
    renderAutoFixDiff(out);
    if (!out.changed) {
      setStatus("未发现可自动修复的问题", true);
      return;
    }
    setStatus(
      `自动修复完成: 重复连线-${out.removed_dup_edges}, 自环-${out.removed_self_loops}, 断裂连线-${out.removed_broken_edges}, 孤立节点-${out.removed_isolated_nodes}`,
      true
    );
    await runWorkflowPreflight();
  });

  els.btnRun.addEventListener("click", runWorkflow);
  bindIfPresent(els.btnEnqueueRun, "click", enqueueWorkflowRun);
  bindIfPresent(els.btnQueueRefresh, "click", refreshQueue);
  bindIfPresent(els.btnQueuePause, "click", pauseQueue);
  bindIfPresent(els.btnQueueResume, "click", resumeQueue);
  bindIfPresent(els.btnVersionsRefresh, "click", refreshVersions);
  bindIfPresent(els.btnCompareVersions, "click", compareVersions);
  bindIfPresent(els.btnCacheRefresh, "click", refreshCacheStats);
  bindIfPresent(els.btnCacheClear, "click", clearCache);
  els.btnExport.addEventListener("click", exportJson);
  els.btnSaveFlow.addEventListener("click", saveFlow);
  els.btnLoadFlow.addEventListener("click", loadFlow);
}

export {
  bindIfPresent,
  bindWorkflowCoreToolbarActions,
};
