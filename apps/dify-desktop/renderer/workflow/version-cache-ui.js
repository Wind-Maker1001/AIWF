function createWorkflowVersionCacheUi(els, deps = {}) {
  const {
    setStatus = () => {},
    renderVersionRows = () => {},
    renderVersionCompare = () => {},
    renderCacheStats = () => {},
  } = deps;

  async function refreshVersions() {
    try {
      const out = await window.aiwfDesktop.listWorkflowVersions({ limit: 120 });
      renderVersionRows(out?.items || []);
    } catch {
      renderVersionRows([]);
    }
  }

  async function compareVersions() {
    const a = String(els.versionCompareA?.value || "").trim();
    const b = String(els.versionCompareB?.value || "").trim();
    if (!a || !b) {
      setStatus("请填写版本 A/B", false);
      return;
    }
    const out = await window.aiwfDesktop.compareWorkflowVersions({ version_a: a, version_b: b });
    renderVersionCompare(out);
    setStatus(out?.ok ? "版本对比完成" : `版本对比失败: ${out?.error || "unknown"}`, !!out?.ok);
  }

  async function refreshCacheStats() {
    try {
      const out = await window.aiwfDesktop.getWorkflowNodeCacheStats();
      renderCacheStats(out?.stats || {});
    } catch {
      renderCacheStats({});
    }
  }

  async function clearCache() {
    const out = await window.aiwfDesktop.clearWorkflowNodeCache();
    renderCacheStats(out?.stats || {});
    setStatus(out?.ok ? "缓存已清空" : `清空缓存失败: ${out?.error || "unknown"}`, !!out?.ok);
  }

  return {
    refreshVersions,
    compareVersions,
    refreshCacheStats,
    clearCache,
  };
}

export { createWorkflowVersionCacheUi };
