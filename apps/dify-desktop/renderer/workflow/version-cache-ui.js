import {
  cacheStatsStatusText,
  versionComparePayload,
  versionListRequestPayload,
} from "./version-cache-support.js";

function createWorkflowVersionCacheUi(els, deps = {}) {
  const {
    setStatus = () => {},
    renderVersionRows = () => {},
    renderVersionCompare = () => {},
    renderCacheStats = () => {},
  } = deps;

  async function refreshVersions() {
    try {
      const out = await window.aiwfDesktop.listWorkflowVersions(versionListRequestPayload());
      renderVersionRows(out?.items || []);
    } catch {
      renderVersionRows([]);
    }
  }

  async function compareVersions() {
    const payload = versionComparePayload(els.versionCompareA?.value || "", els.versionCompareB?.value || "");
    if (!payload.version_a || !payload.version_b) {
      setStatus("请填写版本 A/B", false);
      return;
    }
    const out = await window.aiwfDesktop.compareWorkflowVersions(payload);
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
    setStatus(cacheStatsStatusText(!!out?.ok, out?.error), !!out?.ok);
  }

  return {
    refreshVersions,
    compareVersions,
    refreshCacheStats,
    clearCache,
  };
}

export { createWorkflowVersionCacheUi };
