import {
  normalizeQualityGateExportFormat,
  parseQualityGatePrefs,
} from "./quality-gate-support.js";

function createWorkflowQualityGateUi(els, deps = {}) {
  const {
    setStatus = () => {},
    prefsStorageKey = "aiwf.workflow.qualityGatePrefs.v1",
    qualityGatePrefsPayload = () => ({}),
    qualityGateFilterPayload = () => ({}),
    renderQualityGateRows = () => {},
  } = deps;

  function saveQualityGatePrefs() {
    try {
      localStorage.setItem(prefsStorageKey, JSON.stringify(qualityGatePrefsPayload()));
    } catch {}
  }

  function loadQualityGatePrefs() {
    try {
      const prefs = parseQualityGatePrefs(localStorage.getItem(prefsStorageKey));
      if (!prefs) return;
      if (els.qualityGateRunIdFilter) els.qualityGateRunIdFilter.value = prefs.filter.run_id;
      if (els.qualityGateStatusFilter) els.qualityGateStatusFilter.value = prefs.filter.status;
      if (els.qualityGateExportFormat) els.qualityGateExportFormat.value = prefs.format;
    } catch {}
  }

  async function refreshQualityGateReports() {
    try {
      saveQualityGatePrefs();
      const out = await window.aiwfDesktop.listWorkflowQualityGateReports({
        limit: 120,
        filter: qualityGateFilterPayload(),
      });
      renderQualityGateRows(out?.items || []);
    } catch {
      renderQualityGateRows([]);
    }
  }

  async function exportQualityGateReports() {
    saveQualityGatePrefs();
    const out = await window.aiwfDesktop.exportWorkflowQualityGateReports({
      limit: 500,
      format: normalizeQualityGateExportFormat(els.qualityGateExportFormat?.value || "md"),
      filter: qualityGateFilterPayload(),
    });
    if (!out?.ok) {
      if (!out?.canceled) {
        setStatus(`导出质量门禁失败: ${out?.error || "unknown"}`, false);
      }
      return;
    }
    setStatus(`质量门禁报告已导出: ${out.path}`, true);
  }

  return {
    saveQualityGatePrefs,
    loadQualityGatePrefs,
    refreshQualityGateReports,
    exportQualityGateReports,
  };
}

export { createWorkflowQualityGateUi };
