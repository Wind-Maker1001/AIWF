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
      const raw = localStorage.getItem(prefsStorageKey);
      if (!raw) return;
      const obj = JSON.parse(raw);
      const filter = obj?.filter && typeof obj.filter === "object" ? obj.filter : {};
      if (els.qualityGateRunIdFilter) els.qualityGateRunIdFilter.value = String(filter.run_id || "");
      const status = String(filter.status || "all").trim().toLowerCase();
      if (els.qualityGateStatusFilter) {
        els.qualityGateStatusFilter.value = (status === "blocked" || status === "pass") ? status : "all";
      }
      const fmt = String(obj?.format || "md").trim().toLowerCase();
      if (els.qualityGateExportFormat) els.qualityGateExportFormat.value = fmt === "json" ? "json" : "md";
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
      format: String(els.qualityGateExportFormat?.value || "md").trim().toLowerCase() === "json" ? "json" : "md",
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
