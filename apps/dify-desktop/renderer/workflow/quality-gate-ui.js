function createWorkflowQualityGateUi(els, deps = {}) {
  const {
    setStatus = () => {},
    saveQualityGatePrefs = () => {},
    qualityGateFilterPayload = () => ({}),
    renderQualityGateRows = () => {},
  } = deps;

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
    refreshQualityGateReports,
    exportQualityGateReports,
  };
}

export { createWorkflowQualityGateUi };
