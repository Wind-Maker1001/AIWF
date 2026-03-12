function createWorkflowPreflightActionsUi(els, deps = {}) {
  const {
    setStatus = () => {},
    runWorkflowPreflight = async () => ({ ok: true, issues: [] }),
    allTemplates = () => [],
    currentTemplateGovernance = () => ({}),
    autoFixGraphStructure = () => ({}),
    renderAutoFixDiff = () => {},
    getLastPreflightReport = () => null,
    getLastTemplateAcceptanceReport = () => null,
    setLastAutoFixSummary = () => {},
    setLastTemplateAcceptanceReport = () => {},
  } = deps;

  async function exportPreflightReport() {
    const report = getLastPreflightReport() || await runWorkflowPreflight();
    const format = String(els.preflightExportFormat?.value || "md").trim().toLowerCase() === "json" ? "json" : "md";
    const out = await window.aiwfDesktop.exportWorkflowPreflightReport({
      report,
      format,
    });
    if (!out?.ok) {
      if (!out?.canceled) setStatus(`导出预检报告失败: ${out?.error || "unknown"}`, false);
      return;
    }
    setStatus(`预检报告已导出: ${out.path}`, true);
  }

  async function runTemplateAcceptance() {
    const id = String(els.templateSelect?.value || "").trim();
    const tpl = allTemplates().find((x) => String(x.id || "") === id);
    const before = await runWorkflowPreflight();
    const fix = autoFixGraphStructure();
    setLastAutoFixSummary(fix);
    renderAutoFixDiff(fix);
    const after = await runWorkflowPreflight();
    const accepted = !!after?.ok;
    const report = {
      ts: new Date().toISOString(),
      template_id: id || "",
      template_name: String(tpl?.name || ""),
      accepted,
      governance: currentTemplateGovernance(),
      before,
      auto_fix: fix,
      after,
    };
    setLastTemplateAcceptanceReport(report);
    if (accepted) setStatus("模板验收通过", true);
    else setStatus("模板验收未通过：仍有错误，请修复后重试", false);
    if (els.log) els.log.textContent = JSON.stringify(report, null, 2);
    return report;
  }

  async function exportTemplateAcceptanceReport() {
    const report = getLastTemplateAcceptanceReport() || await runTemplateAcceptance();
    const format = String(els.templateAcceptanceExportFormat?.value || "md").trim().toLowerCase() === "json" ? "json" : "md";
    const out = await window.aiwfDesktop.exportWorkflowTemplateAcceptanceReport({
      report,
      format,
    });
    if (!out?.ok) {
      if (!out?.canceled) setStatus(`导出模板验收报告失败: ${out?.error || "unknown"}`, false);
      return;
    }
    setStatus(`模板验收报告已导出: ${out.path}`, true);
  }

  return {
    exportPreflightReport,
    runTemplateAcceptance,
    exportTemplateAcceptanceReport,
  };
}

export { createWorkflowPreflightActionsUi };
