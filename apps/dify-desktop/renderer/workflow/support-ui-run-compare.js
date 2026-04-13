import { formatAiwfError } from "./workflow-contract.js";
import { createWorkflowSupportRunBaseline } from "./support-ui-run-baseline.js";
import { createWorkflowSupportRunCompareRenderer } from "./support-ui-run-compare-renderer.js";

function createWorkflowSupportRunCompare(els, deps = {}) {
  const {
    setStatus = () => {},
    setLastCompareResult = () => {},
  } = deps;

  const renderer = createWorkflowSupportRunCompareRenderer(els, deps);
  const baseline = createWorkflowSupportRunBaseline(els, deps);

  async function compareRuns() {
    const runA = String(els.compareRunA?.value || "").trim();
    const runB = String(els.compareRunB?.value || "").trim();
    if (!runA || !runB) {
      setStatus("请先填写 Run A / Run B", false);
      return;
    }
    const out = await window.aiwfDesktop.compareWorkflowRuns({ run_a: runA, run_b: runB });
    setLastCompareResult(out);
    els.log.textContent = JSON.stringify(out, null, 2);
    renderer.renderCompareResult(out);
    setStatus(out?.ok ? "运行对比完成" : `运行对比失败: ${formatAiwfError(out)}`, !!out?.ok);
  }

  async function exportCompareReport() {
    const runA = String(els.compareRunA?.value || "").trim();
    const runB = String(els.compareRunB?.value || "").trim();
    if (!runA || !runB) {
      setStatus("请先填写 Run A / Run B", false);
      return;
    }
    try {
      const format = String(els.compareReportFormat?.value || "md").trim() || "md";
      const out = await window.aiwfDesktop.exportCompareReport({ run_a: runA, run_b: runB, format });
      if (!out?.ok) {
        if (!out?.canceled) setStatus(`导出运行对比报告失败: ${formatAiwfError(out)}`, false);
        return;
      }
      setStatus(`运行对比报告已导出: ${out.path}`, true);
    } catch (error) {
      setStatus(`导出运行对比报告失败: ${error}`, false);
    }
  }

  return {
    compareRuns,
    compareWithLatestBaseline: baseline.compareWithLatestBaseline,
    exportCompareReport,
    loadLineageForRunA: baseline.loadLineageForRunA,
    renderCompareResult: renderer.renderCompareResult,
    saveCurrentRunAsBaseline: baseline.saveCurrentRunAsBaseline,
  };
}

export { createWorkflowSupportRunCompare };
