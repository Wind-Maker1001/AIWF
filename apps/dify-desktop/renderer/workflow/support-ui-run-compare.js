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
      setStatus("璇峰～鍐?Run A / Run B", false);
      return;
    }
    const out = await window.aiwfDesktop.compareWorkflowRuns({ run_a: runA, run_b: runB });
    setLastCompareResult(out);
    els.log.textContent = JSON.stringify(out, null, 2);
    renderer.renderCompareResult(out);
    setStatus(out?.ok ? "杩愯瀵规瘮瀹屾垚" : `杩愯瀵规瘮澶辫触: ${out?.error || "unknown"}`, !!out?.ok);
  }

  async function exportCompareReport() {
    const runA = String(els.compareRunA?.value || "").trim();
    const runB = String(els.compareRunB?.value || "").trim();
    if (!runA || !runB) {
      setStatus("璇峰厛濉啓 Run A / Run B", false);
      return;
    }
    try {
      const format = String(els.compareReportFormat?.value || "md").trim() || "md";
      const out = await window.aiwfDesktop.exportCompareReport({ run_a: runA, run_b: runB, format });
      if (!out?.ok) {
        if (!out?.canceled) setStatus(`瀵煎嚭瀵规瘮鎶ュ憡澶辫触: ${out?.error || "unknown"}`, false);
        return;
      }
      setStatus(`瀵规瘮鎶ュ憡宸插鍑? ${out.path}`, true);
    } catch (error) {
      setStatus(`瀵煎嚭瀵规瘮鎶ュ憡澶辫触: ${error}`, false);
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
