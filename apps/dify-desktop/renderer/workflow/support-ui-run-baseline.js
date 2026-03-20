function createWorkflowSupportRunBaseline(els, deps = {}) {
  const { setStatus = () => {} } = deps;

  async function saveCurrentRunAsBaseline() {
    const runA = String(els.compareRunA?.value || "").trim();
    if (!runA) {
      setStatus("请先在“运行对比”里选择 Run A 作为基线", false);
      return;
    }
    const out = await window.aiwfDesktop.saveRunBaseline({
      run_id: runA,
      name: `baseline_${runA.slice(0, 8)}`,
    });
    if (out?.ok) setStatus(`基线已保存: ${out?.item?.baseline_id}`, true);
    else setStatus(`保存基线失败: ${out?.error || "unknown"}`, false);
  }

  async function compareWithLatestBaseline() {
    const runB = String(els.compareRunB?.value || "").trim();
    if (!runB) {
      setStatus("请先在“运行对比”里选择 Run B", false);
      return;
    }
    const baselines = await window.aiwfDesktop.listRunBaselines();
    const first = Array.isArray(baselines?.items) ? baselines.items[0] : null;
    if (!first) {
      setStatus("未找到基线，请先保存基线", false);
      return;
    }
    const out = await window.aiwfDesktop.compareRunWithBaseline({
      run_id: runB,
      baseline_id: first.baseline_id,
    });
    if (!out?.ok) {
      setStatus(`基线对比失败: ${out?.error || "unknown"}`, false);
      return;
    }
    const regression = out.regression || {};
    setStatus(
      `基线对比完成: changed=${regression.changed_nodes || 0}, status_flip=${regression.status_flip_nodes || 0}, perf_hot=${regression.perf_hot_nodes || 0}`,
      true
    );
  }

  async function loadLineageForRunA() {
    const runId = String(els.compareRunA?.value || "").trim();
    if (!runId) {
      setStatus("请先在“运行对比”里选择 Run A", false);
      return;
    }
    const out = await window.aiwfDesktop.getWorkflowLineage({ run_id: runId });
    if (!out?.ok) {
      setStatus(`加载血缘失败: ${out?.error || "unknown"}`, false);
      return;
    }
    const lineage = out.lineage || {};
    const nodes = Array.isArray(lineage?.nodes) ? lineage.nodes.length : Number(lineage?.node_count || 0);
    const edges = Array.isArray(lineage?.edges) ? lineage.edges.length : Number(lineage?.edge_count || 0);
    if (els.log) els.log.textContent = JSON.stringify({ run_id: runId, lineage }, null, 2);
    setStatus(`血缘已加载: nodes=${nodes}, edges=${edges}`, true);
  }

  return {
    compareWithLatestBaseline,
    loadLineageForRunA,
    saveCurrentRunAsBaseline,
  };
}

export { createWorkflowSupportRunBaseline };
