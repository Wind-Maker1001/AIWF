function createWorkflowFlowIoUi(els, deps = {}) {
  const {
    setStatus = () => {},
    graphPayload = () => ({}),
    refreshVersions = async () => {},
    migrateLoadedWorkflowGraph = (graph) => ({ migrated: false, graph, notes: [] }),
    applyLoadedWorkflowGraph = () => {},
    getLoadedWorkflowName = () => "",
    renderMigrationReport = () => {},
  } = deps;

  function exportJson() {
    const json = JSON.stringify(graphPayload(), null, 2);
    if (els.log) els.log.textContent = json;
    setStatus("已导出流程 JSON 到右侧日志区", true);
  }

  async function saveFlow() {
    try {
      const graph = graphPayload();
      const name = String(els.workflowName?.value || "").trim() || "workflow";
      const out = await window.aiwfDesktop.saveWorkflow(graph, name);
      if (out?.ok) {
        setStatus(`流程已保存: ${out.path}`, true);
        await refreshVersions();
      } else if (!out?.canceled) {
        setStatus(`保存失败: ${out?.error || "unknown"}`, false);
      }
    } catch (e) {
      setStatus(`保存失败: ${e}`, false);
    }
  }

  async function loadFlow() {
    try {
      const out = await window.aiwfDesktop.loadWorkflow();
      if (!out?.ok) {
        if (!out?.canceled) setStatus(`加载失败: ${out?.error || "unknown"}`, false);
        return;
      }
      const migrated = migrateLoadedWorkflowGraph(out.graph || {});
      applyLoadedWorkflowGraph(migrated.graph || {});
      if (els.workflowName) els.workflowName.value = getLoadedWorkflowName() || "自定义流程";
      renderMigrationReport(migrated);
      if (migrated.migrated) {
        setStatus(`流程已加载并迁移: ${out.path} (${migrated.notes.join(", ")})`, true);
      } else {
        setStatus(`流程已加载: ${out.path}`, true);
      }
    } catch (e) {
      setStatus(`加载失败: ${e}`, false);
    }
  }

  return {
    exportJson,
    saveFlow,
    loadFlow,
  };
}

export { createWorkflowFlowIoUi };
