function createWorkflowGraphShellUi(els, deps = {}) {
  const {
    store,
    setStatus = () => {},
    renderAll = () => {},
    setSelectedEdge = () => {},
    getResetWorkflowName = () => "自由编排流程",
    renderMigrationReport = () => {},
  } = deps;

  function applyRestoredWorkflowGraph(graph) {
    store.importGraph(graph || {});
    setSelectedEdge(null);
    renderAll();
  }

  function resetWorkflow() {
    store.reset();
    setSelectedEdge(null);
    if (els.workflowName) els.workflowName.value = getResetWorkflowName();
    renderAll();
    renderMigrationReport({ migrated: false });
    setStatus("已重置默认流程", true);
  }

  function clearWorkflow() {
    store.clear();
    setSelectedEdge(null);
    renderAll();
    renderMigrationReport({ migrated: false });
    setStatus("画布已清空", true);
  }

  return {
    applyRestoredWorkflowGraph,
    resetWorkflow,
    clearWorkflow,
  };
}

export { createWorkflowGraphShellUi };
