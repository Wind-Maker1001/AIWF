import {
  applyGraphShellClearState,
  applyGraphShellResetState,
} from "./graph-shell-support.js";

function createWorkflowGraphShellUi(els, deps = {}) {
  const {
    store,
    setSelectedEdge = () => {},
    renderAll = () => {},
    setStatus = () => {},
    getResetWorkflowName = () => "鑷敱缂栨帓娴佺▼",
    renderMigrationReport = () => {},
  } = deps;

  function applyRestoredWorkflowGraph(graph) {
    const imported = store.importGraph(graph || {});
    setSelectedEdge(null);
    renderAll();
    return imported;
  }

  function resetWorkflow() {
    applyGraphShellResetState({
      store,
      setSelectedEdge,
      renderAll,
      renderMigrationReport,
      setStatus,
      workflowName: els.workflowName,
      getResetWorkflowName,
    });
  }

  function clearWorkflow() {
    applyGraphShellClearState({
      store,
      setSelectedEdge,
      renderAll,
      renderMigrationReport,
      setStatus,
    });
  }

  return {
    applyRestoredWorkflowGraph,
    resetWorkflow,
    clearWorkflow,
  };
}

export { createWorkflowGraphShellUi };
