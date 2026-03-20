function applyGraphShellResetState(ctx = {}) {
  const {
    store,
    setSelectedEdge = () => {},
    renderAll = () => {},
    renderMigrationReport = () => {},
    setStatus = () => {},
    workflowName,
    getResetWorkflowName = () => "自由编排流程",
  } = ctx;

  store.reset();
  setSelectedEdge(null);
  if (workflowName) workflowName.value = getResetWorkflowName();
  renderAll();
  renderMigrationReport({ migrated: false });
  setStatus("已重置默认流程", true);
}

function applyGraphShellClearState(ctx = {}) {
  const {
    store,
    setSelectedEdge = () => {},
    renderAll = () => {},
    renderMigrationReport = () => {},
    setStatus = () => {},
  } = ctx;

  store.clear();
  setSelectedEdge(null);
  renderAll();
  renderMigrationReport({ migrated: false });
  setStatus("画布已清空", true);
}

export {
  applyGraphShellClearState,
  applyGraphShellResetState,
};
