function buildFlowIoDeps(ctx = {}) {
  const {
    setStatus = () => {},
    graphPayload = () => ({}),
    refreshVersions = async () => {},
    migrateLoadedWorkflowGraph = () => ({}),
    applyLoadedWorkflowGraph = () => {},
    getLoadedWorkflowName = () => "",
    renderMigrationReport = () => {},
  } = ctx;

  return {
    setStatus,
    graphPayload,
    refreshVersions: () => refreshVersions(),
    migrateLoadedWorkflowGraph,
    applyLoadedWorkflowGraph,
    getLoadedWorkflowName,
    renderMigrationReport,
  };
}

function buildPaletteUiDeps(ctx = {}) {
  const {
    setStatus = () => {},
    nodeCatalog = {},
    defaultNodeConfigFn = () => ({}),
    store,
    selectNodeIds = () => {},
    renderAll = () => {},
    computeDropPosition = () => ({ x: 0, y: 0 }),
  } = ctx;

  return {
    setStatus,
    nodeCatalog,
    defaultNodeConfigFn,
    createNode: (type, x, y, config) => store.addNode(type, x, y, config),
    selectNodeIds: (ids) => selectNodeIds(ids),
    renderAll,
    computeDropPosition,
  };
}

function buildCanvasViewUiDeps(ctx = {}) {
  const {
    canvas,
    setStatus = () => {},
    renderNodeConfigEditor = () => {},
    renderEdgeConfigEditor = () => {},
    refreshOfflineBoundaryHint = () => {},
    getNode = () => null,
    selectNodeIds = () => {},
    renderAll = () => {},
  } = ctx;

  return {
    canvas,
    setStatus,
    renderNodeConfigEditor: () => renderNodeConfigEditor(),
    renderEdgeConfigEditor: () => renderEdgeConfigEditor(),
    refreshOfflineBoundaryHint,
    getNode: (id) => getNode(id),
    selectNodeIds: (ids) => selectNodeIds(ids),
    renderAll,
  };
}

export {
  buildCanvasViewUiDeps,
  buildFlowIoDeps,
  buildPaletteUiDeps,
};
