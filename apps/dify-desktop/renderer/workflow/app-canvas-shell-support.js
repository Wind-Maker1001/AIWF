function buildWorkflowCanvasDeps(ctx = {}) {
  const {
    store,
    nodeCatalog,
    els,
    setStatus = () => {},
    renderAll = () => {},
    selectedEdgeRef = { get: () => null, set: () => {} },
    getRenderNodeConfigEditor = () => () => {},
    getRenderEdgeConfigEditor = () => () => {},
  } = ctx;

  return {
    store,
    nodeCatalog,
    canvasWrap: els.canvasWrap,
    canvasSurface: els.canvasSurface,
    nodesLayer: els.nodesLayer,
    guideLayer: els.guideLayer,
    minimapCanvas: els.minimap,
    edgesSvg: els.edges,
    onChange: renderAll,
    onWarn: (msg) => setStatus(msg, false),
    onSelectionChange: () => getRenderNodeConfigEditor()(),
    onEdgeSelect: (edge) => {
      selectedEdgeRef.set(edge && edge.from && edge.to ? { ...edge } : null);
      getRenderEdgeConfigEditor()();
    },
  };
}

function buildGraphShellDeps(ctx = {}) {
  const {
    els,
    store,
    setStatus = () => {},
    renderAll = () => {},
    selectedEdgeRef = { get: () => null, set: () => {} },
    getResetWorkflowName = () => "自由编排流程",
    renderMigrationReport = () => {},
  } = ctx;

  return {
    els,
    store,
    setStatus,
    renderAll,
    setSelectedEdge: (edge) => { selectedEdgeRef.set(edge); },
    getResetWorkflowName,
    renderMigrationReport,
  };
}

export {
  buildGraphShellDeps,
  buildWorkflowCanvasDeps,
};
