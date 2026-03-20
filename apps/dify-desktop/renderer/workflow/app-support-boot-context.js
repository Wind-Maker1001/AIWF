function buildSelectedEdgeAccess(selectedEdgeRef) {
  return {
    get: () => selectedEdgeRef.get(),
    set: (edge) => { selectedEdgeRef.set(edge); },
  };
}

function buildBootWorkflowContext(ctx = {}) {
  const {
    els,
    setStatus = () => {},
    compareState,
    cfgViewModeRef,
    autoFixSummaryRef,
    selectedEdgeRef,
    getRenderMigrationReport = () => () => {},
    refreshOfflineBoundaryHint = () => {},
    renderAll = () => {},
    store,
    canvas,
    defaultNodeConfig,
    syncCanvasPanels = () => {},
    bootServices = {},
  } = ctx;

  return {
    els,
    setStatus,
    ...bootServices,
    setLastAutoFixSummary: (out) => { autoFixSummaryRef.set(out); },
    getLastCompareResult: () => compareState.get(),
    refreshOfflineBoundaryHint,
    store,
    canvas,
    renderAll,
    selectedEdgeRef: buildSelectedEdgeAccess(selectedEdgeRef),
    defaultNodeConfig,
    getCfgViewMode: () => cfgViewModeRef.get(),
    syncCanvasPanels,
    renderMigrationReport: getRenderMigrationReport(),
  };
}

export {
  buildBootWorkflowContext,
  buildSelectedEdgeAccess,
};
