function buildCoreServicesContext(ctx = {}) {
  const {
    els,
    store,
    setStatus = () => {},
    state = {},
    renderMigrationReport = () => {},
    staticConfig = {},
    renderAll = () => {},
    refreshOfflineBoundaryHint = () => {},
    coreServicesRef = { current: null },
    canvas,
  } = ctx;

  return {
    els,
    store,
    setStatus,
    getLastCompareResult: () => state.getLastCompareResult?.() ?? null,
    setLastCompareResult: (out) => state.setLastCompareResult?.(out),
    renderMigrationReport: (...args) => renderMigrationReport(...args),
    syncRunParamsFormFromJson: (...args) => coreServicesRef.current?.syncRunParamsFormFromJson?.(...args),
    templateStorageKey: staticConfig.templateStorageKey,
    builtinTemplates: staticConfig.builtinTemplates,
    renderAll,
    refreshOfflineBoundaryHint,
    getSelectedEdge: () => state.getSelectedEdge?.() ?? null,
    setSelectedEdge: (edge) => state.setSelectedEdge?.(edge),
    getCfgViewMode: () => state.getCfgViewMode?.() ?? "form",
    setCfgViewMode: (mode) => state.setCfgViewMode?.(mode),
    nodeFormSchemas: staticConfig.nodeFormSchemas,
    edgeHintsByNodeType: staticConfig.edgeHintsByNodeType,
    sandboxDedupWindowSec: () => coreServicesRef.current?.sandboxDedupWindowSec?.() ?? 0,
    canvas,
  };
}

function combineWorkflowAppServices(coreServices, panelsUi, panelServices, lateServices) {
  return {
    ...coreServices,
    ...panelsUi,
    ...panelServices,
    ...lateServices,
    renderMigrationReportImpl: panelsUi.renderMigrationReport,
  };
}

export {
  buildCoreServicesContext,
  combineWorkflowAppServices,
};
