function computeCanvasDropPosition(ctx = {}) {
  const {
    canvas,
    evt,
    snapEnabled = false,
    grid = 24,
  } = ctx;

  const world = canvas.clientToWorld(evt.clientX, evt.clientY);
  const rawX = world.x - 105;
  const rawY = world.y - 43;
  return {
    x: snapEnabled ? Math.round(rawX / grid) * grid : rawX,
    y: snapEnabled ? Math.round(rawY / grid) * grid : rawY,
  };
}

function buildLateServicesDeps(ctx = {}) {
  const {
    els,
    setStatus = () => {},
    coreServices = {},
    panelServices = {},
    migrateLoadedWorkflowGraph,
    graphShellApi,
    store,
    canvas,
    staticConfig = {},
    defaultNodeConfig,
    renderAll = () => {},
    refreshOfflineBoundaryHint = () => {},
    state = {},
    renderNodeRuns = () => {},
    refreshDiagnostics = async () => {},
    panelsUi = {},
  } = ctx;

  return {
    els,
    setStatus,
    graphPayload: coreServices.graphPayload,
    refreshVersions: panelServices.refreshVersions,
    migrateLoadedWorkflowGraph,
    applyLoadedWorkflowGraph: (graph) => graphShellApi.applyRestoredWorkflowGraph(graph),
    getLoadedWorkflowName: () => String(store.state.graph?.name || ""),
    renderMigrationReport: panelsUi.renderMigrationReport,
    store,
    canvas,
    nodeCatalog: staticConfig.nodeCatalog,
    defaultNodeConfigFn: defaultNodeConfig,
    renderAll,
    renderNodeConfigEditor: () => coreServices.renderNodeConfigEditor(),
    renderEdgeConfigEditor: () => coreServices.renderEdgeConfigEditor(),
    refreshOfflineBoundaryHint,
    getNode: (id) => store.getNode(id),
    selectNodeIds: (ids) => canvas.setSelectedIds(ids),
    computeDropPosition: (evt) => computeCanvasDropPosition({
      canvas,
      evt,
      snapEnabled: !!els.snapGrid?.checked,
    }),
    allTemplates: coreServices.allTemplates,
    currentTemplateGovernance: coreServices.currentTemplateGovernance,
    parseRunParamsLoose: coreServices.parseRunParamsLoose,
    collectAppSchemaFromForm: coreServices.collectAppSchemaFromForm,
    normalizeAppSchemaObject: coreServices.normalizeAppSchemaObject,
    getLastPreflightReport: () => state.getLastPreflightReport?.() ?? null,
    getLastTemplateAcceptanceReport: () => state.getLastTemplateAcceptanceReport?.() ?? null,
    setLastAutoFixSummary: (summary) => state.setLastAutoFixSummary?.(summary),
    setLastTemplateAcceptanceReport: (report) => state.setLastTemplateAcceptanceReport?.(report),
    setLastPreflightReport: (report) => state.setLastPreflightReport?.(report),
    appSchemaRowsFromObject: coreServices.appSchemaRowsFromObject,
    renderAppSchemaForm: coreServices.renderAppSchemaForm,
    syncAppSchemaJsonFromForm: coreServices.syncAppSchemaJsonFromForm,
    syncAppSchemaFormFromJson: coreServices.syncAppSchemaFormFromJson,
    syncRunParamsJsonFromForm: coreServices.syncRunParamsJsonFromForm,
    syncRunParamsFormFromJson: coreServices.syncRunParamsFormFromJson,
    runPayload: coreServices.runPayload,
    renderNodeRuns,
    refreshDiagnostics,
    refreshRunHistory: panelServices.refreshRunHistory,
    refreshReviewQueue: panelServices.refreshReviewQueue,
    refreshQueue: panelServices.refreshQueue,
    renderAppRows: panelsUi.renderAppRows,
  };
}

export {
  buildLateServicesDeps,
  computeCanvasDropPosition,
};
