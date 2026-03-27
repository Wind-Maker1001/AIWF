import { createWorkflowCoreServices } from "./app-core-services.js";
import { createWorkflowPanelsUi } from "./panels-ui.js";
import { createWorkflowPanelServices } from "./app-panel-services.js";
import { createWorkflowLateServices } from "./app-late-services.js";
// Keep the service combiner local so the assembly root is visible here.
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
    nodeCatalog: staticConfig.nodeCatalog,
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

function buildPanelsUiDeps(ctx = {}) {
  const {
    els,
    setStatus = () => {},
    getPanelServices = () => null,
    coreServices = {},
    graphShellApi,
    refreshDiagnostics = async () => {},
  } = ctx;

  return {
    setStatus,
    refreshRunHistory: () => getPanelServices()?.refreshRunHistory?.(),
    refreshReviewQueue: () => getPanelServices()?.refreshReviewQueue?.(),
    showReviewQueue: async () => {
      await getPanelServices()?.refreshReviewQueue?.();
      try { els.btnReviewsRefresh?.scrollIntoView?.({ block: "center" }); } catch {}
    },
    showQualityGate: async (runId) => {
      if (els.qualityGateRunIdFilter) els.qualityGateRunIdFilter.value = String(runId || "");
      if (els.qualityGateStatusFilter) els.qualityGateStatusFilter.value = "blocked";
      await getPanelServices()?.refreshQualityGateReports?.();
      try { els.btnQualityGateRefresh?.scrollIntoView?.({ block: "center" }); } catch {}
    },
    refreshReviewHistory: () => coreServices.refreshReviewHistory(),
    refreshQueue: () => getPanelServices()?.refreshQueue?.(),
    refreshDiagnostics,
    refreshSandboxRuleVersions: () => getPanelServices()?.refreshSandboxRuleVersions?.(),
    refreshSandboxAlerts: () => getPanelServices()?.refreshSandboxAlerts?.(),
    applySandboxRulesToUi: coreServices.applySandboxRulesToUi,
    applyRestoredGraph: (graph) => graphShellApi.applyRestoredWorkflowGraph(graph),
    renderSandboxHealth: coreServices.renderSandboxHealth,
    normalizeAppSchemaObject: coreServices.normalizeAppSchemaObject,
    renderAppSchemaForm: coreServices.renderAppSchemaForm,
    appSchemaRowsFromObject: coreServices.appSchemaRowsFromObject,
    renderRunParamsFormBySchema: coreServices.renderRunParamsFormBySchema,
    collectRunParamsForm: coreServices.collectRunParamsForm,
    runPayload: coreServices.runPayload,
  };
}

function buildPanelServicesDeps(ctx = {}) {
  const {
    els,
    store,
    staticConfig = {},
    setStatus = () => {},
    panelsUi = {},
    coreServices = {},
  } = ctx;

  return {
    els,
    store,
    qualityGatePrefsKey: staticConfig.qualityGatePrefsKey,
    setStatus,
    qualityGateFilterPayload: panelsUi.qualityGateFilterPayload,
    qualityGatePrefsPayload: panelsUi.qualityGatePrefsPayload,
    renderQualityGateRows: panelsUi.renderQualityGateRows,
    sandboxThresholdsPayload: coreServices.sandboxThresholdsPayload,
    sandboxDedupWindowSec: coreServices.sandboxDedupWindowSec,
    sandboxRulesPayloadFromUi: coreServices.sandboxRulesPayloadFromUi,
    applySandboxRulesToUi: coreServices.applySandboxRulesToUi,
    applySandboxPresetToUi: coreServices.applySandboxPresetToUi,
    currentSandboxPresetPayload: coreServices.currentSandboxPresetPayload,
    applySandboxPresetPayload: coreServices.applySandboxPresetPayload,
    renderSandboxRows: panelsUi.renderSandboxRows,
    renderSandboxRuleVersionRows: panelsUi.renderSandboxRuleVersionRows,
    renderSandboxAutoFixRows: panelsUi.renderSandboxAutoFixRows,
    renderTimelineRows: panelsUi.renderTimelineRows,
    renderFailureRows: panelsUi.renderFailureRows,
    renderAuditRows: panelsUi.renderAuditRows,
    renderRunHistoryRows: panelsUi.renderRunHistoryRows,
    renderQueueRows: panelsUi.renderQueueRows,
    renderQueueControl: panelsUi.renderQueueControl,
    renderReviewRows: panelsUi.renderReviewRows,
    renderVersionRows: panelsUi.renderVersionRows,
    renderVersionCompare: panelsUi.renderVersionCompare,
    renderCacheStats: panelsUi.renderCacheStats,
  };
}

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

function createWorkflowAppServices(ctx = {}) {
  const {
    els,
    store,
    canvas,
    setStatus = () => {},
    renderAll = () => {},
    refreshOfflineBoundaryHint = () => {},
    graphShellApi,
    state = {},
    staticConfig = {},
    defaultNodeConfig,
    migrateLoadedWorkflowGraph,
    renderNodeRuns = () => {},
    refreshDiagnostics = async () => {},
  } = ctx;

  let panelServices = null;
  let renderMigrationReport = () => {};
  const coreServicesRef = { current: null };

  const coreServices = createWorkflowCoreServices(buildCoreServicesContext({
    els,
    store,
    setStatus,
    state,
    renderMigrationReport: (...args) => renderMigrationReport(...args),
    staticConfig,
    renderAll,
    refreshOfflineBoundaryHint,
    coreServicesRef,
    canvas,
  }));
  coreServicesRef.current = coreServices;

  const panelsUi = createWorkflowPanelsUi(els, buildPanelsUiDeps({
    els,
    setStatus,
    getPanelServices: () => panelServices,
    coreServices,
    graphShellApi,
    refreshDiagnostics,
  }));

  renderMigrationReport = panelsUi.renderMigrationReport;

  panelServices = createWorkflowPanelServices(buildPanelServicesDeps({
    els,
    store,
    staticConfig,
    setStatus,
    panelsUi,
    coreServices,
  }));

  const lateServices = createWorkflowLateServices(buildLateServicesDeps({
    els,
    setStatus,
    coreServices,
    panelServices,
    migrateLoadedWorkflowGraph,
    graphShellApi,
    store,
    canvas,
    staticConfig,
    defaultNodeConfig,
    renderAll,
    refreshOfflineBoundaryHint,
    state,
    renderNodeRuns,
    refreshDiagnostics,
    panelsUi,
  }));

  return combineWorkflowAppServices(coreServices, panelsUi, panelServices, lateServices);
}

export {
  buildLateServicesDeps,
  buildCoreServicesContext,
  buildPanelServicesDeps,
  buildPanelsUiDeps,
  combineWorkflowAppServices,
  computeCanvasDropPosition,
  createWorkflowAppServices,
};
