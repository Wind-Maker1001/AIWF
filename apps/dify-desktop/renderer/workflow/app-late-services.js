import { createWorkflowFlowIoUi } from "./flow-io-ui.js";
import { createWorkflowPaletteUi } from "./palette-ui.js";
import { createWorkflowCanvasViewUi } from "./canvas-view-ui.js";
import { createWorkflowPreflightUi } from "./preflight-ui.js";
import { createWorkflowPreflightControllerUi } from "./preflight-controller-ui.js";
import { createWorkflowPreflightActionsUi } from "./preflight-actions-ui.js";
import { createWorkflowAppPublishUi } from "./app-publish-ui.js";
import { createWorkflowRunControllerUi } from "./run-controller-ui.js";

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

function buildPreflightControllerDeps(ctx = {}) {
  const {
    graphPayload = () => ({}),
    store,
    applyLoadedWorkflowGraph = () => {},
    computePreflightRisk = () => ({}),
    renderPreflightReport = () => {},
    setLastPreflightReport = () => {},
  } = ctx;

  return {
    graphPayload,
    exportGraph: () => store.exportGraph(),
    applyGraph: (graph) => applyLoadedWorkflowGraph(graph),
    computePreflightRisk,
    renderPreflightReport,
    setLastPreflightReport,
  };
}

function buildPreflightActionsDeps(ctx = {}) {
  const {
    setStatus = () => {},
    runWorkflowPreflight = async () => ({ ok: true }),
    allTemplates = () => [],
    currentTemplateGovernance = () => ({}),
    autoFixGraphStructure = () => ({}),
    renderAutoFixDiff = () => {},
    getLastPreflightReport = () => null,
    getLastTemplateAcceptanceReport = () => null,
    setLastAutoFixSummary = () => {},
    setLastTemplateAcceptanceReport = () => {},
  } = ctx;

  return {
    setStatus,
    runWorkflowPreflight,
    allTemplates,
    currentTemplateGovernance,
    autoFixGraphStructure,
    renderAutoFixDiff,
    getLastPreflightReport,
    getLastTemplateAcceptanceReport,
    setLastAutoFixSummary,
    setLastTemplateAcceptanceReport,
  };
}

function buildAppPublishUiDeps(ctx = {}) {
  const {
    setStatus = () => {},
    graphPayload = () => ({}),
    runWorkflowPreflight = async () => ({ ok: true }),
    collectAppSchemaFromForm = () => ({}),
    normalizeAppSchemaObject = (obj) => obj,
    currentTemplateGovernance = () => ({}),
    parseRunParamsLoose = () => ({}),
    getLastPreflightReport = () => null,
    getLastTemplateAcceptanceReport = () => null,
    renderAppRows = () => {},
    appSchemaRowsFromObject = () => [],
    renderAppSchemaForm = () => {},
    syncAppSchemaJsonFromForm = () => {},
    syncAppSchemaFormFromJson = () => {},
    syncRunParamsJsonFromForm = () => {},
    syncRunParamsFormFromJson = () => {},
  } = ctx;

  return {
    setStatus,
    graphPayload,
    runWorkflowPreflight,
    collectAppSchemaFromForm,
    normalizeAppSchemaObject,
    currentTemplateGovernance,
    parseRunParamsLoose,
    getLastPreflightReport,
    getLastTemplateAcceptanceReport,
    renderAppRows,
    appSchemaRowsFromObject,
    renderAppSchemaForm,
    syncAppSchemaJsonFromForm,
    syncAppSchemaFormFromJson,
    syncRunParamsJsonFromForm,
    syncRunParamsFormFromJson,
  };
}

function buildRunControllerUiDeps(ctx = {}) {
  const {
    setStatus = () => {},
    runWorkflowPreflight = async () => ({ ok: true }),
    runPayload = () => ({}),
    renderNodeRuns = () => {},
    refreshDiagnostics = async () => {},
    refreshRunHistory = async () => {},
    refreshReviewQueue = async () => {},
    refreshQueue = async () => {},
  } = ctx;

  return {
    setStatus,
    runWorkflowPreflight,
    runPayload,
    renderNodeRuns,
    refreshDiagnostics,
    refreshRunHistory,
    refreshReviewQueue,
    refreshQueue,
  };
}

function createWorkflowLateServices(ctx = {}) {
  const {
    els,
    store,
    canvas,
    setLastAutoFixSummary = () => {},
    setLastTemplateAcceptanceReport = () => {},
    setLastPreflightReport = () => {},
    allTemplates = () => [],
    currentTemplateGovernance = () => ({}),
    getLastPreflightReport = () => null,
    getLastTemplateAcceptanceReport = () => null,
    collectAppSchemaFromForm = () => ({}),
    normalizeAppSchemaObject = (obj) => obj,
    parseRunParamsLoose = () => ({}),
    appSchemaRowsFromObject = () => [],
    renderAppSchemaForm = () => {},
    syncAppSchemaJsonFromForm = () => {},
    syncAppSchemaFormFromJson = () => {},
    syncRunParamsJsonFromForm = () => {},
    syncRunParamsFormFromJson = () => {},
    runPayload = () => ({}),
    renderNodeRuns = () => {},
    renderAppRows = () => {},
  } = ctx;

  const { exportJson, saveFlow, loadFlow } = createWorkflowFlowIoUi(els, buildFlowIoDeps(ctx));

  const {
    handleAddNode,
    renderPalette,
    renderNodeTypePolicyHint,
    handleCanvasDragOver,
    handleCanvasDrop,
  } = createWorkflowPaletteUi(els, buildPaletteUiDeps({ ...ctx, store }));

  const {
    syncCanvasPanels,
    setZoom,
    fitCanvasToView,
    applyArrange,
    focusNodeInCanvas,
  } = createWorkflowCanvasViewUi(els, buildCanvasViewUiDeps({ ...ctx, canvas }));

  const {
    computePreflightRisk,
    renderPreflightReport,
    renderAutoFixDiff,
  } = createWorkflowPreflightUi(els, { focusNodeInCanvas });

  const {
    autoFixGraphStructure,
    runWorkflowPreflight,
  } = createWorkflowPreflightControllerUi(els, buildPreflightControllerDeps({
    ...ctx,
    store,
    computePreflightRisk,
    renderPreflightReport,
    setLastPreflightReport: (report) => setLastPreflightReport(report),
  }));

  const {
    exportPreflightReport,
    runTemplateAcceptance,
    exportTemplateAcceptanceReport,
  } = createWorkflowPreflightActionsUi(els, buildPreflightActionsDeps({
    ...ctx,
    runWorkflowPreflight,
    allTemplates,
    currentTemplateGovernance,
    autoFixGraphStructure,
    renderAutoFixDiff,
    getLastPreflightReport,
    getLastTemplateAcceptanceReport,
    setLastAutoFixSummary,
    setLastTemplateAcceptanceReport,
  }));

  const {
    handleAppSchemaAdd,
    handleAppSchemaSyncJson,
    handleAppSchemaFromJson,
    handleAppRunSyncJson,
    handleAppRunFromJson,
    publishApp,
    refreshApps,
  } = createWorkflowAppPublishUi(els, buildAppPublishUiDeps({
    ...ctx,
    runWorkflowPreflight,
    collectAppSchemaFromForm,
    normalizeAppSchemaObject,
    currentTemplateGovernance,
    parseRunParamsLoose,
    getLastPreflightReport,
    getLastTemplateAcceptanceReport,
    renderAppRows,
    appSchemaRowsFromObject,
    renderAppSchemaForm,
    syncAppSchemaJsonFromForm,
    syncAppSchemaFormFromJson,
    syncRunParamsJsonFromForm,
    syncRunParamsFormFromJson,
  }));

  const {
    runWorkflow,
    enqueueWorkflowRun,
  } = createWorkflowRunControllerUi(els, buildRunControllerUiDeps({
    ...ctx,
    runWorkflowPreflight,
    runPayload,
    renderNodeRuns,
  }));

  return {
    exportJson,
    saveFlow,
    loadFlow,
    handleAddNode,
    renderPalette,
    renderNodeTypePolicyHint,
    handleCanvasDragOver,
    handleCanvasDrop,
    syncCanvasPanels,
    setZoom,
    fitCanvasToView,
    applyArrange,
    focusNodeInCanvas,
    computePreflightRisk,
    renderPreflightReport,
    renderAutoFixDiff,
    autoFixGraphStructure,
    runWorkflowPreflight,
    exportPreflightReport,
    runTemplateAcceptance,
    exportTemplateAcceptanceReport,
    handleAppSchemaAdd,
    handleAppSchemaSyncJson,
    handleAppSchemaFromJson,
    handleAppRunSyncJson,
    handleAppRunFromJson,
    publishApp,
    refreshApps,
    runWorkflow,
    enqueueWorkflowRun,
  };
}

export { createWorkflowLateServices };
export {
  buildAppPublishUiDeps,
  buildCanvasViewUiDeps,
  buildFlowIoDeps,
  buildPaletteUiDeps,
  buildPreflightActionsDeps,
  buildPreflightControllerDeps,
  buildRunControllerUiDeps,
};
