import { createWorkflowFlowIoUi } from "./flow-io-ui.js";
import { createWorkflowPaletteUi } from "./palette-ui.js";
import { createWorkflowCanvasViewUi } from "./canvas-view-ui.js";
import { createWorkflowPreflightUi } from "./preflight-ui.js";
import { createWorkflowPreflightControllerUi } from "./preflight-controller-ui.js";
import { createWorkflowPreflightActionsUi } from "./preflight-actions-ui.js";
import { createWorkflowAppPublishUi } from "./app-publish-ui.js";
import { createWorkflowRunControllerUi } from "./run-controller-ui.js";
import {
  buildAppPublishUiDeps,
  buildCanvasViewUiDeps,
  buildFlowIoDeps,
  buildPaletteUiDeps,
  buildPreflightActionsDeps,
  buildPreflightControllerDeps,
  buildRunControllerUiDeps,
} from "./app-late-services-support.js";

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
