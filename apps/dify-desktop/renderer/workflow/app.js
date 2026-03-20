import { defaultNodeConfig, NODE_CATALOG } from "./defaults.js";
import { createWorkflowStore } from "./store.js";
import { createWorkflowCanvasShell } from "./app-canvas-shell.js";
import { getWorkflowElements } from "./elements.js";
import { migrateLoadedWorkflowGraph } from "./template-utils.js";
import {
  buildAppServicesContext,
  buildBootWorkflowContext,
  buildWorkflowBootServices,
  buildCanvasShellContext,
  buildUiServicesContext,
  buildWorkflowStaticConfig,
  ensureWorkflowDesktopBridge,
} from "./app-support.js";
// Static workflow metadata remains sourced from `static-config.js`.

import { bootWorkflowApp } from "./app-boot.js";
import { createWorkflowAppState } from "./app-state.js";
import { createWorkflowUiServices } from "./app-ui-services.js";
import { createWorkflowAppServices } from "./app-services.js";

ensureWorkflowDesktopBridge(window);

const $ = (id) => document.getElementById(id);

const store = createWorkflowStore();
const QUALITY_GATE_PREFS_KEY = "aiwf.workflow.qualityGatePrefs.v1";

const els = getWorkflowElements($);
let syncCanvasPanels = () => {};

function renderAll() {
  canvas.setSnap(!!els.snapGrid.checked);
  canvas.setArrangePolicy({ preventOverlapOnAlign: false });
  canvas.render();
  syncCanvasPanels();
}

const appState = createWorkflowAppState();
const {
  cfgViewModeRef,
  selectedEdgeRef,
  compareState,
  preflightReportRef,
  autoFixSummaryRef,
  templateAcceptanceRef,
  migrationReportRef,
  graphShellApi,
  state,
  assignGraphShellApi,
  setRenderMigrationReport,
  getRenderMigrationReport,
} = appState;

let appServices = null;

const { canvas, attachGraphShell } = createWorkflowCanvasShell(buildCanvasShellContext({
  store,
  nodeCatalog: NODE_CATALOG,
  els,
  setStatus: (...args) => setStatus(...args),
  renderAll,
  selectedEdgeRef,
  getRenderNodeConfigEditor: () => appServices?.renderNodeConfigEditor || (() => {}),
  getRenderEdgeConfigEditor: () => appServices?.renderEdgeConfigEditor || (() => {}),
}));

const {
  setStatus,
  renderNodeRuns,
  renderDiagRuns,
  refreshDiagnostics,
  refreshOfflineBoundaryHint,
  applyDeepSeekDefaults,
} = createWorkflowUiServices(buildUiServicesContext({
  els,
  store,
  canvas,
  renderAll,
}));


appServices = createWorkflowAppServices(buildAppServicesContext({
  els,
  store,
  canvas,
  setStatus,
  renderAll,
  refreshOfflineBoundaryHint,
  graphShellApi,
  state,
  staticConfig: buildWorkflowStaticConfig(NODE_CATALOG, QUALITY_GATE_PREFS_KEY),
  defaultNodeConfig,
  migrateLoadedWorkflowGraph,
  renderNodeRuns,
  refreshDiagnostics,
}));
syncCanvasPanels = appServices.syncCanvasPanels;

setRenderMigrationReport(appServices.renderMigrationReportImpl);
attachGraphShell({
  assignGraphShellApi,
  getResetWorkflowName: () => String(store.state.graph?.name || "自由编排流程"),
  renderMigrationReport: appServices.renderMigrationReportImpl,
});
const {
  resetWorkflow,
  clearWorkflow,
} = graphShellApi;
bootWorkflowApp(buildBootWorkflowContext({
  els,
  setStatus,
  compareState,
  cfgViewModeRef,
  autoFixSummaryRef,
  selectedEdgeRef,
  getRenderMigrationReport,
  refreshOfflineBoundaryHint,
  store,
  canvas,
  renderAll,
  defaultNodeConfig,
  syncCanvasPanels,
  bootServices: buildWorkflowBootServices({
    uiServices: {
      applyDeepSeekDefaults,
      renderNodeRuns,
      renderDiagRuns,
    },
    appServices,
    resetWorkflow,
    clearWorkflow,
  }),
}));



