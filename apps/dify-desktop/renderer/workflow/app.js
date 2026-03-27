import { defaultNodeConfig, NODE_CATALOG } from "./defaults.js";
import { createWorkflowStore } from "./store.js";
import { createWorkflowCanvasShell } from "./app-canvas-shell.js";
import { getWorkflowElements } from "./elements.js";
import { migrateLoadedWorkflowGraph } from "./template-utils.js";
import {
  buildWorkflowBootCoreServices,
} from "./app-support-boot-services-core.js";
import {
  buildWorkflowBootGovernanceServices,
} from "./app-support-boot-services-governance.js";
import {
  buildWorkflowBootTemplateServices,
} from "./app-support-boot-services-template.js";
import {
  buildWorkflowBootEditorServices,
} from "./app-support-boot-services-editor.js";
import {
  buildWorkflowBootPanelServices,
} from "./app-support-boot-services-panels.js";
import {
  BUILTIN_TEMPLATES,
  EDGE_HINTS_BY_NODE_TYPE,
  NODE_FORM_SCHEMAS,
  TEMPLATE_STORAGE_KEY,
} from "./static-config.js";
// Static workflow metadata remains sourced from `static-config.js`.

import { bootWorkflowApp } from "./app-boot.js";
import { createWorkflowAppState } from "./app-state.js";
import { createWorkflowUiServices } from "./app-ui-services.js";
import { createWorkflowAppServices } from "./app-services.js";

function ensureWorkflowDesktopBridge(windowObj) {
  if (windowObj.aiwfDesktop) return;
  try {
    if (windowObj.parent && windowObj.parent !== windowObj && windowObj.parent.aiwfDesktop) {
      windowObj.aiwfDesktop = windowObj.parent.aiwfDesktop;
    }
  } catch {}
}

function buildSelectedEdgeAccess(ref) {
  return {
    get: () => ref.get(),
    set: (edge) => { ref.set(edge); },
  };
}

function buildWorkflowBootServices(ctx = {}) {
  return {
    ...buildWorkflowBootCoreServices(ctx),
    ...buildWorkflowBootGovernanceServices(ctx),
    ...buildWorkflowBootTemplateServices(ctx),
    ...buildWorkflowBootEditorServices(ctx),
    ...buildWorkflowBootPanelServices(ctx),
  };
}

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

const { canvas, attachGraphShell } = createWorkflowCanvasShell({
  store,
  nodeCatalog: NODE_CATALOG,
  els,
  setStatus: (...args) => setStatus(...args),
  renderAll,
  selectedEdgeRef,
  getRenderNodeConfigEditor: () => appServices?.renderNodeConfigEditor || (() => {}),
  getRenderEdgeConfigEditor: () => appServices?.renderEdgeConfigEditor || (() => {}),
});

const {
  setStatus,
  renderNodeRuns,
  renderDiagRuns,
  refreshDiagnostics,
  refreshOfflineBoundaryHint,
  applyDeepSeekDefaults,
} = createWorkflowUiServices({
  els,
  store,
  canvas,
  renderAll,
});


appServices = createWorkflowAppServices({
  els,
  store,
  canvas,
  setStatus,
  renderAll,
  refreshOfflineBoundaryHint,
  graphShellApi,
  state,
  staticConfig: {
    templateStorageKey: TEMPLATE_STORAGE_KEY,
    builtinTemplates: BUILTIN_TEMPLATES,
    nodeFormSchemas: NODE_FORM_SCHEMAS,
    edgeHintsByNodeType: EDGE_HINTS_BY_NODE_TYPE,
    qualityGatePrefsKey: QUALITY_GATE_PREFS_KEY,
    nodeCatalog: NODE_CATALOG,
  },
  defaultNodeConfig,
  migrateLoadedWorkflowGraph,
  renderNodeRuns,
  refreshDiagnostics,
});
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
bootWorkflowApp({
  els,
  setStatus,
  getLastCompareResult: () => compareState.get(),
  getCfgViewMode: () => cfgViewModeRef.get(),
  setLastAutoFixSummary: (out) => { autoFixSummaryRef.set(out); },
  selectedEdgeRef: buildSelectedEdgeAccess(selectedEdgeRef),
  renderMigrationReport: getRenderMigrationReport(),
  refreshOfflineBoundaryHint,
  store,
  canvas,
  renderAll,
  defaultNodeConfig,
  syncCanvasPanels,
  ...buildWorkflowBootServices({
    uiServices: {
      applyDeepSeekDefaults,
      renderNodeRuns,
      renderDiagRuns,
    },
    appServices,
    resetWorkflow,
    clearWorkflow,
  }),
});



