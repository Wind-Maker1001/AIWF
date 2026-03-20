import {
  TEMPLATE_STORAGE_KEY,
  BUILTIN_TEMPLATES,
  EDGE_HINTS_BY_NODE_TYPE,
  NODE_FORM_SCHEMAS,
} from "./static-config.js";

function ensureWorkflowDesktopBridge(windowObj) {
  if (windowObj.aiwfDesktop) return;
  try {
    if (windowObj.parent && windowObj.parent !== windowObj && windowObj.parent.aiwfDesktop) {
      windowObj.aiwfDesktop = windowObj.parent.aiwfDesktop;
    }
  } catch {}
}

function buildWorkflowStaticConfig(nodeCatalog, qualityGatePrefsKey) {
  return {
    templateStorageKey: TEMPLATE_STORAGE_KEY,
    builtinTemplates: BUILTIN_TEMPLATES,
    nodeFormSchemas: NODE_FORM_SCHEMAS,
    edgeHintsByNodeType: EDGE_HINTS_BY_NODE_TYPE,
    qualityGatePrefsKey,
    nodeCatalog,
  };
}

function buildCanvasShellContext(ctx = {}) {
  const {
    store,
    nodeCatalog,
    els,
    setStatus = () => {},
    renderAll = () => {},
    selectedEdgeRef,
    getRenderNodeConfigEditor = () => () => {},
    getRenderEdgeConfigEditor = () => () => {},
  } = ctx;

  return {
    store,
    nodeCatalog,
    els,
    setStatus: (...args) => setStatus(...args),
    renderAll,
    selectedEdgeRef,
    getRenderNodeConfigEditor,
    getRenderEdgeConfigEditor,
  };
}

function buildUiServicesContext(ctx = {}) {
  const {
    els,
    store,
    canvas,
    renderAll = () => {},
  } = ctx;

  return {
    els,
    store,
    canvas,
    renderAll,
  };
}

function buildAppServicesContext(ctx = {}) {
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

  return {
    els,
    store,
    canvas,
    setStatus,
    renderAll,
    refreshOfflineBoundaryHint,
    graphShellApi,
    state,
    staticConfig,
    defaultNodeConfig,
    migrateLoadedWorkflowGraph,
    renderNodeRuns,
    refreshDiagnostics,
  };
}

export {
  buildAppServicesContext,
  buildCanvasShellContext,
  buildUiServicesContext,
  buildWorkflowStaticConfig,
  ensureWorkflowDesktopBridge,
};
