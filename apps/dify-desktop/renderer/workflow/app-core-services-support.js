function buildSupportUiDeps(ctx = {}) {
  const {
    setStatus = () => {},
    getLastCompareResult = () => null,
    setLastCompareResult = () => {},
  } = ctx;

  return {
    setStatus,
    getLastCompareResult,
    setLastCompareResult,
  };
}

function buildRunPayloadUiDeps(ctx = {}) {
  const {
    store,
    supportUi = {},
  } = ctx;

  return {
    store,
    sandboxDedupWindowSec: supportUi.sandboxDedupWindowSec,
  };
}

function buildAppFormUiDeps(ctx = {}) {
  const { setStatus = () => {} } = ctx;
  return { setStatus };
}

function buildConfigUiDeps(ctx = {}) {
  const {
    store,
    canvas,
    nodeFormSchemas = {},
    edgeHintsByNodeType = {},
    setStatus = () => {},
    renderAll = () => {},
    refreshOfflineBoundaryHint = () => {},
    getSelectedEdge = () => null,
    setSelectedEdge = () => {},
    getCfgViewMode = () => "form",
    setCfgViewMode = () => {},
  } = ctx;

  return {
    store,
    canvas,
    nodeFormSchemas,
    edgeHintsByNodeType,
    setStatus,
    renderAll,
    refreshOfflineBoundaryHint,
    getSelectedEdge,
    setSelectedEdge,
    getCfgViewMode,
    setCfgViewMode,
  };
}

function buildTemplateUiDeps(ctx = {}) {
  const {
    templateStorageKey = "",
    builtinTemplates = [],
    store,
    setStatus = () => {},
    renderAll = () => {},
    renderMigrationReport = () => {},
    runPayloadUi = {},
    syncRunParamsFormFromJson = () => {},
  } = ctx;

  return {
    templateStorageKey,
    builtinTemplates,
    store,
    setStatus,
    renderAll,
    renderMigrationReport,
    graphPayload: runPayloadUi.graphPayload,
    syncRunParamsFormFromJson,
  };
}

export {
  buildAppFormUiDeps,
  buildConfigUiDeps,
  buildRunPayloadUiDeps,
  buildSupportUiDeps,
  buildTemplateUiDeps,
};
