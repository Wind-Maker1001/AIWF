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

export {
  buildAppPublishUiDeps,
  buildPreflightActionsDeps,
  buildPreflightControllerDeps,
  buildRunControllerUiDeps,
};
