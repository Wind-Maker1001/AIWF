function createRefState(initialValue) {
  let value = initialValue;
  return {
    get: () => value,
    set: (nextValue) => { value = nextValue; },
  };
}

function createWorkflowAppState() {
  const cfgViewModeRef = createRefState("form");
  const selectedEdgeRef = createRefState(null);
  const compareState = createRefState(null);
  const preflightReportRef = createRefState(null);
  const autoFixSummaryRef = createRefState(null);
  const templateAcceptanceRef = createRefState(null);
  const migrationReportRef = createRefState(() => {});

  const graphShellApi = {
    applyRestoredWorkflowGraph: () => {},
    resetWorkflow: () => {},
    clearWorkflow: () => {},
  };

  return {
    cfgViewModeRef,
    selectedEdgeRef,
    compareState,
    preflightReportRef,
    autoFixSummaryRef,
    templateAcceptanceRef,
    migrationReportRef,
    graphShellApi,
    state: {
      getSelectedEdge: () => selectedEdgeRef.get(),
      setSelectedEdge: (edge) => selectedEdgeRef.set(edge),
      getCfgViewMode: () => cfgViewModeRef.get(),
      setCfgViewMode: (mode) => cfgViewModeRef.set(mode),
      getLastCompareResult: () => compareState.get(),
      setLastCompareResult: (out) => compareState.set(out),
      getLastPreflightReport: () => preflightReportRef.get(),
      getLastTemplateAcceptanceReport: () => templateAcceptanceRef.get(),
      setLastPreflightReport: (report) => preflightReportRef.set(report),
      setLastAutoFixSummary: (summary) => autoFixSummaryRef.set(summary),
      setLastTemplateAcceptanceReport: (report) => templateAcceptanceRef.set(report),
    },
    assignGraphShellApi(api) {
      Object.assign(graphShellApi, api || {});
    },
    setRenderMigrationReport(fn) {
      migrationReportRef.set(typeof fn === "function" ? fn : () => {});
    },
    getRenderMigrationReport() {
      return migrationReportRef.get();
    },
  };
}

export { createWorkflowAppState };
