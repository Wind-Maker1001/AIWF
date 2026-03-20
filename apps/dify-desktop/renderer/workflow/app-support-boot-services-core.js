function buildWorkflowBootCoreServices(ctx = {}) {
  const {
    appServices = {},
    resetWorkflow = () => {},
    clearWorkflow = () => {},
  } = ctx;

  return {
    handleAddNode: appServices.handleAddNode,
    resetWorkflow,
    clearWorkflow,
    runWorkflowPreflight: appServices.runWorkflowPreflight,
    exportPreflightReport: appServices.exportPreflightReport,
    autoFixGraphStructure: appServices.autoFixGraphStructure,
    renderAutoFixDiff: appServices.renderAutoFixDiff,
    runWorkflow: appServices.runWorkflow,
    enqueueWorkflowRun: appServices.enqueueWorkflowRun,
    pauseQueue: appServices.pauseQueue,
    resumeQueue: appServices.resumeQueue,
    refreshVersions: appServices.refreshVersions,
    compareVersions: appServices.compareVersions,
    refreshCacheStats: appServices.refreshCacheStats,
    clearCache: appServices.clearCache,
    exportJson: appServices.exportJson,
    saveFlow: appServices.saveFlow,
    loadFlow: appServices.loadFlow,
  };
}

export { buildWorkflowBootCoreServices };
