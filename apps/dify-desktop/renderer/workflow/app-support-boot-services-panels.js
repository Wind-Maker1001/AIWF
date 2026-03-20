function buildWorkflowBootPanelServices(ctx = {}) {
  const { appServices = {} } = ctx;

  return {
    renderRunHistoryRows: appServices.renderRunHistoryRows,
    renderQueueRows: appServices.renderQueueRows,
    renderQueueControl: appServices.renderQueueControl,
    renderVersionRows: appServices.renderVersionRows,
    renderVersionCompare: appServices.renderVersionCompare,
    renderReviewRows: appServices.renderReviewRows,
    renderCacheStats: appServices.renderCacheStats,
    syncAppSchemaJsonFromForm: appServices.syncAppSchemaJsonFromForm,
    renderAppRows: appServices.renderAppRows,
    renderTimelineRows: appServices.renderTimelineRows,
    renderFailureRows: appServices.renderFailureRows,
    sandboxThresholdsPayload: appServices.sandboxThresholdsPayload,
    sandboxDedupWindowSec: appServices.sandboxDedupWindowSec,
    renderSandboxRows: appServices.renderSandboxRows,
    renderSandboxRuleVersionRows: appServices.renderSandboxRuleVersionRows,
    renderSandboxAutoFixRows: appServices.renderSandboxAutoFixRows,
    loadQualityGatePrefs: appServices.loadQualityGatePrefs,
    renderAuditRows: appServices.renderAuditRows,
    renderReviewHistoryRows: appServices.renderReviewHistoryRows,
    renderCompareResult: appServices.renderCompareResult,
    renderPreflightReport: appServices.renderPreflightReport,
  };
}

export { buildWorkflowBootPanelServices };
