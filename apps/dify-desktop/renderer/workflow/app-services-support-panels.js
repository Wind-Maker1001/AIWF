function buildPanelsUiDeps(ctx = {}) {
  const {
    els,
    setStatus = () => {},
    getPanelServices = () => null,
    coreServices = {},
    graphShellApi,
    refreshDiagnostics = async () => {},
  } = ctx;

  return {
    setStatus,
    refreshRunHistory: () => getPanelServices()?.refreshRunHistory?.(),
    refreshReviewQueue: () => getPanelServices()?.refreshReviewQueue?.(),
    showReviewQueue: async () => {
      await getPanelServices()?.refreshReviewQueue?.();
      try { els.btnReviewsRefresh?.scrollIntoView?.({ block: "center" }); } catch {}
    },
    showQualityGate: async (runId) => {
      if (els.qualityGateRunIdFilter) els.qualityGateRunIdFilter.value = String(runId || "");
      if (els.qualityGateStatusFilter) els.qualityGateStatusFilter.value = "blocked";
      await getPanelServices()?.refreshQualityGateReports?.();
      try { els.btnQualityGateRefresh?.scrollIntoView?.({ block: "center" }); } catch {}
    },
    refreshReviewHistory: () => coreServices.refreshReviewHistory(),
    refreshQueue: () => getPanelServices()?.refreshQueue?.(),
    refreshDiagnostics,
    refreshSandboxRuleVersions: () => getPanelServices()?.refreshSandboxRuleVersions?.(),
    refreshSandboxAlerts: () => getPanelServices()?.refreshSandboxAlerts?.(),
    applySandboxRulesToUi: coreServices.applySandboxRulesToUi,
    applyRestoredGraph: (graph) => graphShellApi.applyRestoredWorkflowGraph(graph),
    renderSandboxHealth: coreServices.renderSandboxHealth,
    normalizeAppSchemaObject: coreServices.normalizeAppSchemaObject,
    renderAppSchemaForm: coreServices.renderAppSchemaForm,
    appSchemaRowsFromObject: coreServices.appSchemaRowsFromObject,
    renderRunParamsFormBySchema: coreServices.renderRunParamsFormBySchema,
    collectRunParamsForm: coreServices.collectRunParamsForm,
    runPayload: coreServices.runPayload,
  };
}

function buildPanelServicesDeps(ctx = {}) {
  const {
    els,
    store,
    staticConfig = {},
    setStatus = () => {},
    panelsUi = {},
    coreServices = {},
  } = ctx;

  return {
    els,
    store,
    qualityGatePrefsKey: staticConfig.qualityGatePrefsKey,
    setStatus,
    qualityGateFilterPayload: panelsUi.qualityGateFilterPayload,
    qualityGatePrefsPayload: panelsUi.qualityGatePrefsPayload,
    renderQualityGateRows: panelsUi.renderQualityGateRows,
    sandboxThresholdsPayload: coreServices.sandboxThresholdsPayload,
    sandboxDedupWindowSec: coreServices.sandboxDedupWindowSec,
    sandboxRulesPayloadFromUi: coreServices.sandboxRulesPayloadFromUi,
    applySandboxRulesToUi: coreServices.applySandboxRulesToUi,
    applySandboxPresetToUi: coreServices.applySandboxPresetToUi,
    currentSandboxPresetPayload: coreServices.currentSandboxPresetPayload,
    applySandboxPresetPayload: coreServices.applySandboxPresetPayload,
    renderSandboxRows: panelsUi.renderSandboxRows,
    renderSandboxRuleVersionRows: panelsUi.renderSandboxRuleVersionRows,
    renderSandboxAutoFixRows: panelsUi.renderSandboxAutoFixRows,
    renderTimelineRows: panelsUi.renderTimelineRows,
    renderFailureRows: panelsUi.renderFailureRows,
    renderAuditRows: panelsUi.renderAuditRows,
    renderRunHistoryRows: panelsUi.renderRunHistoryRows,
    renderQueueRows: panelsUi.renderQueueRows,
    renderQueueControl: panelsUi.renderQueueControl,
    renderReviewRows: panelsUi.renderReviewRows,
    renderVersionRows: panelsUi.renderVersionRows,
    renderVersionCompare: panelsUi.renderVersionCompare,
    renderCacheStats: panelsUi.renderCacheStats,
  };
}

export {
  buildPanelsUiDeps,
  buildPanelServicesDeps,
};
