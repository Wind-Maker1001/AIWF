function buildWorkflowBootGovernanceServices(ctx = {}) {
  const { appServices = {} } = ctx;

  return {
    handleAppSchemaAdd: appServices.handleAppSchemaAdd,
    handleAppSchemaSyncJson: appServices.handleAppSchemaSyncJson,
    handleAppSchemaFromJson: appServices.handleAppSchemaFromJson,
    handleAppRunSyncJson: appServices.handleAppRunSyncJson,
    handleAppRunFromJson: appServices.handleAppRunFromJson,
    publishApp: appServices.publishApp,
    refreshApps: appServices.refreshApps,
    refreshTimeline: appServices.refreshTimeline,
    refreshFailureSummary: appServices.refreshFailureSummary,
    exportSandboxAudit: appServices.exportSandboxAudit,
    loadSandboxRules: appServices.loadSandboxRules,
    saveSandboxRules: appServices.saveSandboxRules,
    applySandboxPreset: appServices.applySandboxPreset,
    applySandboxMute: appServices.applySandboxMute,
    exportSandboxPreset: appServices.exportSandboxPreset,
    importSandboxPreset: appServices.importSandboxPreset,
    refreshSandboxAutoFixLog: appServices.refreshSandboxAutoFixLog,
    refreshQualityGateReports: appServices.refreshQualityGateReports,
    exportQualityGateReports: appServices.exportQualityGateReports,
    saveQualityGatePrefs: appServices.saveQualityGatePrefs,
    refreshAudit: appServices.refreshAudit,
    exportReviewHistory: appServices.exportReviewHistory,
    compareRuns: appServices.compareRuns,
    saveCurrentRunAsBaseline: appServices.saveCurrentRunAsBaseline,
    compareWithLatestBaseline: appServices.compareWithLatestBaseline,
    loadLineageForRunA: appServices.loadLineageForRunA,
    exportCompareReport: appServices.exportCompareReport,
  };
}

export { buildWorkflowBootGovernanceServices };
