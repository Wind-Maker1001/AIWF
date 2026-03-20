import { bindIfPresent } from "./app-toolbar-bindings-core.js";

function bindWorkflowGovernanceToolbarActions(ctx = {}) {
  const {
    els,
    handleAppSchemaAdd = () => {},
    handleAppSchemaSyncJson = () => {},
    handleAppSchemaFromJson = () => {},
    handleAppRunSyncJson = () => {},
    handleAppRunFromJson = () => {},
    publishApp = () => {},
    refreshApps = () => {},
    refreshTimeline = () => {},
    refreshFailureSummary = () => {},
    refreshSandboxAlerts = () => {},
    exportSandboxAudit = () => {},
    loadSandboxRules = () => {},
    saveSandboxRules = () => {},
    applySandboxPreset = () => {},
    applySandboxMute = () => {},
    refreshSandboxRuleVersions = () => {},
    exportSandboxPreset = () => {},
    importSandboxPreset = () => {},
    refreshSandboxAutoFixLog = () => {},
    refreshQualityGateReports = () => {},
    exportQualityGateReports = () => {},
    saveQualityGatePrefs = () => {},
    refreshAudit = () => {},
    refreshDiagnostics = () => {},
    refreshRunHistory = () => {},
    refreshReviewQueue = () => {},
    refreshReviewHistory = () => {},
    exportReviewHistory = () => {},
    compareRuns = () => {},
    saveCurrentRunAsBaseline = () => {},
    compareWithLatestBaseline = () => {},
    loadLineageForRunA = () => {},
    exportCompareReport = () => {},
  } = ctx;

  bindIfPresent(els.btnAppSchemaAdd, "click", handleAppSchemaAdd);
  bindIfPresent(els.btnAppSchemaSyncJson, "click", handleAppSchemaSyncJson);
  bindIfPresent(els.btnAppSchemaFromJson, "click", handleAppSchemaFromJson);
  bindIfPresent(els.btnAppRunSyncJson, "click", handleAppRunSyncJson);
  bindIfPresent(els.btnAppRunFromJson, "click", handleAppRunFromJson);
  bindIfPresent(els.btnPublishApp, "click", publishApp);
  bindIfPresent(els.btnAppsRefresh, "click", refreshApps);
  bindIfPresent(els.btnTimelineRefresh, "click", refreshTimeline);
  bindIfPresent(els.btnFailureSummaryRefresh, "click", refreshFailureSummary);
  bindIfPresent(els.btnSandboxAlertsRefresh, "click", refreshSandboxAlerts);
  bindIfPresent(els.btnSandboxExport, "click", exportSandboxAudit);
  bindIfPresent(els.btnSandboxRulesLoad, "click", loadSandboxRules);
  bindIfPresent(els.btnSandboxRulesSave, "click", saveSandboxRules);
  bindIfPresent(els.btnSandboxPresetApply, "click", applySandboxPreset);
  bindIfPresent(els.btnSandboxMuteApply, "click", applySandboxMute);
  bindIfPresent(els.btnSandboxRuleVersions, "click", refreshSandboxRuleVersions);
  bindIfPresent(els.btnSandboxPresetExport, "click", exportSandboxPreset);
  bindIfPresent(els.btnSandboxPresetImport, "click", importSandboxPreset);
  bindIfPresent(els.btnSandboxAutoFixLog, "click", refreshSandboxAutoFixLog);
  bindIfPresent(els.btnQualityGateRefresh, "click", refreshQualityGateReports);
  bindIfPresent(els.btnQualityGateExport, "click", exportQualityGateReports);
  bindIfPresent(els.qualityGateRunIdFilter, "change", refreshQualityGateReports);
  bindIfPresent(els.qualityGateStatusFilter, "change", refreshQualityGateReports);
  bindIfPresent(els.qualityGateExportFormat, "change", saveQualityGatePrefs);
  bindIfPresent(els.sandboxThresholdYellow, "change", refreshSandboxAlerts);
  bindIfPresent(els.sandboxThresholdRed, "change", refreshSandboxAlerts);
  bindIfPresent(els.sandboxDedupWindowSec, "change", refreshSandboxAlerts);
  bindIfPresent(els.btnAuditRefresh, "click", refreshAudit);
  els.btnDiagRefresh.addEventListener("click", refreshDiagnostics);
  bindIfPresent(els.btnRunsRefresh, "click", refreshRunHistory);
  bindIfPresent(els.btnReviewsRefresh, "click", refreshReviewQueue);
  bindIfPresent(els.btnReviewHistoryRefresh, "click", refreshReviewHistory);
  bindIfPresent(els.btnReviewHistoryExport, "click", exportReviewHistory);
  bindIfPresent(els.btnCompareRuns, "click", compareRuns);
  bindIfPresent(els.btnSaveBaseline, "click", saveCurrentRunAsBaseline);
  bindIfPresent(els.btnCompareBaseline, "click", compareWithLatestBaseline);
  bindIfPresent(els.btnLoadLineage, "click", loadLineageForRunA);
  bindIfPresent(els.btnExportCompareReport, "click", exportCompareReport);
}

export { bindWorkflowGovernanceToolbarActions };
