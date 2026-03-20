const DEFAULT_APP_SCHEMA_ROWS = [
  { key: "title", type: "string", required: true, defaultText: "", description: "任务标题" },
];

function parseStartupAppSchemaJson(text) {
  try {
    return JSON.parse(String(text || "{}"));
  } catch {
    return {};
  }
}

function renderInitialWorkflowAppState(ctx = {}) {
  const {
    els,
    renderPalette = () => {},
    renderTemplateSelect = () => {},
    setCfgMode = () => {},
    setEdgeWhenBuilderVisibility = () => {},
    rebuildEdgeHints = () => {},
    renderAll = () => {},
    renderNodeRuns = () => {},
    renderDiagRuns = () => {},
    renderRunHistoryRows = () => {},
    renderQueueRows = () => {},
    renderQueueControl = () => {},
    renderVersionRows = () => {},
    renderVersionCompare = () => {},
    renderReviewRows = () => {},
    renderCacheStats = () => {},
    renderAppSchemaForm = () => {},
    syncAppSchemaJsonFromForm = () => {},
    renderRunParamsFormBySchema = () => {},
    renderAppRows = () => {},
    renderTimelineRows = () => {},
    renderFailureRows = () => {},
    renderSandboxHealth = () => {},
    sandboxThresholdsPayload = () => ({}),
    sandboxDedupWindowSec = () => 0,
    renderSandboxRows = () => {},
    renderSandboxRuleVersionRows = () => {},
    renderSandboxAutoFixRows = () => {},
    renderQualityGateRows = () => {},
    loadQualityGatePrefs = () => {},
    renderAuditRows = () => {},
    renderReviewHistoryRows = () => {},
    renderMigrationReport = () => {},
    renderCompareResult = () => {},
    renderPreflightReport = () => {},
    renderAutoFixDiff = () => {},
  } = ctx;

  renderPalette();
  renderTemplateSelect();
  setCfgMode("form");
  setEdgeWhenBuilderVisibility(els.edgeWhenKind?.value || "none");
  rebuildEdgeHints(null);
  renderAll();
  renderNodeRuns([]);
  renderDiagRuns({});
  renderRunHistoryRows([]);
  renderQueueRows([]);
  renderQueueControl({});
  renderVersionRows([]);
  renderVersionCompare({ ok: false, error: "暂无" });
  renderReviewRows([]);
  renderCacheStats({});
  renderAppSchemaForm(DEFAULT_APP_SCHEMA_ROWS);
  syncAppSchemaJsonFromForm();
  renderRunParamsFormBySchema(parseStartupAppSchemaJson(els.appSchemaJson?.value || "{}"), {});
  renderAppRows([]);
  renderTimelineRows({ ok: false });
  renderFailureRows({});
  renderSandboxHealth({
    level: "green",
    total: 0,
    thresholds: sandboxThresholdsPayload(),
    dedup_window_sec: sandboxDedupWindowSec(),
    suppressed: 0,
  });
  renderSandboxRows({});
  renderSandboxRuleVersionRows([]);
  renderSandboxAutoFixRows([]);
  renderQualityGateRows([]);
  loadQualityGatePrefs();
  renderAuditRows([]);
  renderReviewHistoryRows([]);
  renderMigrationReport({ migrated: false });
  renderCompareResult({ ok: false, error: "暂无" });
  renderPreflightReport({ ok: true, issues: [] });
  renderAutoFixDiff(null);
}

function fireAndForgetStartupTask(task) {
  try {
    Promise.resolve(task?.()).catch(() => {});
  } catch {}
}

function runWorkflowStartupRefreshes(ctx = {}) {
  const {
    refreshTemplateMarketplace = async () => {},
    refreshQualityRuleSets = async () => {},
    renderTemplateSelect = () => {},
    refreshDiagnostics = async () => {},
    refreshRunHistory = async () => {},
    refreshQueue = async () => {},
    refreshVersions = async () => {},
    refreshCacheStats = async () => {},
    refreshApps = async () => {},
    refreshFailureSummary = async () => {},
    refreshSandboxAlerts = async () => {},
    refreshSandboxRuleVersions = async () => {},
    refreshSandboxAutoFixLog = async () => {},
    refreshQualityGateReports = async () => {},
    refreshAudit = async () => {},
    refreshReviewQueue = async () => {},
    refreshReviewHistory = async () => {},
  } = ctx;

  fireAndForgetStartupTask(async () => {
    await refreshTemplateMarketplace();
    renderTemplateSelect();
  });
  fireAndForgetStartupTask(refreshQualityRuleSets);
  fireAndForgetStartupTask(refreshDiagnostics);
  fireAndForgetStartupTask(refreshRunHistory);
  fireAndForgetStartupTask(refreshQueue);
  fireAndForgetStartupTask(refreshVersions);
  fireAndForgetStartupTask(refreshCacheStats);
  fireAndForgetStartupTask(refreshApps);
  fireAndForgetStartupTask(refreshFailureSummary);
  fireAndForgetStartupTask(refreshSandboxAlerts);
  fireAndForgetStartupTask(refreshSandboxRuleVersions);
  fireAndForgetStartupTask(refreshSandboxAutoFixLog);
  fireAndForgetStartupTask(refreshQualityGateReports);
  fireAndForgetStartupTask(refreshAudit);
  fireAndForgetStartupTask(refreshReviewQueue);
  fireAndForgetStartupTask(refreshReviewHistory);
}

export {
  DEFAULT_APP_SCHEMA_ROWS,
  fireAndForgetStartupTask,
  parseStartupAppSchemaJson,
  renderInitialWorkflowAppState,
  runWorkflowStartupRefreshes,
};
