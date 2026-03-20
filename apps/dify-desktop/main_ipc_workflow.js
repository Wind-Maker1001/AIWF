const { createWorkflowIpcStateSupport } = require("./workflow_ipc_state");
const { createWorkflowHistorySupport, registerWorkflowHistoryIpc } = require("./workflow_ipc_history");
const { createWorkflowReportSupport, registerWorkflowReportIpc } = require("./workflow_ipc_reports");
const { registerWorkflowStoreIpc } = require("./workflow_ipc_store");
const { registerWorkflowQueueAppsIpc } = require("./workflow_ipc_queue_apps");
const { registerWorkflowRunIpc } = require("./workflow_ipc_run");
const { createWorkflowSandboxSupport } = require("./workflow_ipc_sandbox_alerts");
const { registerWorkflowSandboxManagementIpc } = require("./workflow_ipc_sandbox_management");
const { registerWorkflowSandboxIoIpc } = require("./workflow_ipc_sandbox_io");
const { registerWorkflowReviewIpc } = require("./workflow_ipc_review");

function registerWorkflowIpc(ctx) {
  const {
    app,
    ipcMain,
    dialog,
    fs,
    path,
    createWorkflowWindow,
    loadConfig,
    runMinimalWorkflow,
  } = ctx;
  const {
    appendAudit,
    appendDiagnostics,
    appendRunHistory,
    appendWorkflowVersion,
    cacheStats,
    clearNodeCache,
    createNodeCacheApi,
    defaultQueueControl,
    deepClone,
    diagnosticsLogPath,
    extractSandboxViolations,
    isMockIoAllowed,
    listQualityRuleCenter,
    listRunBaselines,
    listTemplateMarketplace,
    listWorkflowApps,
    listWorkflowVersions,
    loadQueueControl,
    loadWorkflowQueue,
    normalizeQueueControl,
    normalizeWorkflowConfig,
    readJsonFile,
    resolveMockFilePath,
    resolveOutputRoot,
    runHistoryPath,
    reviewHistoryPath,
    reviewQueuePath,
    sandboxAlertRuleVersionsPath,
    sandboxAlertRulesPath,
    sandboxAlertStatePath,
    sandboxAutoFixStatePath,
    saveQualityRuleCenter,
    saveQueueControl,
    saveRunBaselines,
    saveTemplateMarketplace,
    saveWorkflowApps,
    saveWorkflowQueue,
    workflowAuditPath,
    writeJsonFile,
  } = createWorkflowIpcStateSupport({
    app,
    fs,
    path,
    loadConfig,
    nowIso,
  });

  const queueState = {
    running: new Map(),
    draining: false,
    control: loadQueueControl(),
  };

  const historySupport = createWorkflowHistorySupport({
    fs,
    diagnosticsLogPath,
    runHistoryPath,
    reviewQueuePath,
    reviewHistoryPath,
  });

  const sandboxSupport = createWorkflowSandboxSupport({
    fs,
    readJsonFile,
    writeJsonFile,
    nowIso,
    appendAudit,
    extractSandboxViolations,
    sandboxAlertRuleVersionsPath,
    sandboxAlertRulesPath,
    sandboxAlertStatePath,
    sandboxAutoFixStatePath,
    workflowAuditPath,
    listRunHistory: historySupport.listRunHistory,
    queueState,
    defaultQueueControl,
    saveQueueControl,
    enqueueReviews: historySupport.enqueueReviews,
  });

  function nowIso() {
    return new Date().toISOString();
  }

  const reportSupport = createWorkflowReportSupport({
    deepClone,
    findRunById: historySupport.findRunById,
    listQualityRuleCenter,
    listRunBaselines,
  });

  registerWorkflowReportIpc(
    { ipcMain, dialog, app, fs, path },
    {
      isMockIoAllowed,
      resolveMockFilePath,
      nowIso,
      appendAudit,
      findRunById: historySupport.findRunById,
      listRunBaselines,
      saveRunBaselines,
      buildRunCompare: reportSupport.buildRunCompare,
      buildRunRegressionAgainstBaseline: reportSupport.buildRunRegressionAgainstBaseline,
      renderCompareHtml: reportSupport.renderCompareHtml,
      renderCompareMarkdown: reportSupport.renderCompareMarkdown,
      renderPreflightMarkdown: reportSupport.renderPreflightMarkdown,
      renderTemplateAcceptanceMarkdown: reportSupport.renderTemplateAcceptanceMarkdown,
    }
  );

  registerWorkflowHistoryIpc(
    { ipcMain },
    {
      readDiagnostics: historySupport.readDiagnostics,
      buildPerfDashboard: historySupport.buildPerfDashboard,
      listRunHistory: historySupport.listRunHistory,
      findRunById: historySupport.findRunById,
      runTimeline: historySupport.runTimeline,
      failureSummary: historySupport.failureSummary,
    }
  );

  registerWorkflowRunIpc(
    { ipcMain, createWorkflowWindow, loadConfig, runMinimalWorkflow },
    {
      normalizeWorkflowConfig,
      resolveOutputRoot,
      createNodeCacheApi,
      appendDiagnostics,
      appendRunHistory,
      extractSandboxViolations,
      appendAudit,
      historySupport,
      reportSupport,
      sandboxSupport,
    }
  );

  registerWorkflowReviewIpc(
    { ipcMain, dialog, app, fs, path, loadConfig, runMinimalWorkflow },
    {
      loadReviewQueue: historySupport.loadReviewQueue,
      saveReviewQueue: historySupport.saveReviewQueue,
      listReviewHistory: historySupport.listReviewHistory,
      filterReviewHistory: historySupport.filterReviewHistory,
      isMockIoAllowed,
      resolveMockFilePath,
      findRunById: historySupport.findRunById,
      normalizeWorkflowConfig,
      applyQualityRuleSetToPayload: reportSupport.applyQualityRuleSetToPayload,
      applySandboxAutoFixPayload: sandboxSupport.applySandboxAutoFixPayload,
      attachQualityGate: sandboxSupport.attachQualityGate,
      resolveOutputRoot,
      createNodeCacheApi,
      appendDiagnostics,
      appendRunHistory,
      extractSandboxViolations,
      appendSandboxViolationAudit: sandboxSupport.appendSandboxViolationAudit,
      maybeApplySandboxAutoFix: sandboxSupport.maybeApplySandboxAutoFix,
      enqueueReviews: historySupport.enqueueReviews,
      reviewHistoryPath,
    }
  );

  registerWorkflowStoreIpc(
    { ipcMain, dialog, app, fs, path },
    {
      appendAudit,
      appendWorkflowVersion,
      isMockIoAllowed,
      listQualityRuleCenter,
      listTemplateMarketplace,
      nowIso,
      resolveMockFilePath,
      saveQualityRuleCenter,
      saveTemplateMarketplace,
    }
  );

  registerWorkflowQueueAppsIpc(
    { ipcMain, loadConfig, runMinimalWorkflow },
    {
      queueState,
      defaultQueueControl,
      loadWorkflowQueue,
      saveWorkflowQueue,
      loadQueueControl,
      saveQueueControl,
      normalizeQueueControl,
      normalizeWorkflowConfig,
      resolveOutputRoot,
      createNodeCacheApi,
      appendDiagnostics,
      appendRunHistory,
      extractSandboxViolations,
      appendAudit,
      enqueueReviews: historySupport.enqueueReviews,
      listWorkflowVersions,
      listWorkflowApps,
      saveWorkflowApps,
      cacheStats,
      clearNodeCache,
      nowIso,
      reportSupport,
      sandboxSupport,
    }
  );

  registerWorkflowSandboxManagementIpc(
    { ipcMain, dialog, app, fs, path },
    {
      isMockIoAllowed,
      resolveMockFilePath,
      nowIso,
      appendAudit,
      sandboxSupport,
    }
  );

  registerWorkflowSandboxIoIpc(
    { ipcMain, dialog, app, fs, path },
    {
      isMockIoAllowed,
      resolveMockFilePath,
      sandboxAlertDedupWindowSec: sandboxSupport.sandboxAlertDedupWindowSec,
      sandboxAlerts: sandboxSupport.sandboxAlerts,
      nowIso,
      appendAudit,
    }
  );

}

module.exports = {
  registerWorkflowIpc,
};




