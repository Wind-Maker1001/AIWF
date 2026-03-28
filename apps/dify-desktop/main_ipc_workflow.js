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
const { createWorkflowQualityRuleSetSupport } = require("./workflow_quality_rule_store");
const { createWorkflowSandboxRuleStore } = require("./workflow_sandbox_rule_store");
const { createWorkflowSandboxAutoFixStore } = require("./workflow_sandbox_autofix_store");
const { createWorkflowAppRegistryStore } = require("./workflow_app_registry_store");
const { createWorkflowVersionStore } = require("./workflow_version_store");
const { createWorkflowManualReviewStore } = require("./workflow_manual_review_store");
const { createWorkflowRunAuditStore } = require("./workflow_run_audit_store");
const { createWorkflowRunBaselineStore } = require("./workflow_run_baseline_store");

function createWorkflowAuditMirrorSupport(deps) {
  const {
    appendAudit,
    appendRunHistory,
    loadConfig = () => ({}),
    workflowRunAuditStore,
  } = deps;

  function shouldWriteLocalAuditMirror(cfg = null) {
    return false;
  }

  function shouldWriteLocalRunHistoryMirror(cfg = null) {
    return false;
  }

  function appendAuditMirrored(action, detail, cfg = null) {
    workflowRunAuditStore.mirrorAudit(action, detail, cfg).catch(() => {});
  }

  function appendRunHistoryMirrored(run, payload, mergedConfig) {
    workflowRunAuditStore.mirrorRun(run, payload, mergedConfig, mergedConfig).catch(() => {});
  }

  return {
    appendAuditMirrored,
    appendRunHistoryMirrored,
    shouldWriteLocalAuditMirror,
    shouldWriteLocalRunHistoryMirror,
  };
}

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
    cacheStats,
    clearNodeCache,
    createNodeCacheApi,
    defaultQueueControl,
    deepClone,
    diagnosticsLogPath,
    extractSandboxViolations,
    isMockIoAllowed,
    listTemplateMarketplace,
    loadQueueControl,
    loadWorkflowQueue,
    normalizeQueueControl,
    normalizeWorkflowConfig,
    readJsonFile,
    resolveMockFilePath,
    resolveOutputRoot,
    runHistoryPath,
    saveQueueControl,
    saveTemplateMarketplace,
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

  const qualityRuleSetSupport = createWorkflowQualityRuleSetSupport({
    loadConfig,
    nowIso,
  });

  const workflowAppRegistryStore = createWorkflowAppRegistryStore({
    loadConfig,
    nowIso,
    validateWorkflowGraph: (graph) => {
      const { assertWorkflowContract } = require("./workflow_contract");
      assertWorkflowContract(graph, { requireNonEmptyNodes: true });
    },
  });

  const workflowVersionStore = createWorkflowVersionStore({
    loadConfig,
  });

  const workflowRunAuditStore = createWorkflowRunAuditStore({
    loadConfig,
    fs,
    runHistoryPath,
    workflowAuditPath,
  });

  const workflowAuditMirrorSupport = createWorkflowAuditMirrorSupport({
    appendAudit,
    appendRunHistory,
    loadConfig,
    workflowRunAuditStore,
  });
  const {
    appendAuditMirrored,
    appendRunHistoryMirrored,
  } = workflowAuditMirrorSupport;

  const historySupport = createWorkflowHistorySupport({
    fs,
    diagnosticsLogPath,
    runHistoryPath,
    listRunRecords: (limit, cfg = null) => workflowRunAuditStore.listRuns(limit, cfg),
  });

  const workflowManualReviewStore = createWorkflowManualReviewStore({
    loadConfig,
  });

  const workflowRunBaselineStore = createWorkflowRunBaselineStore({
    loadConfig,
  });

  const sandboxSupport = createWorkflowSandboxSupport({
    fs,
    readJsonFile,
    writeJsonFile,
    nowIso,
    appendAudit: appendAuditMirrored,
    extractSandboxViolations,
    listRunHistory: historySupport.listRunHistory,
    listRunRecords: (limit, cfg = null) => workflowRunAuditStore.listRuns(limit, cfg),
    queueState,
    defaultQueueControl,
    saveQueueControl,
    enqueueReviews: (items) => {
      workflowManualReviewStore.enqueue(items).catch(() => {});
    },
    persistSandboxAutoFixState: (state, cfg = null) => sandboxAutoFixStore.persistStateMirror(state, cfg),
  });

  const sandboxRuleStore = createWorkflowSandboxRuleStore({
    loadConfig,
    sandboxSupport,
  });

  const sandboxAutoFixStore = createWorkflowSandboxAutoFixStore({
    loadConfig,
    sandboxSupport,
  });

  function nowIso() {
    return new Date().toISOString();
  }

  const reportSupport = createWorkflowReportSupport({
    deepClone,
    getRun: (runId, cfg = null) => workflowRunAuditStore.getRun(runId, cfg),
    listRunBaselines: (limit = 200, cfg = null) => workflowRunBaselineStore.list(limit, cfg),
    qualityRuleSetSupport,
  });

  registerWorkflowReportIpc(
    { ipcMain, dialog, app, fs, path },
    {
      isMockIoAllowed,
      resolveMockFilePath,
      nowIso,
      appendAudit: appendAuditMirrored,
      getRun: (runId, cfg = null) => workflowRunAuditStore.getRun(runId, cfg),
      listRunBaselines: (limit = 200, cfg = null) => workflowRunBaselineStore.list(limit, cfg),
      saveRunBaseline: (item, cfg = null) => workflowRunBaselineStore.save(item, cfg),
      buildRunCompare: reportSupport.buildRunCompare,
      buildRunRegressionAgainstBaseline: reportSupport.buildRunRegressionAgainstBaseline,
      buildPreflightReportEnvelope: reportSupport.buildPreflightReportEnvelope,
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
      listRunHistory: (limit) => workflowRunAuditStore.listRuns(limit),
      getRun: (runId, cfg = null) => workflowRunAuditStore.getRun(runId, cfg),
      runTimeline: (runId) => workflowRunAuditStore.getRunTimeline(runId),
      failureSummary: (limit) => workflowRunAuditStore.getFailureSummary(limit),
    }
  );

  registerWorkflowRunIpc(
    { ipcMain, createWorkflowWindow, loadConfig, runMinimalWorkflow },
    {
      normalizeWorkflowConfig,
      resolveOutputRoot,
      createNodeCacheApi,
      appendDiagnostics,
      appendRunHistory: appendRunHistoryMirrored,
      extractSandboxViolations,
      appendAudit: appendAuditMirrored,
      getRun: (runId, cfg = null) => workflowRunAuditStore.getRun(runId, cfg),
      enqueueReviews: (items, cfg = null) => workflowManualReviewStore.enqueue(items, cfg),
      reportSupport,
      sandboxSupport,
      sandboxRuleStore,
      sandboxAutoFixStore,
      workflowManualReviewStore,
    }
  );

  registerWorkflowReviewIpc(
    { ipcMain, dialog, app, fs, path, loadConfig, runMinimalWorkflow },
    {
      isMockIoAllowed,
      resolveMockFilePath,
      getRun: (runId, cfg = null) => workflowRunAuditStore.getRun(runId, cfg),
      normalizeWorkflowConfig,
      applyQualityRuleSetToPayload: reportSupport.applyQualityRuleSetToPayload,
      applySandboxAutoFixPayload: sandboxSupport.applySandboxAutoFixPayload,
      attachQualityGate: sandboxSupport.attachQualityGate,
      resolveOutputRoot,
      createNodeCacheApi,
      appendDiagnostics,
      appendRunHistory: appendRunHistoryMirrored,
      extractSandboxViolations,
      appendSandboxViolationAudit: sandboxSupport.appendSandboxViolationAudit,
      maybeApplySandboxAutoFix: sandboxSupport.maybeApplySandboxAutoFix,
      enqueueReviews: (items, cfg = null) => workflowManualReviewStore.enqueue(items, cfg),
      sandboxRuleStore,
      sandboxAutoFixStore,
      workflowManualReviewStore,
    }
  );

  registerWorkflowStoreIpc(
    { ipcMain, dialog, app, fs, path },
    {
      appendAudit: appendAuditMirrored,
      isMockIoAllowed,
      listTemplateMarketplace,
      nowIso,
      qualityRuleSetSupport,
      resolveMockFilePath,
      saveTemplateMarketplace,
      workflowVersionStore,
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
      appendRunHistory: appendRunHistoryMirrored,
      extractSandboxViolations,
      appendAudit: appendAuditMirrored,
      enqueueReviews: (items, cfg = null) => workflowManualReviewStore.enqueue(items, cfg),
      cacheStats,
      clearNodeCache,
      nowIso,
      reportSupport,
      sandboxSupport,
      sandboxRuleStore,
      sandboxAutoFixStore,
      workflowAppRegistryStore,
      workflowVersionStore,
    }
  );

  registerWorkflowSandboxManagementIpc(
    { ipcMain, dialog, app, fs, path },
    {
      isMockIoAllowed,
      resolveMockFilePath,
      nowIso,
      appendAudit: appendAuditMirrored,
      sandboxSupport,
      sandboxRuleStore,
      sandboxAutoFixStore,
      workflowRunAuditStore,
    }
  );

  registerWorkflowSandboxIoIpc(
    { ipcMain, dialog, app, fs, path },
    {
      isMockIoAllowed,
      resolveMockFilePath,
      sandboxAlertDedupWindowSec: sandboxSupport.sandboxAlertDedupWindowSec,
      sandboxAlerts: async (limit, thresholds, dedupWindowSec) => {
        const rulesOut = await sandboxRuleStore.getRules();
        if (!rulesOut?.ok) return rulesOut;
        return await sandboxSupport.sandboxAlerts(limit, thresholds, dedupWindowSec, rulesOut.rules || {});
      },
      nowIso,
      appendAudit: appendAuditMirrored,
    }
  );

}

module.exports = {
  createWorkflowAuditMirrorSupport,
  registerWorkflowIpc,
};




