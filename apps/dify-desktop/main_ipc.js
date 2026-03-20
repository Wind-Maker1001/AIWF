const { registerMetricsIpc } = require("./main_ipc_metrics");
const { registerWorkflowIpc } = require("./main_ipc_workflow");
const { registerRuntimeIpc } = require("./main_ipc_runtime");
const { registerBuildGuardIpc } = require("./main_ipc_build_guard");
const { createMainIpcLogSupport } = require("./main_ipc_logs");
const { createMainIpcPathSupport, registerPathIpc } = require("./main_ipc_paths");
const { registerCleaningIpc } = require("./main_ipc_cleaning");

function registerIpcHandlers(ctx) {
  const {
    app,
    ipcMain,
    shell,
    loadConfig,
    saveConfig,
    baseHealth,
    runOfflineCleaningInWorker,
    runOfflinePrecheckInWorker,
    runOfflinePreviewInWorker,
    runViaBaseApi,
    listCleaningTemplates,
    path,
    fs,
    runModeAuditLogPath,
  } = ctx;

  const {
    appendRunModeAudit,
    localGateAuditLogPath,
    appendLocalGateAudit,
    readJsonlTail,
    hasRequiredGatePasses,
    localBuildAuditLogPath,
    appendLocalBuildAudit,
    classifyRemoteFailure,
    shouldFallbackByPolicy,
  } = createMainIpcLogSupport(ctx);
  const {
    resolveOutputRoot,
    isTrustedPath,
    buildDesktopOutputDir,
    buildSamplePoolDir,
    listSamplePoolFiles,
    addSamplePoolFiles,
    clearSamplePool,
    getLatestArtifactsDir,
    checkDesktopBuildArtifacts,
    copyRecentBuildArtifactsToDesktop,
  } = createMainIpcPathSupport(ctx);

  registerCleaningIpc(ctx, {
    resolveOutputRoot,
    appendRunModeAudit,
    classifyRemoteFailure,
    shouldFallbackByPolicy,
  });

  registerPathIpc(ctx, {
    isTrustedPath,
    getLatestArtifactsDir,
    listSamplePoolFiles,
    addSamplePoolFiles,
    clearSamplePool,
  });

  registerBuildGuardIpc(ctx, {
    localGateAuditLogPath,
    readJsonlTail,
    hasRequiredGatePasses,
    buildSamplePoolDir,
    resolveOutputRoot,
    appendLocalGateAudit,
    localBuildAuditLogPath,
    appendLocalBuildAudit,
    checkDesktopBuildArtifacts,
    copyRecentBuildArtifactsToDesktop,
    buildDesktopOutputDir,
  });

  registerMetricsIpc(ctx);
  registerWorkflowIpc(ctx);
  registerRuntimeIpc(ctx);
}

module.exports = {
  registerIpcHandlers,
};


