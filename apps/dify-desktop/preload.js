const { contextBridge, ipcRenderer, webUtils } = require("electron");

contextBridge.exposeInMainWorld("aiwfDesktop", {
  getConfig: () => ipcRenderer.invoke("aiwf:getConfig"),
  saveConfig: (cfg) => ipcRenderer.invoke("aiwf:saveConfig", cfg),
  health: (cfg) => ipcRenderer.invoke("aiwf:health", cfg),
  listCleaningTemplates: () => ipcRenderer.invoke("aiwf:listCleaningTemplates"),
  precheckCleaning: (payload, cfg) => ipcRenderer.invoke("aiwf:precheckCleaning", payload, cfg),
  previewDebateStyle: (payload, cfg) => ipcRenderer.invoke("aiwf:previewDebateStyle", payload, cfg),
  runCleaning: (payload, cfg) => ipcRenderer.invoke("aiwf:runCleaning", payload, cfg),
  runWorkflow: (payload, cfg) => ipcRenderer.invoke("aiwf:runWorkflow", payload, cfg),
  enqueueWorkflowTask: (req) => ipcRenderer.invoke("aiwf:enqueueWorkflowTask", req),
  listWorkflowQueue: (req) => ipcRenderer.invoke("aiwf:listWorkflowQueue", req),
  getWorkflowQueueControl: () => ipcRenderer.invoke("aiwf:getWorkflowQueueControl"),
  setWorkflowQueueControl: (req) => ipcRenderer.invoke("aiwf:setWorkflowQueueControl", req),
  cancelWorkflowTask: (req) => ipcRenderer.invoke("aiwf:cancelWorkflowTask", req),
  retryWorkflowTask: (req) => ipcRenderer.invoke("aiwf:retryWorkflowTask", req),
  listWorkflowRuns: (opts) => ipcRenderer.invoke("aiwf:listWorkflowRuns", opts),
  listWorkflowVersions: (req) => ipcRenderer.invoke("aiwf:listWorkflowVersions", req),
  restoreWorkflowVersion: (req) => ipcRenderer.invoke("aiwf:restoreWorkflowVersion", req),
  compareWorkflowVersions: (req) => ipcRenderer.invoke("aiwf:compareWorkflowVersions", req),
  getWorkflowNodeCacheStats: () => ipcRenderer.invoke("aiwf:getWorkflowNodeCacheStats"),
  clearWorkflowNodeCache: () => ipcRenderer.invoke("aiwf:clearWorkflowNodeCache"),
  publishWorkflowApp: (req) => ipcRenderer.invoke("aiwf:publishWorkflowApp", req),
  listWorkflowApps: (req) => ipcRenderer.invoke("aiwf:listWorkflowApps", req),
  runWorkflowApp: (req, cfg) => ipcRenderer.invoke("aiwf:runWorkflowApp", req, cfg),
  exportWorkflowPreflightReport: (req) => ipcRenderer.invoke("aiwf:exportWorkflowPreflightReport", req),
  exportWorkflowTemplateAcceptanceReport: (req) => ipcRenderer.invoke("aiwf:exportWorkflowTemplateAcceptanceReport", req),
  getWorkflowRunTimeline: (req) => ipcRenderer.invoke("aiwf:getWorkflowRunTimeline", req),
  getWorkflowFailureSummary: (req) => ipcRenderer.invoke("aiwf:getWorkflowFailureSummary", req),
  getWorkflowSandboxAlerts: (req) => ipcRenderer.invoke("aiwf:getWorkflowSandboxAlerts", req),
  getWorkflowSandboxAlertRules: () => ipcRenderer.invoke("aiwf:getWorkflowSandboxAlertRules"),
  setWorkflowSandboxAlertRules: (req) => ipcRenderer.invoke("aiwf:setWorkflowSandboxAlertRules", req),
  muteWorkflowSandboxAlert: (req) => ipcRenderer.invoke("aiwf:muteWorkflowSandboxAlert", req),
  listWorkflowSandboxRuleVersions: (req) => ipcRenderer.invoke("aiwf:listWorkflowSandboxRuleVersions", req),
  compareWorkflowSandboxRuleVersions: (req) => ipcRenderer.invoke("aiwf:compareWorkflowSandboxRuleVersions", req),
  rollbackWorkflowSandboxRuleVersion: (req) => ipcRenderer.invoke("aiwf:rollbackWorkflowSandboxRuleVersion", req),
  getWorkflowSandboxAutoFixState: () => ipcRenderer.invoke("aiwf:getWorkflowSandboxAutoFixState"),
  listWorkflowSandboxAutoFixActions: (req) => ipcRenderer.invoke("aiwf:listWorkflowSandboxAutoFixActions", req),
  exportWorkflowSandboxPreset: (req) => ipcRenderer.invoke("aiwf:exportWorkflowSandboxPreset", req),
  importWorkflowSandboxPreset: (req) => ipcRenderer.invoke("aiwf:importWorkflowSandboxPreset", req),
  listWorkflowQualityGateReports: (req) => ipcRenderer.invoke("aiwf:listWorkflowQualityGateReports", req),
  exportWorkflowQualityGateReports: (req) => ipcRenderer.invoke("aiwf:exportWorkflowQualityGateReports", req),
  exportWorkflowSandboxAuditReport: (req) => ipcRenderer.invoke("aiwf:exportWorkflowSandboxAuditReport", req),
  listWorkflowAuditLogs: (req) => ipcRenderer.invoke("aiwf:listWorkflowAuditLogs", req),
  replayWorkflowRun: (req, cfg) => ipcRenderer.invoke("aiwf:replayWorkflowRun", req, cfg),
  compareWorkflowRuns: (req) => ipcRenderer.invoke("aiwf:compareWorkflowRuns", req),
  getWorkflowLineage: (req) => ipcRenderer.invoke("aiwf:getWorkflowLineage", req),
  listRunBaselines: () => ipcRenderer.invoke("aiwf:listRunBaselines"),
  saveRunBaseline: (req) => ipcRenderer.invoke("aiwf:saveRunBaseline", req),
  compareRunWithBaseline: (req) => ipcRenderer.invoke("aiwf:compareRunWithBaseline", req),
  listTemplateMarketplace: (req) => ipcRenderer.invoke("aiwf:listTemplateMarketplace", req),
  installTemplatePack: (req) => ipcRenderer.invoke("aiwf:installTemplatePack", req),
  removeTemplatePack: (req) => ipcRenderer.invoke("aiwf:removeTemplatePack", req),
  exportTemplatePack: (req) => ipcRenderer.invoke("aiwf:exportTemplatePack", req),
  listQualityRuleSets: () => ipcRenderer.invoke("aiwf:listQualityRuleSets"),
  saveQualityRuleSet: (req) => ipcRenderer.invoke("aiwf:saveQualityRuleSet", req),
  removeQualityRuleSet: (req) => ipcRenderer.invoke("aiwf:removeQualityRuleSet", req),
  exportCompareReport: (req) => ipcRenderer.invoke("aiwf:exportCompareReport", req),
  listManualReviews: () => ipcRenderer.invoke("aiwf:listManualReviews"),
  listManualReviewHistory: (req) => ipcRenderer.invoke("aiwf:listManualReviewHistory", req),
  exportManualReviewHistory: (req) => ipcRenderer.invoke("aiwf:exportManualReviewHistory", req),
  submitManualReview: (req) => ipcRenderer.invoke("aiwf:submitManualReview", req),
  validateWorkflowDefinition: (req) => ipcRenderer.invoke("aiwf:validateWorkflowDefinition", req),
  getWorkflowDiagnostics: (opts) => ipcRenderer.invoke("aiwf:getWorkflowDiagnostics", opts),
  getWorkflowPerfDashboard: (opts) => ipcRenderer.invoke("aiwf:getWorkflowPerfDashboard", opts),
  saveWorkflow: (graph, name, opts) => ipcRenderer.invoke("aiwf:saveWorkflow", graph, name, opts),
  loadWorkflow: (opts) => ipcRenderer.invoke("aiwf:loadWorkflow", opts),
  openWorkflowStudio: () => ipcRenderer.invoke("aiwf:openWorkflowStudio"),
  openPath: (p) => ipcRenderer.invoke("aiwf:openPath", p),
  getLatestArtifactsDir: () => ipcRenderer.invoke("aiwf:getLatestArtifactsDir"),
  getSamplePoolInfo: (cfg) => ipcRenderer.invoke("aiwf:getSamplePoolInfo", cfg),
  samplePoolAddFiles: (paths, cfg) => ipcRenderer.invoke("aiwf:samplePoolAddFiles", paths, cfg),
  samplePoolClear: (cfg) => ipcRenderer.invoke("aiwf:samplePoolClear", cfg),
  logRouteMetrics: (payload) => ipcRenderer.invoke("aiwf:logRouteMetrics", payload),
  getRouteMetricsSummary: () => ipcRenderer.invoke("aiwf:getRouteMetricsSummary"),
  inspectEncoding: (paths) => ipcRenderer.invoke("aiwf:inspectEncoding", paths),
  normalizeEncoding: (paths) => ipcRenderer.invoke("aiwf:normalizeEncoding", paths),
  checkFonts: () => ipcRenderer.invoke("aiwf:checkFonts"),
  installBundledFonts: () => ipcRenderer.invoke("aiwf:installBundledFonts"),
  checkRuntime: () => ipcRenderer.invoke("aiwf:checkRuntime"),
  startupSelfCheck: (cfg) => ipcRenderer.invoke("aiwf:startupSelfCheck", cfg),
  getTaskStoreStatus: (cfg) => ipcRenderer.invoke("aiwf:getTaskStoreStatus", cfg),
  runLocalGateCheck: (req) => ipcRenderer.invoke("aiwf:runLocalGateCheck", req),
  getLocalGateSummary: (req) => ipcRenderer.invoke("aiwf:getLocalGateSummary", req),
  getLocalGateRuntime: () => ipcRenderer.invoke("aiwf:getLocalGateRuntime"),
  cancelLocalGateCheck: () => ipcRenderer.invoke("aiwf:cancelLocalGateCheck"),
  getBuildGuardStatus: () => ipcRenderer.invoke("aiwf:getBuildGuardStatus"),
  runLocalBuildScript: (req) => ipcRenderer.invoke("aiwf:runLocalBuildScript", req),
  getLocalBuildRuntime: () => ipcRenderer.invoke("aiwf:getLocalBuildRuntime"),
  cancelLocalBuildScript: () => ipcRenderer.invoke("aiwf:cancelLocalBuildScript"),
  exportReleaseReport: (req) => ipcRenderer.invoke("aiwf:exportReleaseReport", req),
  onLocalGateLog: (handler) => {
    const h = (_evt, payload) => {
      try { handler && handler(payload); } catch {}
    };
    ipcRenderer.on("aiwf:localGateLog", h);
    return () => ipcRenderer.removeListener("aiwf:localGateLog", h);
  },
  onLocalBuildLog: (handler) => {
    const h = (_evt, payload) => {
      try { handler && handler(payload); } catch {}
    };
    ipcRenderer.on("aiwf:localBuildLog", h);
    return () => ipcRenderer.removeListener("aiwf:localBuildLog", h);
  },
  getDroppedFilePath: (file) => {
    try {
      const p = webUtils.getPathForFile(file);
      if (p) return String(p);
    } catch {}
    try {
      if (file && typeof file.path === "string" && file.path.trim()) return String(file.path);
    } catch {}
    return "";
  },
});
