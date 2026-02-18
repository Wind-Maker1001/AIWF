const { contextBridge, ipcRenderer, webUtils } = require("electron");

contextBridge.exposeInMainWorld("aiwfDesktop", {
  getConfig: () => ipcRenderer.invoke("aiwf:getConfig"),
  saveConfig: (cfg) => ipcRenderer.invoke("aiwf:saveConfig", cfg),
  health: (cfg) => ipcRenderer.invoke("aiwf:health", cfg),
  runCleaning: (payload, cfg) => ipcRenderer.invoke("aiwf:runCleaning", payload, cfg),
  runWorkflow: (payload, cfg) => ipcRenderer.invoke("aiwf:runWorkflow", payload, cfg),
  getWorkflowDiagnostics: (opts) => ipcRenderer.invoke("aiwf:getWorkflowDiagnostics", opts),
  saveWorkflow: (graph, name, opts) => ipcRenderer.invoke("aiwf:saveWorkflow", graph, name, opts),
  loadWorkflow: (opts) => ipcRenderer.invoke("aiwf:loadWorkflow", opts),
  openWorkflowStudio: () => ipcRenderer.invoke("aiwf:openWorkflowStudio"),
  openPath: (p) => ipcRenderer.invoke("aiwf:openPath", p),
  logRouteMetrics: (payload) => ipcRenderer.invoke("aiwf:logRouteMetrics", payload),
  getRouteMetricsSummary: () => ipcRenderer.invoke("aiwf:getRouteMetricsSummary"),
  inspectEncoding: (paths) => ipcRenderer.invoke("aiwf:inspectEncoding", paths),
  normalizeEncoding: (paths) => ipcRenderer.invoke("aiwf:normalizeEncoding", paths),
  checkFonts: () => ipcRenderer.invoke("aiwf:checkFonts"),
  installBundledFonts: () => ipcRenderer.invoke("aiwf:installBundledFonts"),
  checkRuntime: () => ipcRenderer.invoke("aiwf:checkRuntime"),
  startupSelfCheck: (cfg) => ipcRenderer.invoke("aiwf:startupSelfCheck", cfg),
  getTaskStoreStatus: (cfg) => ipcRenderer.invoke("aiwf:getTaskStoreStatus", cfg),
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
