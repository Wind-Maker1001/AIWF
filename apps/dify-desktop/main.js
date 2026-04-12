const { app, BrowserWindow, Menu, dialog, ipcMain, shell } = require("electron");
const path = require("path");
const fs = require("fs");

let services = null;

function loadWorkflowEngine() {
  const candidates = [
    "./workflow_engine.js",
    path.join(__dirname, "workflow_engine.js"),
  ];
  const errors = [];
  for (const mod of candidates) {
    try {
      return require(mod);
    } catch (e) {
      errors.push(`${mod}: ${String(e)}`);
    }
  }
  throw new Error(`Failed to load workflow_engine.js\n${errors.join("\n")}`);
}

function listCleaningTemplatesProxy(...args) {
  const { listCleaningTemplates } = require("./offline_engine");
  return listCleaningTemplates(...args);
}

function runMinimalWorkflowProxy(...args) {
  return loadWorkflowEngine().runMinimalWorkflow(...args);
}

function writeBootMarker(extra = {}) {
  const markerPath = String(process.env.AIWF_BOOT_MARKER_PATH || "").trim();
  if (!markerPath) return;
  try {
    fs.mkdirSync(path.dirname(markerPath), { recursive: true });
    fs.writeFileSync(markerPath, `${JSON.stringify({
      pid: process.pid,
      ts: new Date().toISOString(),
      argv: process.argv.slice(1),
      ...extra,
    })}\n`, "utf8");
  } catch {}
}

process.on("uncaughtException", (e) => {
  writeBootMarker({ stage: "uncaught_exception", error: String(e && e.stack ? e.stack : e) });
  dialog.showErrorBox("AIWF Desktop Error", String(e));
});

function initializeServices() {
  if (services) return services;

  const { execFileSync, fork } = require("child_process");
  const iconv = require("iconv-lite");
  const { registerIpcHandlers } = require("./main_ipc");
  const { createConfigSupport } = require("./main_config_support");
  const { createRuntimeSupport } = require("./main_runtime_support");
  const { createWindowSupport } = require("./main_window_support");

  const config = createConfigSupport({ app, fs, path });
  const runtime = createRuntimeSupport({ app, fs, path, execFileSync, fork, iconv });
  const windows = createWindowSupport({ app, BrowserWindow, Menu, shell, path, loadConfig: config.loadConfig });

  registerIpcHandlers({
    app,
    ipcMain,
    shell,
    dialog,
    fs,
    path,
    loadConfig: config.loadConfig,
    saveConfig: config.saveConfig,
    baseHealth: runtime.baseHealth,
    glueHealth: runtime.glueHealth,
    runOfflineCleaningInWorker: runtime.runOfflineCleaningInWorker,
    runOfflinePrecheckInWorker: runtime.runOfflinePrecheckInWorker,
    runOfflinePreviewInWorker: runtime.runOfflinePreviewInWorker,
    runPrecheckViaGlue: runtime.runPrecheckViaGlue,
    runViaBaseApi: runtime.runViaBaseApi,
    listCleaningTemplates: (...args) => listCleaningTemplatesProxy(...args),
    routeMetricsLogPath: config.routeMetricsLogPath,
    routeMetricsSummaryPath: config.routeMetricsSummaryPath,
    runModeAuditLogPath: config.runModeAuditLogPath,
    rotateLogIfNeeded: config.rotateLogIfNeeded,
    createWorkflowWindow: windows.createWorkflowWindow,
    runMinimalWorkflow: (...args) => runMinimalWorkflowProxy(...args),
    inspectFileEncoding: runtime.inspectFileEncoding,
    toUtf8FileIfNeeded: runtime.toUtf8FileIfNeeded,
    checkChineseOfficeFonts: runtime.checkChineseOfficeFonts,
    installBundledFontsForCurrentUser: runtime.installBundledFontsForCurrentUser,
    checkTesseractRuntime: runtime.checkTesseractRuntime,
    checkTesseractLangs: runtime.checkTesseractLangs,
    checkPdftoppmRuntime: runtime.checkPdftoppmRuntime,
    getTaskStoreStatus: runtime.getTaskStoreStatus,
  });

  services = { config, runtime, windows };
  return services;
}

writeBootMarker({ stage: "process_boot" });

app.whenReady().then(() => {
  writeBootMarker({ stage: "app_ready" });
  const current = initializeServices();
  current.windows.bootFromArgv(process.argv);
  writeBootMarker({ stage: "bootstrapped" });
}).catch((e) => {
  writeBootMarker({ stage: "boot_failed", error: String(e && e.stack ? e.stack : e) });
  throw e;
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) initializeServices().windows.createWindow();
});
