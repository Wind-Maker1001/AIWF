const { app, BrowserWindow, Menu, dialog, ipcMain, shell } = require("electron");
const path = require("path");
const fs = require("fs");
const { execFileSync, fork } = require("child_process");
const iconv = require("iconv-lite");
const { registerIpcHandlers } = require("./main_ipc");
const { createConfigSupport } = require("./main_config_support");
const { createRuntimeSupport } = require("./main_runtime_support");
const { createWindowSupport } = require("./main_window_support");

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

const { runMinimalWorkflow } = loadWorkflowEngine();

const config = createConfigSupport({ app, fs, path });
const runtime = createRuntimeSupport({ app, fs, path, execFileSync, fork, iconv });
const windows = createWindowSupport({ app, BrowserWindow, Menu, shell, path });

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
  runOfflineCleaningInWorker: runtime.runOfflineCleaningInWorker,
  runViaBaseApi: runtime.runViaBaseApi,
  routeMetricsLogPath: config.routeMetricsLogPath,
  routeMetricsSummaryPath: config.routeMetricsSummaryPath,
  rotateLogIfNeeded: config.rotateLogIfNeeded,
  createWorkflowWindow: windows.createWorkflowWindow,
  runMinimalWorkflow,
  inspectFileEncoding: runtime.inspectFileEncoding,
  toUtf8FileIfNeeded: runtime.toUtf8FileIfNeeded,
  checkChineseOfficeFonts: runtime.checkChineseOfficeFonts,
  installBundledFontsForCurrentUser: runtime.installBundledFontsForCurrentUser,
  checkTesseractRuntime: runtime.checkTesseractRuntime,
  checkTesseractLangs: runtime.checkTesseractLangs,
  checkPdftoppmRuntime: runtime.checkPdftoppmRuntime,
  getTaskStoreStatus: runtime.getTaskStoreStatus,
});

app.whenReady().then(() => windows.bootFromArgv(process.argv));
app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});
app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) windows.createWindow();
});

process.on("uncaughtException", (e) => {
  dialog.showErrorBox("AIWF Desktop Error", String(e));
});
