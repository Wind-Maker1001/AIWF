const { registerMetricsIpc } = require("./main_ipc_metrics");
const { registerWorkflowIpc } = require("./main_ipc_workflow");
const { registerRuntimeIpc } = require("./main_ipc_runtime");

function registerIpcHandlers(ctx) {
  const {
    app,
    ipcMain,
    shell,
    loadConfig,
    saveConfig,
    baseHealth,
    runOfflineCleaningInWorker,
    runViaBaseApi,
    path,
  } = ctx;

  ipcMain.handle("aiwf:getConfig", async () => loadConfig());
  ipcMain.handle("aiwf:saveConfig", async (_evt, cfg) => {
    saveConfig(cfg || {});
    return { ok: true };
  });

  ipcMain.handle("aiwf:health", async (_evt, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    if ((merged.mode || "offline_local") === "offline_local") {
      return { ok: true, mode: "offline_local", message: "离线本地模式可用" };
    }
    return await baseHealth(merged);
  });

  ipcMain.handle("aiwf:runCleaning", async (_evt, payload, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    if ((merged.mode || "offline_local") === "offline_local") {
      const outRoot = path.join(app.getPath("documents"), "AIWF-Offline");
      return await runOfflineCleaningInWorker(payload, outRoot);
    }
    return await runViaBaseApi(payload, merged);
  });

  ipcMain.handle("aiwf:openPath", async (_evt, p) => {
    if (!p) return { ok: false };
    await shell.openPath(String(p));
    return { ok: true };
  });

  registerMetricsIpc(ctx);
  registerWorkflowIpc(ctx);
  registerRuntimeIpc(ctx);
}

module.exports = {
  registerIpcHandlers,
};

