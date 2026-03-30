function registerCleaningIpc(ctx, deps) {
  const {
    ipcMain,
    loadConfig,
    saveConfig,
    baseHealth,
    glueHealth,
    runOfflineCleaningInWorker,
    runOfflinePrecheckInWorker,
    runOfflinePreviewInWorker,
    runViaBaseApi,
    listCleaningTemplates,
  } = ctx;

  const {
    resolveOutputRoot,
    appendRunModeAudit,
    classifyRemoteFailure,
  } = deps;

  ipcMain.handle("aiwf:getConfig", async () => loadConfig());

  ipcMain.handle("aiwf:saveConfig", async (_evt, cfg) => {
    saveConfig(cfg || {});
    return { ok: true };
  });

  ipcMain.handle("aiwf:health", async (_evt, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    if ((merged.mode || "offline_local") === "offline_local") {
      const glue = await glueHealth(merged);
      return {
        ok: true,
        mode: "offline_local",
        message: glue.ok ? "离线本地模式可用" : "离线本地模式可用（图片/XLSX 需要 glue sidecar）",
        glue_sidecar: glue,
      };
    }
    return await baseHealth(merged);
  });

  ipcMain.handle("aiwf:runCleaning", async (_evt, payload, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    const outRoot = resolveOutputRoot(merged);
    const startedAt = Date.now();
    if ((merged.mode || "offline_local") === "offline_local") {
      const local = await runOfflineCleaningInWorker(payload, outRoot, merged);
      appendRunModeAudit({
        ts: new Date().toISOString(),
        mode: "offline_local",
        ok: !!(local && local.ok),
        job_id: local && local.job_id ? String(local.job_id) : "",
        duration_ms: Date.now() - startedAt,
      });
      return local;
    }
    try {
      const remote = await runViaBaseApi(payload, merged);
      if (remote && remote.ok) {
        appendRunModeAudit({
          ts: new Date().toISOString(),
          mode: "base_api",
          ok: true,
          job_id: remote && remote.job_id ? String(remote.job_id) : "",
          duration_ms: Date.now() - startedAt,
        });
        return remote;
      }
      const failureClass = classifyRemoteFailure(null, remote);
      appendRunModeAudit({
        ts: new Date().toISOString(),
        mode: "base_api",
        ok: !!(remote && remote.ok),
        reason: "remote_returned_not_ok",
        failure_class: failureClass,
        job_id: remote && remote.job_id ? String(remote.job_id) : "",
        duration_ms: Date.now() - startedAt,
      });
      return remote;
    } catch (error) {
      const failureClass = classifyRemoteFailure(error, null);
      appendRunModeAudit({
        ts: new Date().toISOString(),
        mode: "base_api",
        ok: false,
        reason: "remote_request_failed",
        failure_class: failureClass,
        remote_error: String(error && error.message ? error.message : error),
        duration_ms: Date.now() - startedAt,
      });
      throw error;
    }
  });

  ipcMain.handle("aiwf:precheckCleaning", async (_evt, payload, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    if ((merged.mode || "offline_local") === "offline_local") {
      const outRoot = resolveOutputRoot(merged);
      return await runOfflinePrecheckInWorker(payload, outRoot, merged);
    }
    return { ok: false, error: "当前仅离线本地模式支持模板预检" };
  });

  ipcMain.handle("aiwf:previewDebateStyle", async (_evt, payload, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    if ((merged.mode || "offline_local") === "offline_local") {
      const outRoot = resolveOutputRoot(merged);
      return await runOfflinePreviewInWorker(payload, outRoot, merged);
    }
    return { ok: false, error: "当前仅离线本地模式支持样式预览" };
  });

  ipcMain.handle("aiwf:listCleaningTemplates", async () => {
    try {
      return listCleaningTemplates();
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });
}

module.exports = {
  registerCleaningIpc,
};
