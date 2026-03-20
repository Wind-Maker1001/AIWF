function registerCleaningIpc(ctx, deps) {
  const {
    ipcMain,
    loadConfig,
    saveConfig,
    baseHealth,
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
    shouldFallbackByPolicy,
  } = deps;

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
    const outRoot = resolveOutputRoot(merged);
    const startedAt = Date.now();
    if ((merged.mode || "offline_local") === "offline_local") {
      const local = await runOfflineCleaningInWorker(payload, outRoot);
      appendRunModeAudit({
        ts: new Date().toISOString(),
        mode: "offline_local",
        fallback_applied: false,
        ok: !!(local && local.ok),
        job_id: local && local.job_id ? String(local.job_id) : "",
        duration_ms: Date.now() - startedAt,
      });
      return local;
    }
    const allowFallback = merged.enableOfflineFallback !== false;
    const fallbackPolicy = String(merged.fallbackPolicy || "smart").trim().toLowerCase() || "smart";
    try {
      const remote = await runViaBaseApi(payload, merged);
      if (remote && remote.ok) {
        appendRunModeAudit({
          ts: new Date().toISOString(),
          mode: "base_api",
          fallback_applied: false,
          ok: true,
          job_id: remote && remote.job_id ? String(remote.job_id) : "",
          duration_ms: Date.now() - startedAt,
        });
        return remote;
      }
      const failureClass = classifyRemoteFailure(null, remote);
      if (!allowFallback || !shouldFallbackByPolicy(fallbackPolicy, failureClass)) {
        appendRunModeAudit({
          ts: new Date().toISOString(),
          mode: "base_api",
          fallback_applied: false,
          ok: !!(remote && remote.ok),
          reason: "remote_returned_not_ok",
          failure_class: failureClass,
          fallback_policy: fallbackPolicy,
          job_id: remote && remote.job_id ? String(remote.job_id) : "",
          duration_ms: Date.now() - startedAt,
        });
        return remote;
      }
      const local = await runOfflineCleaningInWorker(payload, outRoot);
      const out = {
        ...(local || {}),
        fallback_applied: true,
        fallback_reason: "remote_returned_not_ok",
        fallback_policy: fallbackPolicy,
        failure_class: failureClass,
        fallback_message: "后端返回失败，已自动切换到离线本地模式完成清洗。",
        remote_result: remote || null,
      };
      appendRunModeAudit({
        ts: new Date().toISOString(),
        mode: "base_api",
        fallback_applied: true,
        ok: !!(out && out.ok),
        reason: "remote_returned_not_ok",
        failure_class: failureClass,
        fallback_policy: fallbackPolicy,
        job_id: out && out.job_id ? String(out.job_id) : "",
        duration_ms: Date.now() - startedAt,
      });
      return out;
    } catch (error) {
      const failureClass = classifyRemoteFailure(error, null);
      if (!allowFallback || !shouldFallbackByPolicy(fallbackPolicy, failureClass)) throw error;
      const local = await runOfflineCleaningInWorker(payload, outRoot);
      const out = {
        ...(local || {}),
        fallback_applied: true,
        fallback_reason: "remote_request_failed",
        fallback_policy: fallbackPolicy,
        failure_class: failureClass,
        fallback_message: "后端不可用，已自动切换到离线本地模式完成清洗。",
        remote_error: String(error && error.message ? error.message : error),
      };
      appendRunModeAudit({
        ts: new Date().toISOString(),
        mode: "base_api",
        fallback_applied: true,
        ok: !!(out && out.ok),
        reason: "remote_request_failed",
        failure_class: failureClass,
        fallback_policy: fallbackPolicy,
        remote_error: String(error && error.message ? error.message : error),
        job_id: out && out.job_id ? String(out.job_id) : "",
        duration_ms: Date.now() - startedAt,
      });
      return out;
    }
  });

  ipcMain.handle("aiwf:precheckCleaning", async (_evt, payload, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    if ((merged.mode || "offline_local") === "offline_local") {
      const outRoot = resolveOutputRoot(merged);
      return await runOfflinePrecheckInWorker(payload, outRoot);
    }
    return { ok: false, error: "当前仅离线本地模式支持模板预检" };
  });

  ipcMain.handle("aiwf:previewDebateStyle", async (_evt, payload, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    if ((merged.mode || "offline_local") === "offline_local") {
      const outRoot = resolveOutputRoot(merged);
      return await runOfflinePreviewInWorker(payload, outRoot);
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
