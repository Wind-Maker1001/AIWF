const { assertWorkflowContract } = require("./workflow_contract");

function registerWorkflowQueueAppsIpc(ctx, deps) {
  const {
    ipcMain,
    loadConfig,
    runMinimalWorkflow,
  } = ctx;
  const {
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
    enqueueReviews,
    cacheStats,
    clearNodeCache,
    nowIso,
    reportSupport,
    sandboxSupport,
    sandboxRuleStore,
    sandboxAutoFixStore,
    workflowAppRegistryStore,
    workflowVersionStore,
  } = deps;

  function queueConcurrency() {
    const n = Number(process.env.AIWF_WORKFLOW_QUEUE_CONCURRENCY || 2);
    return Number.isFinite(n) ? Math.max(1, Math.min(8, Math.floor(n))) : 2;
  }

  function queueTerminalStatus(out) {
    if (out?.ok) return "done";
    const status = String(out?.status || "").trim().toLowerCase();
    if (status) return status;
    return "failed";
  }

  function listQueueItems(limit = 200) {
    const items = loadWorkflowQueue();
    return items
      .slice()
      .sort((a, b) => Number(a.priority || 0) - Number(b.priority || 0) || String(a.created_at || "").localeCompare(String(b.created_at || "")))
      .slice(0, Math.max(1, Math.min(5000, Number(limit || 200))));
  }

  function queueTaskKey(task) {
    const workflowId = String(task?.payload?.workflow_id || "").trim();
    if (workflowId) return workflowId;
    const label = String(task?.label || "").trim();
    return label || "default";
  }

  async function drainQueue() {
    if (queueState.draining) return;
    if (queueState.control?.paused) return;
    queueState.draining = true;
    try {
      while (true) {
        if (queueState.running.size >= queueConcurrency()) break;
        if (queueState.control?.paused) break;
        const items = loadWorkflowQueue();
        const sorted = items
          .filter((item) => String(item.status || "queued") === "queued")
          .sort((a, b) => Number(a.priority || 0) - Number(b.priority || 0) || String(a.created_at || "").localeCompare(String(b.created_at || "")));
        const next = sorted.find((candidate) => {
          const key = queueTaskKey(candidate);
          const quota = Number(queueState.control?.quotas?.[key] || 0);
          if (!Number.isFinite(quota) || quota <= 0) return true;
          let runningCount = 0;
          for (const runningItem of queueState.running.values()) {
            if (queueTaskKey(runningItem.task || {}) === key) runningCount += 1;
          }
          return runningCount < quota;
        });
        if (!next) break;
        const itemId = String(next.task_id || "");
        if (!itemId) break;
        next.status = "running";
        next.started_at = nowIso();
        saveWorkflowQueue(items);
        const promise = (async () => {
          const merged = normalizeWorkflowConfig({ ...loadConfig(), ...(next.cfg || {}) });
          const rulesOut = await sandboxRuleStore.getRuntimeRules(merged);
          if (!rulesOut?.ok) throw new Error(String(rulesOut?.error || "sandbox rules unavailable"));
          const effectivePayload = await reportSupport.applyQualityRuleSetToPayload(
            await sandboxAutoFixStore.applyPayload(next.payload || {}, merged),
            merged
          );
          const out = sandboxSupport.attachQualityGate(await runMinimalWorkflow({
            payload: effectivePayload,
            config: merged,
            outputRoot: resolveOutputRoot(merged),
            nodeCache: createNodeCacheApi(),
          }), effectivePayload || {});
          appendDiagnostics(out);
          appendRunHistory(out, effectivePayload, merged);
          extractSandboxViolations(out).forEach((item) => sandboxSupport.appendSandboxViolationAudit(item, effectivePayload || {}, rulesOut.rules || {}));
          await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, merged);
          if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) {
            await enqueueReviews(out.pending_reviews, merged);
          }
          const latest = loadWorkflowQueue();
          const index = latest.findIndex((item) => String(item.task_id || "") === itemId);
          if (index >= 0) {
            latest[index].status = queueTerminalStatus(out);
            latest[index].finished_at = nowIso();
            latest[index].result = out;
            latest[index].run_id = String(out?.run_id || "");
            saveWorkflowQueue(latest);
          }
        })()
          .catch((error) => {
            const latest = loadWorkflowQueue();
            const index = latest.findIndex((item) => String(item.task_id || "") === itemId);
            if (index >= 0) {
              latest[index].status = "failed";
              latest[index].finished_at = nowIso();
              latest[index].error = String(error);
              saveWorkflowQueue(latest);
            }
          })
          .finally(() => {
            queueState.running.delete(itemId);
            setImmediate(() => {
              drainQueue().catch(() => {});
            });
          });
        queueState.running.set(itemId, { promise, task: next });
      }
    } finally {
      queueState.draining = false;
    }
  }

  function applyTemplateParams(value, params) {
    if (value === null || typeof value === "undefined") return value;
    if (typeof value === "string") {
      return value.replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_match, key) => {
        const nextValue = params && Object.prototype.hasOwnProperty.call(params, key) ? params[key] : "";
        return String(nextValue ?? "");
      });
    }
    if (Array.isArray(value)) return value.map((item) => applyTemplateParams(item, params));
    if (typeof value === "object") {
      const output = {};
      Object.keys(value).forEach((key) => {
        output[key] = applyTemplateParams(value[key], params);
      });
      return output;
    }
    return value;
  }

  ipcMain.handle("aiwf:enqueueWorkflowTask", async (_evt, req) => {
    try {
      const mergedCfg = normalizeWorkflowConfig({ ...loadConfig(), ...((req && req.cfg) || {}) });
      const item = {
        task_id: String((req && req.task_id) || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`),
        label: String((req && req.label) || "workflow_task"),
        payload: await reportSupport.applyQualityRuleSetToPayload((req && req.payload) || {}, mergedCfg),
        cfg: (req && req.cfg) || {},
        priority: Number((req && req.priority) || 100),
        status: "queued",
        created_at: nowIso(),
      };
      const items = loadWorkflowQueue();
      items.push(item);
      saveWorkflowQueue(items);
      appendAudit("queue_enqueue", { task_id: item.task_id, label: item.label });
      drainQueue().catch(() => {});
      return { ok: true, task: item };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:listWorkflowQueue", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    return { ok: true, items: listQueueItems(limit), control: queueState.control || defaultQueueControl() };
  });

  ipcMain.handle("aiwf:getWorkflowQueueControl", async () => {
    queueState.control = loadQueueControl();
    return { ok: true, control: queueState.control };
  });

  ipcMain.handle("aiwf:setWorkflowQueueControl", async (_evt, req) => {
    const base = loadQueueControl();
    const patch = req && typeof req === "object" ? req : {};
    const merged = normalizeQueueControl({
      paused: typeof patch.paused === "boolean" ? patch.paused : base.paused,
      quotas: patch.quotas && typeof patch.quotas === "object" ? patch.quotas : base.quotas,
    });
    queueState.control = merged;
    saveQueueControl(merged);
    appendAudit("queue_control", merged);
    if (!merged.paused) drainQueue().catch(() => {});
    return { ok: true, control: merged };
  });

  ipcMain.handle("aiwf:cancelWorkflowTask", async (_evt, req) => {
    try {
      const id = String(req?.task_id || "").trim();
      if (!id) return { ok: false, error: "task_id required" };
      const items = loadWorkflowQueue();
      const index = items.findIndex((item) => String(item.task_id || "") === id);
      if (index < 0) return { ok: false, error: "task not found" };
      const status = String(items[index].status || "").trim().toLowerCase();
      if (status === "running") return { ok: false, error: "task is running; cancel unsupported" };
      if (status !== "queued") return { ok: false, error: `task status is not cancellable: ${status || "unknown"}` };
      items[index].status = "canceled";
      items[index].finished_at = nowIso();
      saveWorkflowQueue(items);
      appendAudit("queue_cancel", { task_id: id });
      return { ok: true };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:retryWorkflowTask", async (_evt, req) => {
    try {
      const id = String(req?.task_id || "").trim();
      if (!id) return { ok: false, error: "task_id required" };
      const items = loadWorkflowQueue();
      const index = items.findIndex((item) => String(item.task_id || "") === id);
      if (index < 0) return { ok: false, error: "task not found" };
      const current = items[index];
      const retry = {
        ...current,
        task_id: `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`,
        status: "queued",
        created_at: nowIso(),
        started_at: null,
        finished_at: null,
        result: null,
        error: null,
      };
      items.push(retry);
      saveWorkflowQueue(items);
      appendAudit("queue_retry", { task_id: id, retry_task_id: retry.task_id });
      drainQueue().catch(() => {});
      return { ok: true, task: retry };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:listWorkflowVersions", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    const workflowName = String(req?.workflow_name || "");
    return await workflowVersionStore.listVersions(limit, workflowName);
  });

  ipcMain.handle("aiwf:restoreWorkflowVersion", async (_evt, req) => {
    try {
      const versionId = String(req?.version_id || "").trim();
      if (!versionId) return { ok: false, error: "version_id required" };
      const merged = normalizeWorkflowConfig({ ...loadConfig() });
      const hit = await workflowVersionStore.getVersion(versionId, merged);
      if (!hit) return { ok: false, error: "version not found" };
      appendAudit("workflow_restore_version", {
        version_id: versionId,
        workflow_name: String(hit.workflow_name || ""),
        provider: String(hit.provider || ""),
      });
      return { ok: true, graph: hit.graph, meta: hit };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:compareWorkflowVersions", async (_evt, req) => {
    try {
      const merged = normalizeWorkflowConfig({ ...loadConfig() });
      return await workflowVersionStore.compareVersions(
        String(req?.version_a || "").trim(),
        String(req?.version_b || "").trim(),
        merged
      );
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:getWorkflowNodeCacheStats", async () => {
    return { ok: true, stats: cacheStats() };
  });

  ipcMain.handle("aiwf:clearWorkflowNodeCache", async () => {
    clearNodeCache();
    appendAudit("cache_clear", {});
    return { ok: true, stats: cacheStats() };
  });

  ipcMain.handle("aiwf:publishWorkflowApp", async (_evt, req) => {
    const graph = req?.graph && typeof req.graph === "object" ? req.graph : null;
    const name = String(req?.name || graph?.name || "").trim();
    if (!graph) return { ok: false, error: "graph required" };
    if (!name) return { ok: false, error: "name required" };
    try {
      assertWorkflowContract(graph, { requireNonEmptyNodes: true });
    } catch (error) {
      return { ok: false, error: String(error) };
    }
    const out = await workflowAppRegistryStore.publishApp({
      app_id: String(req?.app_id || "").trim() || undefined,
      name,
      workflow_id: String(graph?.workflow_id || "custom"),
      params_schema: req?.params_schema && typeof req.params_schema === "object" ? req.params_schema : {},
      template_policy: req?.template_policy && typeof req.template_policy === "object" ? req.template_policy : {},
      graph,
    });
    if (out?.ok) {
      appendAudit("workflow_publish_app", {
        app_id: String(out?.item?.app_id || ""),
        name: String(out?.item?.name || ""),
        provider: String(out?.provider || ""),
      });
    }
    return out;
  });

  ipcMain.handle("aiwf:listWorkflowApps", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    return await workflowAppRegistryStore.listApps(limit);
  });

  ipcMain.handle("aiwf:runWorkflowApp", async (_evt, req, cfg) => {
    try {
      const appId = String(req?.app_id || "").trim();
      if (!appId) return { ok: false, error: "app_id required" };
      const merged = normalizeWorkflowConfig({ ...loadConfig(), ...(cfg || {}), ...(req?.cfg || {}) });
      const item = await workflowAppRegistryStore.getApp(appId, merged);
      if (!item) return { ok: false, error: "workflow app not found" };
      const params = req?.params && typeof req.params === "object" ? req.params : {};
      const mergedPayload = req?.payload && typeof req.payload === "object" ? { ...req.payload } : {};
      mergedPayload.workflow = applyTemplateParams(item.graph || {}, params);
      if (!mergedPayload.workflow.workflow_id) mergedPayload.workflow.workflow_id = item.workflow_id || "custom";
      const rulesOut = await sandboxRuleStore.getRuntimeRules(merged);
      if (!rulesOut?.ok) return rulesOut;
      const effectivePayload = await reportSupport.applyQualityRuleSetToPayload(
        await sandboxAutoFixStore.applyPayload(mergedPayload, merged),
        merged
      );
      const out = sandboxSupport.attachQualityGate(await runMinimalWorkflow({
        payload: effectivePayload,
        config: merged,
        outputRoot: resolveOutputRoot(merged),
        nodeCache: createNodeCacheApi(),
      }), effectivePayload || {});
      appendDiagnostics(out);
      appendRunHistory(out, effectivePayload, merged);
      extractSandboxViolations(out).forEach((violation) => sandboxSupport.appendSandboxViolationAudit(violation, effectivePayload || {}, rulesOut.rules || {}));
      await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, merged);
      if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) {
        await enqueueReviews(out.pending_reviews, merged);
      }
      appendAudit("workflow_run_app", {
        app_id: appId,
        run_id: String(out?.run_id || ""),
        ok: !!out?.ok,
        provider: String(item?.provider || ""),
      });
      return { ok: true, app_id: appId, provider: String(item?.provider || ""), result: out };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });
}

module.exports = {
  registerWorkflowQueueAppsIpc,
};
