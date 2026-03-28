const {
  assertWorkflowContract,
  NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY,
  WORKFLOW_CONTRACT_AUTHORITY,
} = require("./workflow_contract");
const { workflowStoreRemoteErrorResult } = require("./workflow_store_remote_error");

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

  function workflowContractFailure(error) {
    const details = error && typeof error === "object" && error.details && typeof error.details === "object"
      ? error.details
      : {};
    return {
      ok: false,
      error: String(error?.message || error || "workflow contract invalid"),
      error_code: String(error?.code || "workflow_contract_invalid"),
      graph_contract: String(details.graph_contract || WORKFLOW_CONTRACT_AUTHORITY),
      error_item_contract: String(details.error_item_contract || NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY),
      error_items: Array.isArray(details.error_items) ? details.error_items : [],
    };
  }

  function normalizeWorkflowStoreError(error) {
    return workflowStoreRemoteErrorResult(error);
  }

  function buildPublishedVersionId(graph, req) {
    const explicit = String(req?.published_version_id || req?.version_id || "").trim();
    if (explicit) return explicit;
    const workflowId = String(graph?.workflow_id || "workflow").trim().replace(/[^a-zA-Z0-9_.-]+/g, "_").slice(0, 80) || "workflow";
    return `${workflowId}_published_${Date.now()}_${Math.random().toString(16).slice(2, 10)}`;
  }

  function applyPendingReviewEnqueueResult(out, enqueueOut) {
    if (!enqueueOut?.ok) {
      const reviewEnqueue = {
        ok: false,
        error: String(enqueueOut?.error || "manual review enqueue failed"),
        error_code: String(enqueueOut?.error_code || "manual_review_enqueue_failed"),
        error_item_contract: String(enqueueOut?.error_item_contract || ""),
        graph_contract: String(enqueueOut?.graph_contract || ""),
        error_items: Array.isArray(enqueueOut?.error_items) ? enqueueOut.error_items : [],
      };
      return {
        ...(out && typeof out === "object" ? out : {}),
        review_enqueue_failed: true,
        review_enqueue: reviewEnqueue,
      };
    }
    return out;
  }

  function normalizePendingReviews(items, out, payload = null) {
    const workflow = payload?.workflow && typeof payload.workflow === "object" ? payload.workflow : {};
    const fallbackRunId = String(out?.run_id || "").trim();
    const fallbackWorkflowId = String(out?.workflow_id || workflow.workflow_id || "").trim();
    return (Array.isArray(items) ? items : []).map((item) => {
      const source = item && typeof item === "object" ? item : {};
      const reviewKey = String(source.review_key || source.node_id || "").trim();
      return {
        ...source,
        run_id: String(source.run_id || fallbackRunId).trim(),
        workflow_id: String(source.workflow_id || fallbackWorkflowId).trim(),
        node_id: String(source.node_id || reviewKey).trim(),
        review_key: reviewKey,
        status: String(source.status || "pending").trim().toLowerCase() || "pending",
      };
    });
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
          if (!rulesOut?.ok) throw rulesOut;
          const effectivePayload = await reportSupport.applyQualityRuleSetToPayload(
            await sandboxAutoFixStore.applyPayload(next.payload || {}, merged),
            merged
          );
          let out = sandboxSupport.attachQualityGate(await runMinimalWorkflow({
            payload: effectivePayload,
            config: merged,
            outputRoot: resolveOutputRoot(merged),
            nodeCache: createNodeCacheApi(),
          }), effectivePayload || {});
          if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) {
            const enqueueOut = await enqueueReviews(normalizePendingReviews(out.pending_reviews, out, effectivePayload), merged);
            out = applyPendingReviewEnqueueResult(out, enqueueOut);
          }
          appendDiagnostics(out);
          appendRunHistory(out, effectivePayload, merged);
          extractSandboxViolations(out).forEach((item) => sandboxSupport.appendSandboxViolationAudit(item, effectivePayload || {}, rulesOut.rules || {}));
          const latest = loadWorkflowQueue();
          const index = latest.findIndex((item) => String(item.task_id || "") === itemId);
          if (index >= 0) {
            latest[index].status = queueTerminalStatus(out);
            latest[index].finished_at = nowIso();
            latest[index].result = out;
            latest[index].run_id = String(out?.run_id || "");
            saveWorkflowQueue(latest);
          }
          if (out?.review_enqueue_failed) return;
          await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, merged);
        })()
          .catch((error) => {
            const latest = loadWorkflowQueue();
            const index = latest.findIndex((item) => String(item.task_id || "") === itemId);
            if (index >= 0) {
              const normalizedError = normalizeWorkflowStoreError(error);
              latest[index].status = "failed";
              latest[index].finished_at = nowIso();
              latest[index].error = String(normalizedError?.error || error);
              latest[index].result = normalizedError;
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
      return normalizeWorkflowStoreError(error);
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
      return normalizeWorkflowStoreError(error);
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
      return workflowContractFailure(error);
    }
    const merged = normalizeWorkflowConfig({ ...loadConfig() });
    const versionOut = await workflowVersionStore.recordVersion({
      version_id: buildPublishedVersionId(graph, req),
      workflow_id: String(graph?.workflow_id || "custom"),
      workflow_name: name,
      graph,
    }, merged);
    if (!versionOut?.ok) return versionOut;
    const publishedVersionId = String(versionOut?.item?.version_id || "").trim();
    if (!publishedVersionId) {
      return {
        ok: false,
        error: "published version id missing after snapshot record",
        error_code: "workflow_version_snapshot_missing",
      };
    }
    const out = await workflowAppRegistryStore.publishApp({
      app_id: String(req?.app_id || "").trim() || undefined,
      name,
      workflow_id: String(graph?.workflow_id || "custom"),
      published_version_id: publishedVersionId,
      params_schema: req?.params_schema && typeof req.params_schema === "object" ? req.params_schema : {},
      template_policy: req?.template_policy && typeof req.template_policy === "object" ? req.template_policy : {},
    }, merged);
    if (out?.ok) {
      appendAudit("workflow_publish_app", {
        app_id: String(out?.item?.app_id || ""),
        name: String(out?.item?.name || ""),
        provider: String(out?.provider || ""),
        published_version_id: publishedVersionId,
      });
      return {
        ...out,
        published_version_id: publishedVersionId,
        published_version: versionOut?.item || null,
      };
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
      let versionItem = null;
      const publishedVersionId = String(item?.published_version_id || "").trim();
      if (publishedVersionId) {
        versionItem = await workflowVersionStore.getVersion(publishedVersionId, merged);
        if (!versionItem?.graph || typeof versionItem.graph !== "object") {
          return {
            ok: false,
            error: `workflow version not found for app: ${publishedVersionId}`,
            error_code: "workflow_app_version_missing",
            app_id: appId,
            published_version_id: publishedVersionId,
          };
        }
      }
      const params = req?.params && typeof req.params === "object" ? req.params : {};
      const mergedPayload = req?.payload && typeof req.payload === "object" ? { ...req.payload } : {};
      const sourceGraph = versionItem?.graph && typeof versionItem.graph === "object"
        ? versionItem.graph
        : (item.graph && typeof item.graph === "object" ? item.graph : {});
      mergedPayload.workflow = applyTemplateParams(sourceGraph, params);
      if (!mergedPayload.workflow.workflow_id) mergedPayload.workflow.workflow_id = item.workflow_id || "custom";
      const rulesOut = await sandboxRuleStore.getRuntimeRules(merged);
      if (!rulesOut?.ok) return rulesOut;
      const effectivePayload = await reportSupport.applyQualityRuleSetToPayload(
        await sandboxAutoFixStore.applyPayload(mergedPayload, merged),
        merged
      );
      let out = sandboxSupport.attachQualityGate(await runMinimalWorkflow({
        payload: effectivePayload,
        config: merged,
        outputRoot: resolveOutputRoot(merged),
        nodeCache: createNodeCacheApi(),
      }), effectivePayload || {});
      if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) {
        const enqueueOut = await enqueueReviews(normalizePendingReviews(out.pending_reviews, out, effectivePayload), merged);
        out = applyPendingReviewEnqueueResult(out, enqueueOut);
      }
      appendDiagnostics(out);
      appendRunHistory(out, effectivePayload, merged);
      extractSandboxViolations(out).forEach((violation) => sandboxSupport.appendSandboxViolationAudit(violation, effectivePayload || {}, rulesOut.rules || {}));
      if (!out?.review_enqueue_failed) {
        await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, merged);
      }
        appendAudit("workflow_run_app", {
          app_id: appId,
          run_id: String(out?.run_id || ""),
          ok: !!out?.ok,
          provider: String(item?.provider || ""),
          published_version_id: publishedVersionId,
        });
      return {
        ok: !!out?.ok,
        app_id: appId,
        provider: String(item?.provider || ""),
        published_version_id: publishedVersionId,
        result: out,
        run_id: String(out?.run_id || ""),
        status: String(out?.status || ""),
        error: String(out?.error || ""),
        error_code: String(out?.error_code || ""),
        error_item_contract: String(out?.error_item_contract || ""),
        error_items: Array.isArray(out?.error_items) ? out.error_items : [],
        review_enqueue_failed: !!out?.review_enqueue_failed,
        review_enqueue: out?.review_enqueue && typeof out.review_enqueue === "object" ? out.review_enqueue : null,
      };
    } catch (error) {
      return normalizeWorkflowStoreError(error);
    }
  });
}

module.exports = {
  registerWorkflowQueueAppsIpc,
};
