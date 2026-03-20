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
    listWorkflowVersions,
    listWorkflowApps,
    saveWorkflowApps,
    cacheStats,
    clearNodeCache,
    nowIso,
    reportSupport,
    sandboxSupport,
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
          const effectivePayload = reportSupport.applyQualityRuleSetToPayload(sandboxSupport.applySandboxAutoFixPayload(next.payload || {}));
          const out = sandboxSupport.attachQualityGate(await runMinimalWorkflow({
            payload: effectivePayload,
            config: merged,
            outputRoot: resolveOutputRoot(merged),
            nodeCache: createNodeCacheApi(),
          }), effectivePayload || {});
          appendDiagnostics(out);
          appendRunHistory(out, effectivePayload, merged);
          extractSandboxViolations(out).forEach((item) => sandboxSupport.appendSandboxViolationAudit(item, effectivePayload || {}));
          sandboxSupport.maybeApplySandboxAutoFix(out, effectivePayload || {});
          if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) enqueueReviews(out.pending_reviews);
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

  function compareVersions(versionA, versionB) {
    const items = listWorkflowVersions(5000, "");
    const a = items.find((item) => String(item.version_id || "") === String(versionA || ""));
    const b = items.find((item) => String(item.version_id || "") === String(versionB || ""));
    if (!a || !b) return { ok: false, error: "version not found" };
    const graphA = a.graph && typeof a.graph === "object" ? a.graph : {};
    const graphB = b.graph && typeof b.graph === "object" ? b.graph : {};
    const nodesA = Array.isArray(graphA.nodes) ? graphA.nodes : [];
    const nodesB = Array.isArray(graphB.nodes) ? graphB.nodes : [];
    const edgesA = Array.isArray(graphA.edges) ? graphA.edges : [];
    const edgesB = Array.isArray(graphB.edges) ? graphB.edges : [];
    const mapA = new Map(nodesA.map((node) => [String(node.id || ""), node]));
    const mapB = new Map(nodesB.map((node) => [String(node.id || ""), node]));
    const allNodeIds = Array.from(new Set([...mapA.keys(), ...mapB.keys()]));
    const nodeDiff = allNodeIds.map((id) => {
      const nodeA = mapA.get(id);
      const nodeB = mapB.get(id);
      if (!nodeA) return { id, change: "added", type_a: "", type_b: String(nodeB?.type || "") };
      if (!nodeB) return { id, change: "removed", type_a: String(nodeA?.type || ""), type_b: "" };
      const typeChanged = String(nodeA?.type || "") !== String(nodeB?.type || "");
      const configChanged = JSON.stringify(nodeA?.config || {}) !== JSON.stringify(nodeB?.config || {});
      return {
        id,
        change: typeChanged || configChanged ? "updated" : "same",
        type_a: String(nodeA?.type || ""),
        type_b: String(nodeB?.type || ""),
        type_changed: typeChanged,
        config_changed: configChanged,
      };
    });
    const edgeKey = (edge) => `${String(edge?.from || "")}->${String(edge?.to || "")}:${JSON.stringify(edge?.when ?? null)}`;
    const setA = new Set(edgesA.map(edgeKey));
    const setB = new Set(edgesB.map(edgeKey));
    const addedEdges = Array.from(setB).filter((item) => !setA.has(item));
    const removedEdges = Array.from(setA).filter((item) => !setB.has(item));
    const changedNodes = nodeDiff.filter((item) => item.change !== "same").length;
    return {
      ok: true,
      summary: {
        version_a: String(versionA || ""),
        version_b: String(versionB || ""),
        nodes_a: nodesA.length,
        nodes_b: nodesB.length,
        edges_a: edgesA.length,
        edges_b: edgesB.length,
        changed_nodes: changedNodes,
        added_edges: addedEdges.length,
        removed_edges: removedEdges.length,
      },
      node_diff: nodeDiff,
      added_edges: addedEdges,
      removed_edges: removedEdges,
    };
  }

  ipcMain.handle("aiwf:enqueueWorkflowTask", async (_evt, req) => {
    try {
      const item = {
        task_id: String((req && req.task_id) || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`),
        label: String((req && req.label) || "workflow_task"),
        payload: reportSupport.applyQualityRuleSetToPayload((req && req.payload) || {}),
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
    return { ok: true, items: listWorkflowVersions(limit, workflowName) };
  });

  ipcMain.handle("aiwf:restoreWorkflowVersion", async (_evt, req) => {
    try {
      const versionId = String(req?.version_id || "").trim();
      if (!versionId) return { ok: false, error: "version_id required" };
      const items = listWorkflowVersions(5000, "");
      const hit = items.find((item) => String(item.version_id || "") === versionId);
      if (!hit) return { ok: false, error: "version not found" };
      appendAudit("workflow_restore_version", { version_id: versionId, workflow_name: String(hit.workflow_name || "") });
      return { ok: true, graph: hit.graph, meta: hit };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:compareWorkflowVersions", async (_evt, req) => {
    try {
      return compareVersions(String(req?.version_a || "").trim(), String(req?.version_b || "").trim());
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
    try {
      const graph = req?.graph && typeof req.graph === "object" ? req.graph : null;
      const name = String(req?.name || graph?.name || "").trim();
      if (!graph) return { ok: false, error: "graph required" };
      if (!name) return { ok: false, error: "name required" };
      const schema = req?.params_schema && typeof req.params_schema === "object" ? req.params_schema : {};
      const list = listWorkflowApps(5000);
      const appId = String(req?.app_id || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`);
      const item = {
        app_id: appId,
        name,
        workflow_id: String(graph?.workflow_id || "custom"),
        params_schema: schema,
        graph,
        created_at: nowIso(),
        updated_at: nowIso(),
      };
      const index = list.findIndex((appItem) => String(appItem.app_id || "") === appId);
      if (index >= 0) list[index] = { ...list[index], ...item };
      else list.unshift(item);
      saveWorkflowApps(list);
      appendAudit("workflow_publish_app", { app_id: appId, name });
      return { ok: true, item };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:listWorkflowApps", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    return { ok: true, items: listWorkflowApps(limit) };
  });

  ipcMain.handle("aiwf:runWorkflowApp", async (_evt, req, cfg) => {
    try {
      const appId = String(req?.app_id || "").trim();
      if (!appId) return { ok: false, error: "app_id required" };
      const item = listWorkflowApps(5000).find((appItem) => String(appItem.app_id || "") === appId);
      if (!item) return { ok: false, error: "workflow app not found" };
      const params = req?.params && typeof req.params === "object" ? req.params : {};
      const mergedPayload = req?.payload && typeof req.payload === "object" ? { ...req.payload } : {};
      mergedPayload.workflow = applyTemplateParams(item.graph || {}, params);
      if (!mergedPayload.workflow.workflow_id) mergedPayload.workflow.workflow_id = item.workflow_id || "custom";
      const merged = normalizeWorkflowConfig({ ...loadConfig(), ...(cfg || {}), ...(req?.cfg || {}) });
      const effectivePayload = reportSupport.applyQualityRuleSetToPayload(sandboxSupport.applySandboxAutoFixPayload(mergedPayload));
      const out = sandboxSupport.attachQualityGate(await runMinimalWorkflow({
        payload: effectivePayload,
        config: merged,
        outputRoot: resolveOutputRoot(merged),
        nodeCache: createNodeCacheApi(),
      }), effectivePayload || {});
      appendDiagnostics(out);
      appendRunHistory(out, effectivePayload, merged);
      extractSandboxViolations(out).forEach((violation) => sandboxSupport.appendSandboxViolationAudit(violation, effectivePayload || {}));
      sandboxSupport.maybeApplySandboxAutoFix(out, effectivePayload || {});
      if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) enqueueReviews(out.pending_reviews);
      appendAudit("workflow_run_app", { app_id: appId, run_id: String(out?.run_id || ""), ok: !!out?.ok });
      return { ok: true, app_id: appId, result: out };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });
}

module.exports = {
  registerWorkflowQueueAppsIpc,
};
