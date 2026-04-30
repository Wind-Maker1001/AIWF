const {
  NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY,
  WORKFLOW_CONTRACT_AUTHORITY,
} = require("./workflow_contract");
const {
  createWorkflowStoreRemoteError,
  workflowStoreRemoteErrorResult,
} = require("./workflow_store_remote_error");
const {
  normalizeWorkflowPayloadShape,
  resolveWorkflowDefinitionPayload,
} = require("./workflow_graph");

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
    workflowExecutionSupport = null,
    workflowValidationSupport = {
      validateWorkflowDefinitionAuthoritatively: async ({ workflowDefinition }) => ({
        ok: true,
        normalized_workflow_definition: workflowDefinition,
        notes: [],
      }),
    },
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

  async function validateWorkflowDefinition(graph, options = {}) {
    return await workflowValidationSupport.validateWorkflowDefinitionAuthoritatively({
      workflowDefinition: graph,
      cfg: options?.cfg || null,
      allowVersionMigration: options?.allowVersionMigration === true,
      requireNonEmptyNodes: options?.requireNonEmptyNodes === true,
      validationScope: String(options?.validationScope || "run"),
    });
  }

  async function executeWorkflowPayloadAuthoritatively(effectivePayload, merged) {
    const runRequestKind = String(effectivePayload?.run_request_kind || "draft").trim().toLowerCase();
    if (
      workflowExecutionSupport
      && typeof workflowExecutionSupport === "object"
      && (
        (runRequestKind === "reference" && typeof workflowExecutionSupport.executeReferenceWorkflowAuthoritatively === "function")
        || (runRequestKind !== "reference" && typeof workflowExecutionSupport.executeDraftWorkflowAuthoritatively === "function")
      )
    ) {
      try {
        if (runRequestKind === "reference") {
          return await workflowExecutionSupport.executeReferenceWorkflowAuthoritatively({
            payload: effectivePayload,
            cfg: merged,
          });
        }
        return await workflowExecutionSupport.executeDraftWorkflowAuthoritatively({
          payload: effectivePayload,
          cfg: merged,
        });
      }
      catch (error) {
        throw error;
      }
    }
    throw createWorkflowStoreRemoteError({
      ok: false,
      error: "workflow authoritative execution unavailable: accel-rust draft/reference execution surface is required",
      error_code: "workflow_authoritative_execution_unavailable",
      run_request_kind: runRequestKind || "draft",
    });
  }

  function resolveWorkflowDefinitionRequest(source = {}) {
    return resolveWorkflowDefinitionPayload(source, { fallbackToDefault: false });
  }

  async function canonicalizeWorkflowPayload(payload, cfg, options = {}) {
    const normalizedPayload = normalizeWorkflowPayloadShape(payload);
    const workflowDefinition = resolveWorkflowDefinitionPayload(normalizedPayload, { fallbackToDefault: false });
    if (!workflowDefinition) return normalizedPayload;
    const validated = await validateWorkflowDefinition(workflowDefinition, {
      cfg,
      allowVersionMigration: options?.allowVersionMigration === true,
      requireNonEmptyNodes: options?.requireNonEmptyNodes === true,
      validationScope: options?.validationScope,
    });
    return normalizeWorkflowPayloadShape(normalizedPayload, validated.normalized_workflow_definition);
  }

  function buildPublishedVersionId(graph, req) {
    const explicit = String(req?.published_version_id || req?.version_id || "").trim();
    if (explicit) return explicit;
    const workflowId = String(graph?.workflow_id || "workflow").trim().replace(/[^a-zA-Z0-9_.-]+/g, "_").slice(0, 80) || "workflow";
    return `${workflowId}_published_${Date.now()}_${Math.random().toString(16).slice(2, 10)}`;
  }

  function sanitizeWorkflowVersionSnapshot(item) {
    const source = item && typeof item === "object" ? item : {};
    const workflowDefinition = source.workflow_definition && typeof source.workflow_definition === "object"
      ? source.workflow_definition
      : null;
    return {
      schema_version: String(source.schema_version || ""),
      provider: String(source.provider || ""),
      owner: String(source.owner || ""),
      source_of_truth: String(source.source_of_truth || ""),
      version_id: String(source.version_id || ""),
      ts: String(source.ts || ""),
      workflow_name: String(source.workflow_name || ""),
      workflow_id: String(source.workflow_id || ""),
      workflow_definition: workflowDefinition ? JSON.parse(JSON.stringify(workflowDefinition)) : null,
    };
  }

  function sanitizeWorkflowAppPublication(item) {
    const source = item && typeof item === "object" ? item : {};
    return {
      schema_version: String(source.schema_version || ""),
      provider: String(source.provider || ""),
      owner: String(source.owner || ""),
      source_of_truth: String(source.source_of_truth || ""),
      app_id: String(source.app_id || ""),
      name: String(source.name || ""),
      workflow_id: String(source.workflow_id || ""),
      published_version_id: String(source.published_version_id || ""),
      params_schema: source.params_schema && typeof source.params_schema === "object"
        ? JSON.parse(JSON.stringify(source.params_schema))
        : {},
      template_policy: source.template_policy && typeof source.template_policy === "object"
        ? JSON.parse(JSON.stringify(source.template_policy))
        : {},
      created_at: String(source.created_at || ""),
      updated_at: String(source.updated_at || ""),
    };
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
    const workflow = resolveWorkflowDefinitionPayload(payload, { fallbackToDefault: false }) || {};
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
          let effectivePayload = await reportSupport.applyQualityRuleSetToPayload(
            await sandboxAutoFixStore.applyPayload(next.payload || {}, merged),
            merged
          );
          effectivePayload.run_request_kind = String(effectivePayload.run_request_kind || "draft");
          effectivePayload.workflow_definition_source = String(effectivePayload.workflow_definition_source || "draft_inline");
          effectivePayload = await canonicalizeWorkflowPayload(effectivePayload, merged, {
            requireNonEmptyNodes: true,
            validationScope: "run",
          });
          let out = sandboxSupport.attachQualityGate(
            await executeWorkflowPayloadAuthoritatively(effectivePayload, merged),
            effectivePayload || {}
          );
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

  function ensureDraftRunPayload(payload) {
    const source = payload && typeof payload === "object" ? payload : {};
    return normalizeWorkflowPayloadShape({
      ...source,
      run_request_kind: "draft",
      workflow_definition_source: String(source.workflow_definition_source || "draft_inline"),
    });
  }

  ipcMain.handle("aiwf:enqueueWorkflowTask", async (_evt, req) => {
    try {
      const mergedCfg = normalizeWorkflowConfig({ ...loadConfig(), ...((req && req.cfg) || {}) });
      let payload = await reportSupport.applyQualityRuleSetToPayload(ensureDraftRunPayload((req && req.payload) || {}), mergedCfg);
      payload.run_request_kind = "draft";
      payload.workflow_definition_source = String(payload.workflow_definition_source || "draft_inline");
      payload = await canonicalizeWorkflowPayload(payload, mergedCfg, {
        requireNonEmptyNodes: true,
        validationScope: "run",
      });
      const item = {
        task_id: String((req && req.task_id) || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`),
        label: String((req && req.label) || "workflow_task"),
        payload,
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
      const validated = await validateWorkflowDefinition(hit.workflow_definition || {}, {
        cfg: merged,
        requireNonEmptyNodes: false,
        validationScope: "authoring",
      });
      appendAudit("workflow_restore_version", {
        version_id: versionId,
        workflow_name: String(hit.workflow_name || ""),
        provider: String(hit.provider || ""),
      });
      return {
        ok: true,
        workflow_definition: validated.normalized_workflow_definition,
        meta: sanitizeWorkflowVersionSnapshot(hit),
        notes: Array.isArray(validated?.notes) ? validated.notes : [],
      };
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
    const workflowDefinition = resolveWorkflowDefinitionRequest(req);
    const name = String(req?.name || workflowDefinition?.name || "").trim();
    if (!workflowDefinition) return { ok: false, error: "workflow_definition required" };
    if (!name) return { ok: false, error: "name required" };
    try {
      const merged = normalizeWorkflowConfig({ ...loadConfig() });
      const validated = await validateWorkflowDefinition(workflowDefinition, {
        cfg: merged,
        requireNonEmptyNodes: true,
        validationScope: "publish",
      });
      const normalizedWorkflowDefinition = validated.normalized_workflow_definition;
    const versionOut = await workflowVersionStore.recordVersion({
      version_id: buildPublishedVersionId(normalizedWorkflowDefinition, req),
      workflow_id: String(normalizedWorkflowDefinition?.workflow_id || "custom"),
      workflow_name: name,
      workflow_definition: normalizedWorkflowDefinition,
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
      workflow_id: String(normalizedWorkflowDefinition?.workflow_id || "custom"),
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
        ok: true,
        provider: String(out?.provider || ""),
        item: sanitizeWorkflowAppPublication(out?.item),
        published_version_id: publishedVersionId,
        published_version: sanitizeWorkflowVersionSnapshot(versionOut?.item),
        notes: Array.isArray(validated?.notes) ? validated.notes : [],
      };
    }
    return out;
    } catch (error) {
      return normalizeWorkflowStoreError(error);
    }
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
        if (!versionItem?.workflow_definition || typeof versionItem.workflow_definition !== "object") {
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
      let mergedPayload = normalizeWorkflowPayloadShape(req?.payload && typeof req.payload === "object" ? { ...req.payload } : {});
      mergedPayload.run_request_kind = "reference";
      mergedPayload.workflow_definition_source = "version_reference";
      mergedPayload.version_id = publishedVersionId;
      mergedPayload.published_version_id = publishedVersionId;
      const sourceWorkflowDefinition =
        versionItem?.workflow_definition && typeof versionItem.workflow_definition === "object"
          ? versionItem.workflow_definition
          : {};
      const validated = await validateWorkflowDefinition(sourceWorkflowDefinition, {
        cfg: merged,
        requireNonEmptyNodes: true,
        validationScope: "run",
      });
      const templatedWorkflowDefinition = applyTemplateParams(validated.normalized_workflow_definition, params);
      const runtimeValidated = await validateWorkflowDefinition(templatedWorkflowDefinition, {
        cfg: merged,
        requireNonEmptyNodes: true,
        validationScope: "run",
      });
      const normalizedRuntimeWorkflowDefinition = runtimeValidated.normalized_workflow_definition;
      if (!normalizedRuntimeWorkflowDefinition.workflow_id) normalizedRuntimeWorkflowDefinition.workflow_id = item.workflow_id || "custom";
      mergedPayload = normalizeWorkflowPayloadShape(mergedPayload, normalizedRuntimeWorkflowDefinition);
      const rulesOut = await sandboxRuleStore.getRuntimeRules(merged);
      if (!rulesOut?.ok) return rulesOut;
      let effectivePayload = await reportSupport.applyQualityRuleSetToPayload(
        await sandboxAutoFixStore.applyPayload(mergedPayload, merged),
        merged
      );
      effectivePayload.run_request_kind = "reference";
      effectivePayload.workflow_definition_source = "version_reference";
      effectivePayload.version_id = publishedVersionId;
      effectivePayload.published_version_id = publishedVersionId;
      effectivePayload = await canonicalizeWorkflowPayload(effectivePayload, merged, {
        requireNonEmptyNodes: true,
        validationScope: "run",
      });
      let out = sandboxSupport.attachQualityGate(
        await executeWorkflowPayloadAuthoritatively(effectivePayload, merged),
        effectivePayload || {}
      );
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
        version_id: publishedVersionId,
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
