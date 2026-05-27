const {
  normalizeWorkflowPayloadShape,
} = require("./workflow_graph");

const TEMPLATE_PACK_ENTRY_SCHEMA_VERSION = "template_pack_entry.v1";
const LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION = "local_template_storage.v1";
const LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION = "local_template_entry.v1";
const TEMPLATE_WORKFLOW_DEFINITION_FIELD = "workflow_definition";

function createWorkflowIpcStateSupport(ctx) {
  const {
    app,
    fs,
    path,
    loadConfig,
    nowIso,
  } = ctx || {};

  function diagnosticsLogPath() {
    const dir = path.join(app.getPath("userData"), "logs");
    fs.mkdirSync(dir, { recursive: true });
    return path.join(dir, "workflow_diagnostics.jsonl");
  }

  function workflowStoreDir() {
    const dir = path.join(app.getPath("userData"), "workflow_store");
    fs.mkdirSync(dir, { recursive: true });
    return dir;
  }

  function runHistoryPath() {
    return path.join(workflowStoreDir(), "run_history.jsonl");
  }

  function workflowQueuePath() {
    return path.join(workflowStoreDir(), "workflow_task_queue.json");
  }

  function workflowQueueControlPath() {
    return path.join(workflowStoreDir(), "workflow_queue_control.json");
  }

  function nodeCachePath() {
    return path.join(workflowStoreDir(), "workflow_node_cache.json");
  }

  function nodeCacheMetricsPath() {
    return path.join(workflowStoreDir(), "workflow_node_cache_metrics.json");
  }

  function templateMarketplacePath() {
    return path.join(workflowStoreDir(), "template_marketplace.json");
  }

  function localTemplateStorePath() {
    return path.join(workflowStoreDir(), "local_templates.json");
  }

  function workflowAuditPath() {
    return path.join(workflowStoreDir(), "workflow_audit.jsonl");
  }

  function readJsonFile(filePath, fallback) {
    try {
      if (!fs.existsSync(filePath)) return fallback;
      const raw = fs.readFileSync(filePath, "utf8");
      return JSON.parse(raw);
    } catch {
      return fallback;
    }
  }

  function writeJsonFile(filePath, obj) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
  }

  function normalizeJsonContainer(raw, fallback, normalizePayload) {
    const source = raw && typeof raw === "object" && !Array.isArray(raw) ? raw : {};
    return typeof normalizePayload === "function"
      ? normalizePayload(source)
      : { ...(fallback && typeof fallback === "object" ? fallback : {}), ...source };
  }

  function deepClone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function normalizeTemplatePackItem(item, index = 0) {
    const source = item && typeof item === "object" ? item : {};
    const templates = Array.isArray(source.templates) ? deepClone(source.templates) : [];
    const id = String(source.id || `pack_${index + 1}`).trim() || `pack_${index + 1}`;
    return {
      ...source,
      schema_version: TEMPLATE_PACK_ENTRY_SCHEMA_VERSION,
      id,
      name: String(source.name || id).trim() || id,
      version: String(source.version || "v1").trim() || "v1",
      source: String(source.source || "unknown").trim() || "unknown",
      templates,
      created_at: String(source.created_at || new Date().toISOString()),
    };
  }

  function normalizeLocalTemplateEntry(item, index = 0) {
    const source = item && typeof item === "object" ? item : {};
    const workflowDefinition = source.workflow_definition && typeof source.workflow_definition === "object"
      ? deepClone(source.workflow_definition)
      : (source.graph && typeof source.graph === "object" ? deepClone(source.graph) : null);
    const id = String(source.id || `custom_${index + 1}`).trim() || `custom_${index + 1}`;
    const name = String(source.name || id).trim() || id;
    if (!workflowDefinition || typeof workflowDefinition !== "object") {
      throw new Error(`local template entry[${index}] workflow_definition is required`);
    }
    if (!String(workflowDefinition.version || "").trim()) {
      throw new Error(`local template entry[${index}] workflow_definition.version is required`);
    }
    return {
      schema_version: LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
      id,
      name,
      workflow_definition: workflowDefinition,
      template_spec_version: Number.isFinite(Number(source.template_spec_version))
        ? Math.max(1, Math.floor(Number(source.template_spec_version)))
        : 1,
      params_schema: source.params_schema && typeof source.params_schema === "object" && !Array.isArray(source.params_schema)
        ? deepClone(source.params_schema)
        : {},
      governance: source.governance && typeof source.governance === "object" && !Array.isArray(source.governance)
        ? deepClone(source.governance)
        : {},
      runtime_defaults: source.runtime_defaults && typeof source.runtime_defaults === "object" && !Array.isArray(source.runtime_defaults)
        ? deepClone(source.runtime_defaults)
        : {},
      created_at: String(source.created_at || nowIso()),
    };
  }

  function resolveOutputRoot(cfg = null) {
    const resolvedConfig = cfg && typeof cfg === "object" ? cfg : loadConfig();
    const fromConfig = String(resolvedConfig?.outputRoot || "").trim();
    if (fromConfig) return fromConfig;
    return path.join(app.getPath("documents"), "AIWF-Offline");
  }

  function normalizeWorkflowConfig(cfg = null) {
    const base = cfg && typeof cfg === "object" ? cfg : {};
    return {
      ...base,
      __app_is_packaged: !!app.isPackaged,
    };
  }

  function isMockIoAllowed() {
    return !app.isPackaged;
  }

  function normalizeAbsPath(value) {
    return path.resolve(String(value || "").trim());
  }

  function isPathWithin(childPath, rootPath) {
    try {
      const child = normalizeAbsPath(childPath).toLowerCase();
      const root = normalizeAbsPath(rootPath).toLowerCase();
      return child === root || child.startsWith(`${root}${path.sep.toLowerCase()}`);
    } catch {
      return false;
    }
  }

  function mockIoRoots(cfg = null) {
    const merged = cfg && typeof cfg === "object" ? cfg : loadConfig();
    const roots = [
      workflowStoreDir(),
      resolveOutputRoot(merged),
      app.getPath("documents"),
      app.getPath("desktop"),
      app.getPath("userData"),
    ];
    const tempDir = String(process.env.TEMP || process.env.TMP || "").trim();
    if (tempDir) roots.push(tempDir);
    return Array.from(new Set(roots.map((item) => normalizeAbsPath(item))));
  }

  function resolveMockFilePath(filePath, cfg = null) {
    const raw = String(filePath || "").trim();
    if (!raw) return { ok: false, error: "mock_path_required" };
    if (!path.isAbsolute(raw)) return { ok: false, error: "mock_path_must_be_absolute" };
    const absolutePath = normalizeAbsPath(raw);
    const roots = mockIoRoots(cfg);
    if (!roots.some((root) => isPathWithin(absolutePath, root))) {
      return { ok: false, error: "mock_path_not_allowed" };
    }
    return { ok: true, path: absolutePath };
  }

  function appendDiagnostics(run) {
    try {
      const payload = {
        ts: new Date().toISOString(),
        run_id: run?.run_id || "",
        workflow_id: run?.workflow_id || "",
        ok: !!run?.ok,
        status: run?.status || "",
        diagnostics: run?.diagnostics || {},
      };
      fs.appendFileSync(diagnosticsLogPath(), `${JSON.stringify(payload)}\n`, "utf8");
    } catch {}
  }

  function appendRunHistory(run, payload, mergedConfig) {
    try {
      const sourcePayload = payload && typeof payload === "object" ? payload : {};
      const runRequestKind = String(
        sourcePayload.run_request_kind
        || run?.run_request_kind
        || ((sourcePayload.version_id || sourcePayload.published_version_id) ? "reference" : "draft")
      ).trim() || "draft";
      const versionId = String(sourcePayload.version_id || run?.version_id || "").trim();
      const publishedVersionId = String(sourcePayload.published_version_id || run?.published_version_id || "").trim();
      const workflowDefinitionSource = String(
        sourcePayload.workflow_definition_source
        || run?.workflow_definition_source
        || (runRequestKind === "reference" ? "version_reference" : "draft_inline")
      ).trim() || (runRequestKind === "reference" ? "version_reference" : "draft_inline");
      const payloadSnapshot = normalizeWorkflowPayloadShape(
        JSON.parse(JSON.stringify(sourcePayload || {})),
        null,
        { allowLegacyWorkflowAlias: true },
      );
      const resultSnapshot = normalizeWorkflowPayloadShape(
        JSON.parse(JSON.stringify(run || {})),
        null,
        { allowLegacyWorkflowAlias: true },
      );
      const item = {
        ts: new Date().toISOString(),
        run_id: String(run?.run_id || ""),
        workflow_id: String(run?.workflow_id || ""),
        status: String(run?.status || ""),
        ok: !!run?.ok,
        run_request_kind: runRequestKind,
        version_id: versionId,
        published_version_id: publishedVersionId,
        workflow_definition_source: workflowDefinitionSource,
        payload: payloadSnapshot,
        config: mergedConfig || {},
        result: resultSnapshot,
      };
      fs.appendFileSync(runHistoryPath(), `${JSON.stringify(item)}\n`, "utf8");
    } catch {}
  }

  function extractSandboxViolations(run) {
    const out = [];
    const nodeRuns = Array.isArray(run?.node_runs) ? run.node_runs : [];
    nodeRuns.forEach((nodeRun) => {
      const error = String(nodeRun?.error || "");
      const detail = String(nodeRun?.output?.detail || "");
      const isolationLevel = String(nodeRun?.output?.isolation_level || "");
      const hit = /sandbox_limit_exceeded|sandbox_egress_blocked/i.test(error)
        || /sandbox_limit_exceeded|sandbox_egress_blocked/i.test(detail);
      if (!hit) return;
      out.push({
        run_id: String(run?.run_id || ""),
        workflow_id: String(run?.workflow_id || ""),
        node_id: String(nodeRun?.id || ""),
        node_type: String(nodeRun?.type || ""),
        isolation_level: isolationLevel || "sandbox",
        error: error || detail,
      });
    });
    return out;
  }

  function loadWorkflowQueue() {
    const obj = normalizeJsonContainer(
      readJsonFile(workflowQueuePath(), { items: [] }),
      { items: [] },
      (source) => ({
        items: Array.isArray(source?.items) ? source.items : (Array.isArray(source) ? source : []),
      })
    );
    return Array.isArray(obj?.items) ? obj.items : [];
  }

  function saveWorkflowQueue(items) {
    writeJsonFile(workflowQueuePath(), {
      items: Array.isArray(items) ? items : [],
    });
  }

  function defaultQueueControl() {
    return { paused: false, quotas: {} };
  }

  function normalizeQueueControl(control) {
    const src = control && typeof control === "object" ? control : {};
    const quotas = src.quotas && typeof src.quotas === "object" && !Array.isArray(src.quotas)
      ? src.quotas
      : {};
    const safeQuotas = {};
    Object.keys(quotas).forEach((key) => {
      const value = Number(quotas[key]);
      if (Number.isFinite(value) && value > 0) {
        safeQuotas[String(key)] = Math.max(1, Math.min(8, Math.floor(value)));
      }
    });
    return {
      paused: !!src.paused,
      quotas: safeQuotas,
    };
  }

  function loadQueueControl() {
    const container = normalizeJsonContainer(
      readJsonFile(workflowQueueControlPath(), defaultQueueControl()),
      defaultQueueControl(),
      (source) => normalizeQueueControl(source)
    );
    return normalizeQueueControl(container);
  }

  function saveQueueControl(control) {
    writeJsonFile(workflowQueueControlPath(), normalizeQueueControl(control));
  }

  function loadNodeCacheStore() {
    const obj = normalizeJsonContainer(
      readJsonFile(nodeCachePath(), { items: {}, order: [] }),
      { items: {}, order: [] },
      (source) => ({
        items: source && typeof source.items === "object" && !Array.isArray(source.items) ? source.items : {},
        order: Array.isArray(source?.order) ? source.order.map((item) => String(item || "")) : [],
      })
    );
    const items = obj && typeof obj.items === "object" && !Array.isArray(obj.items) ? obj.items : {};
    const order = Array.isArray(obj?.order) ? obj.order.map((item) => String(item || "")) : [];
    return { items, order };
  }

  function saveNodeCacheStore(store) {
    const max = Number(process.env.AIWF_WORKFLOW_NODE_CACHE_MAX || 4000);
    const capacity = Number.isFinite(max) ? Math.max(100, Math.min(20000, Math.floor(max))) : 4000;
    const items = store && typeof store.items === "object" ? store.items : {};
    const order = Array.isArray(store?.order) ? store.order : [];
    while (order.length > capacity) {
      const key = order.shift();
      if (key) delete items[key];
    }
    writeJsonFile(nodeCachePath(), { items, order });
  }

  function loadNodeCacheMetrics() {
    const metrics = normalizeJsonContainer(
      readJsonFile(nodeCacheMetricsPath(), {}),
      {},
      (source) => ({
        hits: Number(source?.hits || 0),
        misses: Number(source?.misses || 0),
        sets: Number(source?.sets || 0),
        last_reset_at: String(source?.last_reset_at || ""),
        updated_at: String(source?.updated_at || ""),
      })
    );
    return {
      hits: Number(metrics?.hits || 0),
      misses: Number(metrics?.misses || 0),
      sets: Number(metrics?.sets || 0),
      last_reset_at: String(metrics?.last_reset_at || ""),
      updated_at: String(metrics?.updated_at || ""),
    };
  }

  function saveNodeCacheMetrics(metrics) {
    writeJsonFile(nodeCacheMetricsPath(), {
      hits: Number(metrics?.hits || 0),
      misses: Number(metrics?.misses || 0),
      sets: Number(metrics?.sets || 0),
      last_reset_at: String(metrics?.last_reset_at || ""),
      updated_at: nowIso(),
    });
  }

  function createNodeCacheApi() {
    const store = loadNodeCacheStore();
    const metrics = loadNodeCacheMetrics();

    function touch(key) {
      const index = store.order.indexOf(key);
      if (index >= 0) store.order.splice(index, 1);
      store.order.push(key);
    }

    return {
      get(key) {
        const cacheKey = String(key || "");
        if (!cacheKey) return null;
        const item = store.items[cacheKey];
        if (!item) {
          metrics.misses += 1;
          saveNodeCacheMetrics(metrics);
          return null;
        }
        touch(cacheKey);
        saveNodeCacheStore(store);
        metrics.hits += 1;
        saveNodeCacheMetrics(metrics);
        return item.output;
      },
      set(key, output) {
        const cacheKey = String(key || "");
        if (!cacheKey) return;
        store.items[cacheKey] = { output, ts: new Date().toISOString() };
        touch(cacheKey);
        saveNodeCacheStore(store);
        metrics.sets += 1;
        saveNodeCacheMetrics(metrics);
      },
    };
  }

  function cacheStats() {
    const store = loadNodeCacheStore();
    const metrics = loadNodeCacheMetrics();
    const hits = Number(metrics.hits || 0);
    const misses = Number(metrics.misses || 0);
    const total = hits + misses;
    return {
      entries: Object.keys(store.items || {}).length,
      capacity_hint: Number(process.env.AIWF_WORKFLOW_NODE_CACHE_MAX || 4000),
      hits,
      misses,
      sets: Number(metrics.sets || 0),
      hit_rate: total > 0 ? Number((hits / total).toFixed(4)) : 0,
      last_reset_at: String(metrics.last_reset_at || ""),
      updated_at: String(metrics.updated_at || ""),
    };
  }

  function clearNodeCache() {
    saveNodeCacheStore({ items: {}, order: [] });
    saveNodeCacheMetrics({
      hits: 0,
      misses: 0,
      sets: 0,
      last_reset_at: nowIso(),
      updated_at: nowIso(),
    });
  }

  function loadLocalTemplateStore() {
    const raw = readJsonFile(localTemplateStorePath(), {
      schema_version: LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
      items: [],
    });
    const notes = [];
    let itemsSource = [];
    if (Array.isArray(raw)) {
      itemsSource = raw;
      notes.push(`local template storage schema_version migrated to ${LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION}`);
    } else {
      const source = raw && typeof raw === "object" ? raw : {};
      const rawSchemaVersion = String(source.schema_version || "").trim();
      if (!rawSchemaVersion) {
        notes.push(`local template storage schema_version migrated to ${LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION}`);
        itemsSource = Array.isArray(source.items) ? source.items : [];
      } else if (rawSchemaVersion !== LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION) {
        itemsSource = [];
      } else {
        itemsSource = Array.isArray(source.items) ? source.items : [];
      }
    }
    const items = [];
    for (let i = 0; i < itemsSource.length; i += 1) {
      try {
        items.push(normalizeLocalTemplateEntry(itemsSource[i], i));
      } catch {}
    }
    return {
      schema_version: LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
      items,
      migrated: notes.length > 0,
      notes,
    };
  }

  function listLocalTemplates(limit = 500) {
    const normalized = loadLocalTemplateStore();
    if (normalized.migrated) {
      saveLocalTemplates(normalized.items);
    }
    return normalized.items.slice(0, Math.max(1, Math.min(5000, Number(limit || 500))));
  }

  function saveLocalTemplates(items) {
    const normalizedItems = Array.isArray(items)
      ? items.map((item, index) => normalizeLocalTemplateEntry(item, index))
      : [];
    writeJsonFile(localTemplateStorePath(), {
      schema_version: LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
      items: normalizedItems,
    });
  }

  function listTemplateMarketplace(limit = 500) {
    const obj = normalizeJsonContainer(
      readJsonFile(templateMarketplacePath(), { items: [] }),
      { items: [] },
      (source) => ({
        items: Array.isArray(source?.items) ? source.items.map(normalizeTemplatePackItem) : [],
      })
    );
    const items = Array.isArray(obj?.items) ? obj.items : [];
    return items.slice(0, Math.max(1, Math.min(5000, Number(limit || 500))));
  }

  function saveTemplateMarketplace(items) {
    writeJsonFile(templateMarketplacePath(), {
      items: Array.isArray(items) ? items.map(normalizeTemplatePackItem) : [],
    });
  }

  function appendAudit(action, detail) {
    try {
      const item = {
        ts: nowIso(),
        action: String(action || "unknown"),
        detail: detail && typeof detail === "object" ? detail : {},
      };
      fs.appendFileSync(workflowAuditPath(), `${JSON.stringify(item)}\n`, "utf8");
    } catch {}
  }

  return {
    appendAudit,
    appendDiagnostics,
    appendRunHistory,
    cacheStats,
    clearNodeCache,
    createNodeCacheApi,
    deepClone,
    defaultQueueControl,
    diagnosticsLogPath,
    extractSandboxViolations,
    isMockIoAllowed,
    isPathWithin,
    listTemplateMarketplace,
    listLocalTemplates,
    loadLocalTemplateStore,
    loadQueueControl,
    loadWorkflowQueue,
    localTemplateStorePath,
    mockIoRoots,
    nodeCacheMetricsPath,
    nodeCachePath,
    normalizeAbsPath,
    normalizeQueueControl,
    normalizeWorkflowConfig,
    readJsonFile,
    resolveMockFilePath,
    resolveOutputRoot,
    runHistoryPath,
    saveQueueControl,
    saveLocalTemplates,
    saveTemplateMarketplace,
    saveWorkflowQueue,
    templateMarketplacePath,
    workflowAuditPath,
    workflowQueueControlPath,
    workflowQueuePath,
    workflowStoreDir,
    writeJsonFile,
  };
}

module.exports = {
  LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
  LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
  TEMPLATE_WORKFLOW_DEFINITION_FIELD,
  TEMPLATE_PACK_ENTRY_SCHEMA_VERSION,
  createWorkflowIpcStateSupport,
};
