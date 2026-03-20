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

  function reviewQueuePath() {
    return path.join(workflowStoreDir(), "manual_review_queue.json");
  }

  function reviewHistoryPath() {
    return path.join(workflowStoreDir(), "manual_review_history.jsonl");
  }

  function workflowVersionsPath() {
    return path.join(workflowStoreDir(), "workflow_versions.jsonl");
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

  function workflowAppsPath() {
    return path.join(workflowStoreDir(), "workflow_apps.json");
  }

  function templateMarketplacePath() {
    return path.join(workflowStoreDir(), "template_marketplace.json");
  }

  function qualityRuleCenterPath() {
    return path.join(workflowStoreDir(), "quality_rule_center.json");
  }

  function runBaselinePath() {
    return path.join(workflowStoreDir(), "workflow_run_baselines.json");
  }

  function workflowAuditPath() {
    return path.join(workflowStoreDir(), "workflow_audit.jsonl");
  }

  function sandboxAlertStatePath() {
    return path.join(workflowStoreDir(), "workflow_sandbox_alert_state.json");
  }

  function sandboxAlertRulesPath() {
    return path.join(workflowStoreDir(), "workflow_sandbox_alert_rules.json");
  }

  function sandboxAlertRuleVersionsPath() {
    return path.join(workflowStoreDir(), "workflow_sandbox_alert_rule_versions.jsonl");
  }

  function sandboxAutoFixStatePath() {
    return path.join(workflowStoreDir(), "workflow_sandbox_autofix_state.json");
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

  function deepClone(value) {
    return JSON.parse(JSON.stringify(value));
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
      const item = {
        ts: new Date().toISOString(),
        run_id: String(run?.run_id || ""),
        workflow_id: String(run?.workflow_id || ""),
        status: String(run?.status || ""),
        ok: !!run?.ok,
        payload: payload || {},
        config: mergedConfig || {},
        result: run || {},
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

  function appendWorkflowVersion(item) {
    try {
      fs.appendFileSync(workflowVersionsPath(), `${JSON.stringify(item)}\n`, "utf8");
    } catch {}
  }

  function listWorkflowVersions(limit = 200, workflowName = "") {
    const filePath = workflowVersionsPath();
    if (!fs.existsSync(filePath)) return [];
    const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/).filter((line) => line.trim());
    const all = lines
      .map((line) => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      })
      .filter(Boolean)
      .reverse();
    const key = String(workflowName || "").trim();
    const filtered = key ? all.filter((item) => String(item.workflow_name || "") === key) : all;
    return filtered.slice(0, Math.max(1, Math.min(2000, Number(limit || 200))));
  }

  function loadWorkflowQueue() {
    const obj = readJsonFile(workflowQueuePath(), { items: [] });
    return Array.isArray(obj?.items) ? obj.items : [];
  }

  function saveWorkflowQueue(items) {
    writeJsonFile(workflowQueuePath(), { items: Array.isArray(items) ? items : [] });
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
    return normalizeQueueControl(readJsonFile(workflowQueueControlPath(), defaultQueueControl()));
  }

  function saveQueueControl(control) {
    writeJsonFile(workflowQueueControlPath(), normalizeQueueControl(control));
  }

  function loadNodeCacheStore() {
    const obj = readJsonFile(nodeCachePath(), { items: {}, order: [] });
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
    const metrics = readJsonFile(nodeCacheMetricsPath(), {});
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

  function listWorkflowApps(limit = 200) {
    const obj = readJsonFile(workflowAppsPath(), { items: [] });
    const items = Array.isArray(obj?.items) ? obj.items : [];
    return items.slice(0, Math.max(1, Math.min(2000, Number(limit || 200))));
  }

  function saveWorkflowApps(items) {
    writeJsonFile(workflowAppsPath(), { items: Array.isArray(items) ? items : [] });
  }

  function listTemplateMarketplace(limit = 500) {
    const obj = readJsonFile(templateMarketplacePath(), { items: [] });
    const items = Array.isArray(obj?.items) ? obj.items : [];
    return items.slice(0, Math.max(1, Math.min(5000, Number(limit || 500))));
  }

  function saveTemplateMarketplace(items) {
    writeJsonFile(templateMarketplacePath(), { items: Array.isArray(items) ? items : [] });
  }

  function listQualityRuleCenter() {
    const obj = readJsonFile(qualityRuleCenterPath(), { sets: [] });
    return Array.isArray(obj?.sets) ? obj.sets : [];
  }

  function saveQualityRuleCenter(sets) {
    writeJsonFile(qualityRuleCenterPath(), { sets: Array.isArray(sets) ? sets : [] });
  }

  function listRunBaselines() {
    const obj = readJsonFile(runBaselinePath(), { items: [] });
    return Array.isArray(obj?.items) ? obj.items : [];
  }

  function saveRunBaselines(items) {
    writeJsonFile(runBaselinePath(), { items: Array.isArray(items) ? items : [] });
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
    appendWorkflowVersion,
    cacheStats,
    clearNodeCache,
    createNodeCacheApi,
    deepClone,
    defaultQueueControl,
    diagnosticsLogPath,
    extractSandboxViolations,
    isMockIoAllowed,
    isPathWithin,
    listQualityRuleCenter,
    listRunBaselines,
    listTemplateMarketplace,
    listWorkflowApps,
    listWorkflowVersions,
    loadQueueControl,
    loadWorkflowQueue,
    mockIoRoots,
    nodeCacheMetricsPath,
    nodeCachePath,
    normalizeAbsPath,
    normalizeQueueControl,
    normalizeWorkflowConfig,
    qualityRuleCenterPath,
    readJsonFile,
    resolveMockFilePath,
    resolveOutputRoot,
    reviewHistoryPath,
    reviewQueuePath,
    runBaselinePath,
    runHistoryPath,
    sandboxAlertRuleVersionsPath,
    sandboxAlertRulesPath,
    sandboxAlertStatePath,
    sandboxAutoFixStatePath,
    saveQualityRuleCenter,
    saveQueueControl,
    saveRunBaselines,
    saveTemplateMarketplace,
    saveWorkflowApps,
    saveWorkflowQueue,
    templateMarketplacePath,
    workflowAppsPath,
    workflowAuditPath,
    workflowQueueControlPath,
    workflowQueuePath,
    workflowStoreDir,
    workflowVersionsPath,
    writeJsonFile,
  };
}

module.exports = {
  createWorkflowIpcStateSupport,
};
