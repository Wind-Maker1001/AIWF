function registerWorkflowIpc(ctx) {
  const {
    app,
    ipcMain,
    dialog,
    fs,
    path,
    createWorkflowWindow,
    loadConfig,
    runMinimalWorkflow,
  } = ctx;

  function diagnosticsLogPath() {
    const p = path.join(app.getPath("userData"), "logs");
    fs.mkdirSync(p, { recursive: true });
    return path.join(p, "workflow_diagnostics.jsonl");
  }

  function workflowStoreDir() {
    const p = path.join(app.getPath("userData"), "workflow_store");
    fs.mkdirSync(p, { recursive: true });
    return p;
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

  function readJsonFile(fp, fallback) {
    try {
      if (!fs.existsSync(fp)) return fallback;
      const raw = fs.readFileSync(fp, "utf8");
      return JSON.parse(raw);
    } catch {
      return fallback;
    }
  }

  function writeJsonFile(fp, obj) {
    fs.mkdirSync(path.dirname(fp), { recursive: true });
    fs.writeFileSync(fp, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
  }

  function deepClone(v) {
    return JSON.parse(JSON.stringify(v));
  }

  function resolveOutputRoot(cfg = null) {
    const c = cfg && typeof cfg === "object" ? cfg : loadConfig();
    const fromCfg = String(c?.outputRoot || "").trim();
    if (fromCfg) return fromCfg;
    return path.join(app.getPath("documents"), "AIWF-Offline");
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
    nodeRuns.forEach((n) => {
      const err = String(n?.error || "");
      const detail = String(n?.output?.detail || "");
      const level = String(n?.output?.isolation_level || "");
      const hit = /sandbox_limit_exceeded|sandbox_egress_blocked/i.test(err) || /sandbox_limit_exceeded|sandbox_egress_blocked/i.test(detail);
      if (!hit) return;
      out.push({
        run_id: String(run?.run_id || ""),
        workflow_id: String(run?.workflow_id || ""),
        node_id: String(n?.id || ""),
        node_type: String(n?.type || ""),
        isolation_level: level || "sandbox",
        error: err || detail,
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
    const fp = workflowVersionsPath();
    if (!fs.existsSync(fp)) return [];
    const lines = fs.readFileSync(fp, "utf8").split(/\r?\n/).filter((x) => x.trim());
    const all = lines
      .map((x) => {
        try { return JSON.parse(x); } catch { return null; }
      })
      .filter(Boolean)
      .reverse();
    const key = String(workflowName || "").trim();
    const filtered = key ? all.filter((x) => String(x.workflow_name || "") === key) : all;
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
    const quotas = src.quotas && typeof src.quotas === "object" && !Array.isArray(src.quotas) ? src.quotas : {};
    const safeQuotas = {};
    Object.keys(quotas).forEach((k) => {
      const n = Number(quotas[k]);
      if (Number.isFinite(n) && n > 0) safeQuotas[String(k)] = Math.max(1, Math.min(8, Math.floor(n)));
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
    const order = Array.isArray(obj?.order) ? obj.order.map((x) => String(x || "")) : [];
    return { items, order };
  }

  function saveNodeCacheStore(store) {
    const max = Number(process.env.AIWF_WORKFLOW_NODE_CACHE_MAX || 4000);
    const cap = Number.isFinite(max) ? Math.max(100, Math.min(20000, Math.floor(max))) : 4000;
    const items = store && typeof store.items === "object" ? store.items : {};
    const order = Array.isArray(store?.order) ? store.order : [];
    while (order.length > cap) {
      const k = order.shift();
      if (k) delete items[k];
    }
    writeJsonFile(nodeCachePath(), { items, order });
  }

  function loadNodeCacheMetrics() {
    const m = readJsonFile(nodeCacheMetricsPath(), {});
    return {
      hits: Number(m?.hits || 0),
      misses: Number(m?.misses || 0),
      sets: Number(m?.sets || 0),
      last_reset_at: String(m?.last_reset_at || ""),
      updated_at: String(m?.updated_at || ""),
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
      const i = store.order.indexOf(key);
      if (i >= 0) store.order.splice(i, 1);
      store.order.push(key);
    }
    return {
      get(key) {
        const k = String(key || "");
        if (!k) return null;
        const it = store.items[k];
        if (!it) {
          metrics.misses += 1;
          saveNodeCacheMetrics(metrics);
          return null;
        }
        touch(k);
        saveNodeCacheStore(store);
        metrics.hits += 1;
        saveNodeCacheMetrics(metrics);
        return it.output;
      },
      set(key, output) {
        const k = String(key || "");
        if (!k) return;
        store.items[k] = { output, ts: new Date().toISOString() };
        touch(k);
        saveNodeCacheStore(store);
        metrics.sets += 1;
        saveNodeCacheMetrics(metrics);
      },
    };
  }

  function cacheStats() {
    const store = loadNodeCacheStore();
    const m = loadNodeCacheMetrics();
    const hits = Number(m.hits || 0);
    const misses = Number(m.misses || 0);
    const total = hits + misses;
    return {
      entries: Object.keys(store.items || {}).length,
      capacity_hint: Number(process.env.AIWF_WORKFLOW_NODE_CACHE_MAX || 4000),
      hits,
      misses,
      sets: Number(m.sets || 0),
      hit_rate: total > 0 ? Number((hits / total).toFixed(4)) : 0,
      last_reset_at: String(m.last_reset_at || ""),
      updated_at: String(m.updated_at || ""),
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
    const arr = Array.isArray(obj?.items) ? obj.items : [];
    return arr.slice(0, Math.max(1, Math.min(2000, Number(limit || 200))));
  }

  function saveWorkflowApps(items) {
    writeJsonFile(workflowAppsPath(), { items: Array.isArray(items) ? items : [] });
  }

  function listTemplateMarketplace(limit = 500) {
    const obj = readJsonFile(templateMarketplacePath(), { items: [] });
    const arr = Array.isArray(obj?.items) ? obj.items : [];
    return arr.slice(0, Math.max(1, Math.min(5000, Number(limit || 500))));
  }

  function saveTemplateMarketplace(items) {
    writeJsonFile(templateMarketplacePath(), { items: Array.isArray(items) ? items : [] });
  }

  function listQualityRuleCenter() {
    const obj = readJsonFile(qualityRuleCenterPath(), { sets: [] });
    const arr = Array.isArray(obj?.sets) ? obj.sets : [];
    return arr;
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

  function sandboxAlertCode(v) {
    const text = String(v?.error || "");
    const m = text.match(/(sandbox_(?:limit_exceeded|egress_blocked)(?::[a-z_]+)?)/i);
    return String((m && m[1]) || "sandbox_violation").toLowerCase();
  }

  function normalizeSandboxAlertRules(rules) {
    const src = rules && typeof rules === "object" ? rules : {};
    const toList = (arr) => (Array.isArray(arr) ? arr : [])
      .map((x) => String(x || "").trim().toLowerCase())
      .filter(Boolean);
    const muteSrc = src.mute_until_by_key && typeof src.mute_until_by_key === "object" ? src.mute_until_by_key : {};
    const mute = {};
    Object.keys(muteSrc).forEach((k) => {
      const key = String(k || "").trim().toLowerCase();
      const ts = Date.parse(String(muteSrc[k] || ""));
      if (!key || !Number.isFinite(ts)) return;
      mute[key] = new Date(ts).toISOString();
    });
    return {
      whitelist_codes: Array.from(new Set(toList(src.whitelist_codes))),
      whitelist_node_types: Array.from(new Set(toList(src.whitelist_node_types))),
      whitelist_keys: Array.from(new Set(toList(src.whitelist_keys))),
      mute_until_by_key: mute,
    };
  }

  function loadSandboxAlertRules() {
    return normalizeSandboxAlertRules(readJsonFile(sandboxAlertRulesPath(), {
      whitelist_codes: [],
      whitelist_node_types: [],
      whitelist_keys: [],
      mute_until_by_key: {},
    }));
  }

  function saveSandboxAlertRules(rules) {
    writeJsonFile(sandboxAlertRulesPath(), normalizeSandboxAlertRules(rules));
  }

  function appendSandboxRuleVersion(rules, meta = {}) {
    try {
      const item = {
        version_id: `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`,
        ts: nowIso(),
        rules: normalizeSandboxAlertRules(rules),
        meta: meta && typeof meta === "object" ? meta : {},
      };
      fs.appendFileSync(sandboxAlertRuleVersionsPath(), `${JSON.stringify(item)}\n`, "utf8");
      return item;
    } catch {
      return null;
    }
  }

  function listSandboxRuleVersions(limit = 200) {
    const fp = sandboxAlertRuleVersionsPath();
    if (!fs.existsSync(fp)) return [];
    const lines = fs.readFileSync(fp, "utf8").split(/\r?\n/).filter((x) => x.trim());
    return lines
      .map((x) => {
        try { return JSON.parse(x); } catch { return null; }
      })
      .filter(Boolean)
      .reverse()
      .slice(0, Math.max(1, Math.min(2000, Number(limit || 200))));
  }

  function compareSandboxRuleVersions(versionA, versionB) {
    const items = listSandboxRuleVersions(5000);
    const a = items.find((x) => String(x.version_id || "") === String(versionA || ""));
    const b = items.find((x) => String(x.version_id || "") === String(versionB || ""));
    if (!a || !b) return { ok: false, error: "rule version not found" };
    const ra = normalizeSandboxAlertRules(a.rules || {});
    const rb = normalizeSandboxAlertRules(b.rules || {});
    function diffList(key) {
      const sa = new Set(Array.isArray(ra[key]) ? ra[key] : []);
      const sb = new Set(Array.isArray(rb[key]) ? rb[key] : []);
      return {
        added: Array.from(sb).filter((x) => !sa.has(x)),
        removed: Array.from(sa).filter((x) => !sb.has(x)),
      };
    }
    const muteA = ra.mute_until_by_key && typeof ra.mute_until_by_key === "object" ? ra.mute_until_by_key : {};
    const muteB = rb.mute_until_by_key && typeof rb.mute_until_by_key === "object" ? rb.mute_until_by_key : {};
    const muteKeys = Array.from(new Set([...Object.keys(muteA), ...Object.keys(muteB)])).sort();
    const muteChanged = muteKeys
      .filter((k) => String(muteA[k] || "") !== String(muteB[k] || ""))
      .map((k) => ({ key: k, from: String(muteA[k] || ""), to: String(muteB[k] || "") }));
    return {
      ok: true,
      summary: {
        version_a: String(versionA || ""),
        version_b: String(versionB || ""),
      },
      whitelist_codes: diffList("whitelist_codes"),
      whitelist_node_types: diffList("whitelist_node_types"),
      whitelist_keys: diffList("whitelist_keys"),
      mute_changed: muteChanged,
    };
  }

  function loadSandboxAutoFixState() {
    const obj = readJsonFile(sandboxAutoFixStatePath(), {
      violation_events: [],
      forced_isolation_mode: "",
      forced_until: "",
      last_actions: [],
      green_streak: 0,
    });
    return {
      violation_events: Array.isArray(obj?.violation_events) ? obj.violation_events : [],
      forced_isolation_mode: String(obj?.forced_isolation_mode || ""),
      forced_until: String(obj?.forced_until || ""),
      last_actions: Array.isArray(obj?.last_actions) ? obj.last_actions : [],
      green_streak: Number.isFinite(Number(obj?.green_streak)) ? Math.max(0, Math.floor(Number(obj.green_streak))) : 0,
    };
  }

  function saveSandboxAutoFixState(state) {
    writeJsonFile(sandboxAutoFixStatePath(), {
      violation_events: Array.isArray(state?.violation_events) ? state.violation_events.slice(-5000) : [],
      forced_isolation_mode: String(state?.forced_isolation_mode || ""),
      forced_until: String(state?.forced_until || ""),
      last_actions: Array.isArray(state?.last_actions) ? state.last_actions.slice(-200) : [],
      green_streak: Number.isFinite(Number(state?.green_streak)) ? Math.max(0, Math.floor(Number(state.green_streak))) : 0,
      updated_at: nowIso(),
    });
  }

  function applySandboxAutoFixPayload(payload) {
    const base = payload && typeof payload === "object" ? { ...payload } : {};
    const st = loadSandboxAutoFixState();
    const until = Date.parse(String(st.forced_until || ""));
    if (st.forced_isolation_mode && Number.isFinite(until) && Date.now() < until) {
      base.chiplet_isolation_enabled = true;
      base.chiplet_isolation_mode = st.forced_isolation_mode;
      if (Array.isArray(base.chiplet_isolated_types) && !base.chiplet_isolated_types.includes("*")) {
        base.chiplet_isolated_types = [...base.chiplet_isolated_types, "*"];
      } else if (!Array.isArray(base.chiplet_isolated_types)) {
        base.chiplet_isolated_types = ["*"];
      }
    }
    return base;
  }

  async function sendSandboxNotification(event, payload = {}, extra = {}) {
    const req = payload && typeof payload === "object" ? payload : {};
    if (req.sandbox_notify_enabled === false) return { ok: false, skipped: true };
    const webhook = String(req.sandbox_notify_webhook || "").trim();
    if (!webhook) return { ok: false, skipped: true };
    try {
      const body = {
        ts: nowIso(),
        event: String(event || "sandbox_event"),
        workflow_id: String(extra?.workflow_id || ""),
        run_id: String(extra?.run_id || ""),
        level: String(extra?.level || ""),
        count: Number(extra?.count || 0),
        actions: Array.isArray(extra?.actions) ? extra.actions : [],
      };
      const resp = await fetch(webhook, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        appendAudit("sandbox_notify_failed", { status: resp.status, webhook });
        return { ok: false, status: resp.status };
      }
      appendAudit("sandbox_notify_sent", { webhook, event });
      return { ok: true };
    } catch (e) {
      appendAudit("sandbox_notify_failed", { error: String(e) });
      return { ok: false, error: String(e) };
    }
  }

  async function maybeApplySandboxAutoFix(run, payload = {}) {
    const req = payload && typeof payload === "object" ? payload : {};
    if (req.sandbox_autofix_enabled === false) return { triggered: false, actions: [] };
    const violations = extractSandboxViolations(run);
    const now = Date.now();
    const red = Number.isFinite(Number(req?.sandbox_autofix_red_threshold)) ? Math.max(1, Math.floor(Number(req.sandbox_autofix_red_threshold))) : 3;
    const windowSec = Number.isFinite(Number(req?.sandbox_autofix_window_sec)) ? Math.max(60, Math.floor(Number(req.sandbox_autofix_window_sec))) : 900;
    const recoverGreenRuns = Number.isFinite(Number(req?.sandbox_autofix_recover_green_runs))
      ? Math.max(1, Math.floor(Number(req.sandbox_autofix_recover_green_runs)))
      : 3;
    const doRecoverQueue = req?.sandbox_autofix_recover_queue !== false;
    const doRecoverIsolation = req?.sandbox_autofix_recover_isolation !== false;
    const forceMode = String(req?.sandbox_autofix_force_mode || "process").trim().toLowerCase();
    const forceMins = Number.isFinite(Number(req?.sandbox_autofix_force_minutes)) ? Math.max(1, Math.floor(Number(req.sandbox_autofix_force_minutes))) : 60;
    const doPause = req?.sandbox_autofix_pause_queue !== false;
    const doReview = req?.sandbox_autofix_require_review !== false;
    const doForce = req?.sandbox_autofix_force_isolation !== false;
    const st = loadSandboxAutoFixState();
    st.green_streak = Number.isFinite(Number(st.green_streak)) ? Math.max(0, Math.floor(Number(st.green_streak))) : 0;
    const fresh = Array.isArray(st.violation_events)
      ? st.violation_events.filter((x) => Number.isFinite(Number(x?.ts_ms || 0)) && now - Number(x.ts_ms || 0) <= windowSec * 1000)
      : [];
    if (violations.length) {
      st.green_streak = 0;
      violations.forEach((v) => fresh.push({
        ts_ms: now,
        run_id: String(run?.run_id || ""),
        node_type: String(v?.node_type || ""),
        node_id: String(v?.node_id || ""),
        code: sandboxAlertCode(v),
      }));
    } else {
      st.green_streak += 1;
    }
    st.violation_events = fresh;
    const count = fresh.length;
    const actions = [];
    if (count >= red) {
      if (doPause) {
        queueState.control = { ...(queueState.control || defaultQueueControl()), paused: true };
        saveQueueControl(queueState.control);
        actions.push("pause_queue");
      }
      if (doReview) {
        enqueueReviews([{
          run_id: String(run?.run_id || ""),
          workflow_id: String(run?.workflow_id || ""),
          node_id: "sandbox_autofix",
          review_key: "sandbox_autofix",
          reviewer: "security_reviewer",
          comment: `sandbox violations reached ${count} within ${windowSec}s`,
          created_at: nowIso(),
          status: "pending",
        }]);
        actions.push("require_manual_review");
      }
      if (doForce) {
        st.forced_isolation_mode = forceMode || "process";
        st.forced_until = new Date(now + forceMins * 60000).toISOString();
        actions.push(`force_isolation:${st.forced_isolation_mode}`);
      }
      st.last_actions.push({
        ts: nowIso(),
        run_id: String(run?.run_id || ""),
        count,
        actions,
      });
      appendAudit("sandbox_autofix_triggered", {
        run_id: String(run?.run_id || ""),
        count,
        window_sec: windowSec,
        threshold_red: red,
        actions,
      });
      await sendSandboxNotification("sandbox_autofix_triggered", req, {
        workflow_id: String(run?.workflow_id || ""),
        run_id: String(run?.run_id || ""),
        level: "red",
        count,
        actions,
      });
    } else if (!violations.length && st.green_streak >= recoverGreenRuns) {
      if (doRecoverQueue && queueState.control?.paused) {
        queueState.control = { ...(queueState.control || defaultQueueControl()), paused: false };
        saveQueueControl(queueState.control);
        actions.push("resume_queue");
      }
      if (doRecoverIsolation && st.forced_isolation_mode) {
        st.forced_isolation_mode = "";
        st.forced_until = "";
        actions.push("clear_forced_isolation");
      }
      if (actions.length) {
        st.last_actions.push({
          ts: nowIso(),
          run_id: String(run?.run_id || ""),
          count,
          actions,
          recovered: true,
        });
        appendAudit("sandbox_autofix_recovered", {
          run_id: String(run?.run_id || ""),
          green_streak: st.green_streak,
          recover_green_runs: recoverGreenRuns,
          actions,
        });
        await sendSandboxNotification("sandbox_autofix_recovered", req, {
          workflow_id: String(run?.workflow_id || ""),
          run_id: String(run?.run_id || ""),
          level: "green",
          count,
          actions,
        });
      }
    }
    saveSandboxAutoFixState(st);
    return {
      triggered: actions.length > 0,
      actions,
      count,
      window_sec: windowSec,
      threshold_red: red,
      green_streak: st.green_streak,
      recover_green_runs: recoverGreenRuns,
    };
  }

  function sandboxRuleKeyVariants(v) {
    const nt = String(v?.node_type || "").trim().toLowerCase() || "*";
    const nid = String(v?.node_id || "").trim().toLowerCase() || "*";
    const code = sandboxAlertCode(v);
    return [
      `${nt}::${nid}::${code}`,
      `${nt}::${nid}::*`,
      `${nt}::*::${code}`,
      `${nt}::*::*`,
      `*::${nid}::${code}`,
      `*::${nid}::*`,
      `*::*::${code}`,
      `*::*::*`,
    ];
  }

  function sandboxSuppressionReason(v, rules, nowTs = Date.now()) {
    const r = normalizeSandboxAlertRules(rules);
    const code = sandboxAlertCode(v);
    const nt = String(v?.node_type || "").trim().toLowerCase();
    if (r.whitelist_codes.includes(code)) return "whitelist";
    if (nt && r.whitelist_node_types.includes(nt)) return "whitelist";
    const variants = sandboxRuleKeyVariants(v);
    if (variants.some((k) => r.whitelist_keys.includes(k))) return "whitelist";
    for (const k of variants) {
      const until = Date.parse(String(r.mute_until_by_key?.[k] || ""));
      if (Number.isFinite(until) && nowTs < until) return "muted";
    }
    return "";
  }

  function sandboxAlertDedupWindowSec(payload = null) {
    const fromPayload = Number(payload?.sandbox_alert_dedup_window_sec);
    if (Number.isFinite(fromPayload) && fromPayload >= 0) return Math.floor(fromPayload);
    const fromEnv = Number(process.env.AIWF_SANDBOX_ALERT_DEDUP_WINDOW_SEC || 600);
    return Number.isFinite(fromEnv) && fromEnv >= 0 ? Math.floor(fromEnv) : 600;
  }

  function loadSandboxAlertState() {
    const obj = readJsonFile(sandboxAlertStatePath(), { last_by_key: {} });
    const m = obj?.last_by_key && typeof obj.last_by_key === "object" ? obj.last_by_key : {};
    return { last_by_key: m };
  }

  function saveSandboxAlertState(state) {
    writeJsonFile(sandboxAlertStatePath(), {
      last_by_key: state?.last_by_key && typeof state.last_by_key === "object" ? state.last_by_key : {},
      updated_at: nowIso(),
    });
  }

  function appendSandboxViolationAudit(v, payload = null) {
    const rules = loadSandboxAlertRules();
    const reason = sandboxSuppressionReason(v, rules, Date.now());
    if (reason) return false;
    const winSec = sandboxAlertDedupWindowSec(payload);
    const key = `${String(v?.node_type || "")}::${String(v?.node_id || "")}::${sandboxAlertCode(v)}`;
    if (winSec > 0) {
      const state = loadSandboxAlertState();
      const now = Date.now();
      const prev = Number(state?.last_by_key?.[key] || 0);
      if (Number.isFinite(prev) && now - prev < winSec * 1000) return false;
      state.last_by_key[key] = now;
      saveSandboxAlertState(state);
    }
    appendAudit("sandbox_violation", v);
    return true;
  }

  function dedupSandboxItems(items, dedupWindowSec = 0) {
    const win = Number.isFinite(Number(dedupWindowSec)) ? Math.max(0, Math.floor(Number(dedupWindowSec))) : 0;
    if (win <= 0) return { items, suppressed: 0 };
    const sorted = (Array.isArray(items) ? items : [])
      .slice()
      .sort((a, b) => String(a.ts || "").localeCompare(String(b.ts || "")));
    const lastTsByKey = new Map();
    const kept = [];
    let suppressed = 0;
    sorted.forEach((it) => {
      const code = sandboxAlertCode(it);
      const key = `${String(it.node_type || "")}::${String(it.node_id || "")}::${code}`;
      const cur = Date.parse(String(it.ts || ""));
      const prev = lastTsByKey.get(key);
      if (Number.isFinite(cur) && Number.isFinite(prev) && cur - prev < win * 1000) {
        suppressed += 1;
        return;
      }
      if (Number.isFinite(cur)) lastTsByKey.set(key, cur);
      kept.push(it);
    });
    return { items: kept.reverse(), suppressed };
  }

  function officeQualityGate(run, payload = {}) {
    const req = payload && typeof payload === "object" ? payload : {};
    const enabled = req.office_quality_gate_enabled === true;
    const maxMojibakeRate = Number.isFinite(Number(req.office_quality_gate_max_mojibake_rate))
      ? Math.max(0, Math.min(1, Number(req.office_quality_gate_max_mojibake_rate)))
      : 0.01;
    const minOfficeBytes = Number.isFinite(Number(req.office_quality_gate_min_office_bytes))
      ? Math.max(256, Math.floor(Number(req.office_quality_gate_min_office_bytes)))
      : 10 * 1024;
    const requireSlot = req.office_quality_gate_require_slot_binding !== false;
    const issues = [];
    const artifacts = Array.isArray(run?.artifacts) ? run.artifacts : [];
    const officeArtifacts = artifacts.filter((a) => /\.(docx|pptx|xlsx)$/i.test(String(a?.path || "")));
    officeArtifacts.forEach((a) => {
      try {
        const p = String(a?.path || "");
        if (!p || !fs.existsSync(p)) {
          issues.push(`office_missing:${p}`);
          return;
        }
        const st = fs.statSync(p);
        if (Number(st.size || 0) < minOfficeBytes) issues.push(`office_too_small:${p}`);
      } catch (e) {
        issues.push(`office_stat_failed:${String(e)}`);
      }
    });
    const textArtifacts = artifacts.filter((a) => /\.(md|txt|json)$/i.test(String(a?.path || "")));
    textArtifacts.slice(0, 20).forEach((a) => {
      try {
        const p = String(a?.path || "");
        if (!p || !fs.existsSync(p)) return;
        const txt = fs.readFileSync(p, "utf8");
        const bad = (txt.match(/\uFFFD/g) || []).length;
        const rate = txt.length > 0 ? bad / txt.length : 0;
        if (rate > maxMojibakeRate) issues.push(`mojibake_high:${p}:${rate.toFixed(4)}`);
      } catch {}
    });
    if (requireSlot) {
      const nodeOutputs = run?.node_outputs && typeof run.node_outputs === "object" ? run.node_outputs : {};
      const hasBinding = Object.values(nodeOutputs).some((x) => String(x?.binding_path || "").trim().toLowerCase().endsWith(".json"));
      if (!hasBinding) issues.push("slot_binding_missing");
    }
    return {
      enabled,
      passed: issues.length === 0,
      blocked: enabled && issues.length > 0,
      issues,
      checked_at: nowIso(),
      config: {
        max_mojibake_rate: maxMojibakeRate,
        min_office_bytes: minOfficeBytes,
        require_slot_binding: requireSlot,
      },
    };
  }

  function listQualityGateReports(limit = 200, filter = {}) {
    const runs = listRunHistory(limit);
    const runIdLike = String(filter?.run_id || "").trim().toLowerCase();
    const statusFilter = String(filter?.status || "").trim().toLowerCase();
    return runs
      .map((r) => ({
        ts: String(r?.ts || ""),
        run_id: String(r?.run_id || ""),
        workflow_id: String(r?.workflow_id || ""),
        quality_gate: r?.result?.quality_gate || null,
        status: String(r?.result?.status || ""),
        ok: !!r?.result?.ok,
      }))
      .filter((x) => x.quality_gate && typeof x.quality_gate === "object")
      .filter((x) => {
        if (runIdLike && !String(x.run_id || "").toLowerCase().includes(runIdLike)) return false;
        if (!statusFilter || statusFilter === "all") return true;
        const blocked = !!x?.quality_gate?.blocked;
        const passed = !!x?.quality_gate?.passed;
        if (statusFilter === "blocked") return blocked;
        if (statusFilter === "pass" || statusFilter === "passed") return passed;
        return true;
      });
  }

  function attachQualityGate(run, payload = {}) {
    const out = run && typeof run === "object" ? run : {};
    const gate = officeQualityGate(out, payload || {});
    out.quality_gate = gate;
    if (gate.blocked) {
      out.ok = false;
      out.status = "quality_blocked";
      const msg = `quality_gate_blocked:${gate.issues.join("|")}`;
      out.error = out.error ? `${String(out.error)}; ${msg}` : msg;
    }
    return out;
  }

  function listAudit(limit = 200, action = "") {
    const fp = workflowAuditPath();
    if (!fs.existsSync(fp)) return [];
    const lines = fs.readFileSync(fp, "utf8").split(/\r?\n/).filter((x) => x.trim());
    const key = String(action || "").trim();
    const all = lines
      .slice(Math.max(0, lines.length - Math.max(1, Math.min(5000, Number(limit || 200)))))
      .map((x) => {
        try { return JSON.parse(x); } catch { return null; }
      })
      .filter(Boolean)
      .reverse();
    return key ? all.filter((x) => String(x.action || "") === key) : all;
  }

  const queueState = {
    running: new Map(),
    draining: false,
    control: loadQueueControl(),
  };

  function nowIso() {
    return new Date().toISOString();
  }

  function queueConcurrency() {
    const n = Number(process.env.AIWF_WORKFLOW_QUEUE_CONCURRENCY || 2);
    return Number.isFinite(n) ? Math.max(1, Math.min(8, Math.floor(n))) : 2;
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
          .filter((x) => String(x.status || "queued") === "queued")
          .sort((a, b) => Number(a.priority || 0) - Number(b.priority || 0) || String(a.created_at || "").localeCompare(String(b.created_at || "")));
        const next = sorted.find((candidate) => {
          const key = queueTaskKey(candidate);
          const quota = Number(queueState.control?.quotas?.[key] || 0);
          if (!Number.isFinite(quota) || quota <= 0) return true;
          let runningCnt = 0;
          for (const runningItem of queueState.running.values()) {
            if (queueTaskKey(runningItem.task || {}) === key) runningCnt += 1;
          }
          return runningCnt < quota;
        });
        if (!next) break;
        const itemId = String(next.task_id || "");
        if (!itemId) break;
        next.status = "running";
        next.started_at = nowIso();
        saveWorkflowQueue(items);
        const promise = (async () => {
          const merged = { ...loadConfig(), ...(next.cfg || {}) };
          const effectivePayload = applyQualityRuleSetToPayload(applySandboxAutoFixPayload(next.payload || {}));
          const out = attachQualityGate(await runMinimalWorkflow({
            payload: effectivePayload,
            config: merged,
            outputRoot: resolveOutputRoot(merged),
            nodeCache: createNodeCacheApi(),
          }), effectivePayload || {});
          appendDiagnostics(out);
          appendRunHistory(out, effectivePayload, merged);
          extractSandboxViolations(out).forEach((v) => appendSandboxViolationAudit(v, effectivePayload || {}));
          maybeApplySandboxAutoFix(out, effectivePayload || {});
          if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) enqueueReviews(out.pending_reviews);
          const latest = loadWorkflowQueue();
          const idx = latest.findIndex((x) => String(x.task_id || "") === itemId);
          if (idx >= 0) {
            latest[idx].status = out?.ok ? "done" : "failed";
            latest[idx].finished_at = nowIso();
            latest[idx].result = out;
            latest[idx].run_id = String(out?.run_id || "");
            saveWorkflowQueue(latest);
          }
        })()
          .catch((e) => {
            const latest = loadWorkflowQueue();
            const idx = latest.findIndex((x) => String(x.task_id || "") === itemId);
            if (idx >= 0) {
              latest[idx].status = "failed";
              latest[idx].finished_at = nowIso();
              latest[idx].error = String(e);
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
      return value.replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_m, key) => {
        const v = params && Object.prototype.hasOwnProperty.call(params, key) ? params[key] : "";
        return String(v ?? "");
      });
    }
    if (Array.isArray(value)) return value.map((x) => applyTemplateParams(x, params));
    if (typeof value === "object") {
      const out = {};
      Object.keys(value).forEach((k) => {
        out[k] = applyTemplateParams(value[k], params);
      });
      return out;
    }
    return value;
  }

  function compareVersions(versionA, versionB) {
    const items = listWorkflowVersions(5000, "");
    const a = items.find((x) => String(x.version_id || "") === String(versionA || ""));
    const b = items.find((x) => String(x.version_id || "") === String(versionB || ""));
    if (!a || !b) return { ok: false, error: "version not found" };
    const ga = a.graph && typeof a.graph === "object" ? a.graph : {};
    const gb = b.graph && typeof b.graph === "object" ? b.graph : {};
    const nodesA = Array.isArray(ga.nodes) ? ga.nodes : [];
    const nodesB = Array.isArray(gb.nodes) ? gb.nodes : [];
    const edgesA = Array.isArray(ga.edges) ? ga.edges : [];
    const edgesB = Array.isArray(gb.edges) ? gb.edges : [];
    const mapA = new Map(nodesA.map((n) => [String(n.id || ""), n]));
    const mapB = new Map(nodesB.map((n) => [String(n.id || ""), n]));
    const allNodeIds = Array.from(new Set([...mapA.keys(), ...mapB.keys()]));
    const node_diff = allNodeIds.map((id) => {
      const na = mapA.get(id);
      const nb = mapB.get(id);
      if (!na) return { id, change: "added", type_a: "", type_b: String(nb?.type || "") };
      if (!nb) return { id, change: "removed", type_a: String(na?.type || ""), type_b: "" };
      const typeChanged = String(na?.type || "") !== String(nb?.type || "");
      const cfgChanged = JSON.stringify(na?.config || {}) !== JSON.stringify(nb?.config || {});
      return {
        id,
        change: typeChanged || cfgChanged ? "updated" : "same",
        type_a: String(na?.type || ""),
        type_b: String(nb?.type || ""),
        type_changed: typeChanged,
        config_changed: cfgChanged,
      };
    });
    const edgeKey = (e) => `${String(e?.from || "")}->${String(e?.to || "")}:${JSON.stringify(e?.when ?? null)}`;
    const setA = new Set(edgesA.map(edgeKey));
    const setB = new Set(edgesB.map(edgeKey));
    const added_edges = Array.from(setB).filter((x) => !setA.has(x));
    const removed_edges = Array.from(setA).filter((x) => !setB.has(x));
    const changed_nodes = node_diff.filter((x) => x.change !== "same").length;
    return {
      ok: true,
      summary: {
        version_a: String(versionA || ""),
        version_b: String(versionB || ""),
        nodes_a: nodesA.length,
        nodes_b: nodesB.length,
        edges_a: edgesA.length,
        edges_b: edgesB.length,
        changed_nodes,
        added_edges: added_edges.length,
        removed_edges: removed_edges.length,
      },
      node_diff,
      added_edges,
      removed_edges,
    };
  }

  function runTimeline(runId) {
    const found = findRunById(runId);
    if (!found) return { ok: false, error: "run not found" };
    const rows = Array.isArray(found?.result?.node_runs) ? found.result.node_runs : [];
    const timeline = rows
      .map((n) => ({
        node_id: String(n?.id || ""),
        type: String(n?.type || ""),
        status: String(n?.status || ""),
        started_at: String(n?.started_at || ""),
        ended_at: String(n?.ended_at || ""),
        seconds: Number(n?.seconds || 0),
      }))
      .sort((a, b) => String(a.started_at || "").localeCompare(String(b.started_at || "")));
    return {
      ok: true,
      run_id: runId,
      status: String(found?.result?.status || ""),
      timeline,
    };
  }

  function failureSummary(limit = 400) {
    const runs = listRunHistory(limit);
    const failedRuns = runs.filter((x) => !x?.result?.ok);
    const byNode = {};
    failedRuns.forEach((r) => {
      const nodeRuns = Array.isArray(r?.result?.node_runs) ? r.result.node_runs : [];
      nodeRuns
        .filter((n) => String(n?.status || "") === "failed")
        .forEach((n) => {
          const key = String(n?.type || "unknown");
          if (!byNode[key]) byNode[key] = { failed: 0, samples: [] };
          byNode[key].failed += 1;
          if (byNode[key].samples.length < 3) byNode[key].samples.push(String(n?.error || "").slice(0, 200));
        });
    });
    return {
      ok: true,
      total_runs: runs.length,
      failed_runs: failedRuns.length,
      by_node: byNode,
    };
  }

  function sandboxAlerts(limit = 400, thresholds = null, dedupWindowSec = 0) {
    const runs = listRunHistory(limit);
    const items = [];
    runs.forEach((r) => {
      const run = r?.result || {};
      extractSandboxViolations(run).forEach((v) => {
        items.push({
          ts: String(r?.ts || ""),
          ...v,
        });
      });
    });
    const rules = loadSandboxAlertRules();
    const filtered = [];
    let suppressedWhitelist = 0;
    let suppressedMuted = 0;
    items.forEach((it) => {
      const rs = sandboxSuppressionReason(it, rules, Date.now());
      if (!rs) {
        filtered.push(it);
        return;
      }
      if (rs === "whitelist") suppressedWhitelist += 1;
      else if (rs === "muted") suppressedMuted += 1;
    });
    const dedup = dedupSandboxItems(filtered, dedupWindowSec);
    const effectiveItems = Array.isArray(dedup.items) ? dedup.items : [];
    const byNode = {};
    effectiveItems.forEach((x) => {
      const key = `${x.node_type}::${x.node_id}`;
      if (!byNode[key]) byNode[key] = { count: 0, node_type: x.node_type, node_id: x.node_id, last_run_id: "", last_ts: "" };
      byNode[key].count += 1;
      byNode[key].last_run_id = x.run_id;
      byNode[key].last_ts = x.ts;
    });
    const t = thresholds && typeof thresholds === "object" ? thresholds : {};
    const yellow = Number.isFinite(Number(t.yellow)) ? Math.max(1, Math.floor(Number(t.yellow))) : 1;
    const red = Number.isFinite(Number(t.red)) ? Math.max(yellow + 1, Math.floor(Number(t.red))) : Math.max(3, yellow + 1);
    const total = effectiveItems.length;
    const level = total >= red ? "red" : (total >= yellow ? "yellow" : "green");
    return {
      ok: true,
      total,
      items: effectiveItems.slice(0, 200),
      by_node: Object.values(byNode).sort((a, b) => Number(b.count || 0) - Number(a.count || 0)).slice(0, 80),
      rules,
      health: {
        level,
        total,
        thresholds: { yellow, red },
        dedup_window_sec: Math.max(0, Math.floor(Number(dedupWindowSec || 0))),
        suppressed: Number(dedup.suppressed || 0) + suppressedWhitelist + suppressedMuted,
        suppressed_dedup: Number(dedup.suppressed || 0),
        suppressed_whitelist: suppressedWhitelist,
        suppressed_muted: suppressedMuted,
      },
    };
  }

  function readDiagnostics(limit = 50) {
    const fp = diagnosticsLogPath();
    if (!fs.existsSync(fp)) return { ok: true, items: [], by_chiplet: {} };
    const lines = fs.readFileSync(fp, "utf8").split(/\r?\n/).filter((x) => x.trim());
    const items = lines
      .slice(Math.max(0, lines.length - limit))
      .map((x) => {
        try {
          return JSON.parse(x);
        } catch {
          return null;
        }
      })
      .filter(Boolean);

    const by = {};
    for (const it of items) {
      const chiplets = it?.diagnostics?.chiplets || {};
      Object.keys(chiplets).forEach((k) => {
        const c = chiplets[k] || {};
        if (!by[k]) by[k] = { runs: 0, failed: 0, seconds_total: 0, attempts_total: 0 };
        by[k].runs += Number(c.runs || 0);
        by[k].failed += Number(c.failed || 0);
        by[k].seconds_total += Number(c.seconds_total || 0);
        by[k].attempts_total += Number(c.attempts_total || 0);
      });
    }

    Object.values(by).forEach((v) => {
      v.seconds_total = Number(v.seconds_total.toFixed(3));
      v.seconds_avg = v.runs > 0 ? Number((v.seconds_total / v.runs).toFixed(3)) : 0;
      v.failure_rate = v.runs > 0 ? Number((v.failed / v.runs).toFixed(4)) : 0;
    });

    return { ok: true, items, by_chiplet: by };
  }

  function percentile(values, q) {
    const arr = (Array.isArray(values) ? values : [])
      .map((x) => Number(x))
      .filter((x) => Number.isFinite(x))
      .sort((a, b) => a - b);
    if (!arr.length) return 0;
    const p = Math.max(0, Math.min(1, Number(q)));
    const i = Math.ceil(arr.length * p) - 1;
    return Number(arr[Math.max(0, Math.min(arr.length - 1, i))].toFixed(3));
  }

  function isFallbackNodeRun(nodeRun) {
    const status = String(nodeRun?.output?.status || nodeRun?.status || "").toLowerCase();
    if (status === "fallback") return true;
    if (nodeRun?.output?.fallback === true || nodeRun?.output?.degraded === true) return true;
    const detail = String(nodeRun?.output?.detail || nodeRun?.error || "").toLowerCase();
    return /fallback|degrad|降级/.test(detail);
  }

  function buildPerfDashboard(limit = 200) {
    const runs = listRunHistory(limit);
    const by = {};
    runs.forEach((it) => {
      const nodes = Array.isArray(it?.result?.node_runs) ? it.result.node_runs : [];
      nodes.forEach((n) => {
        const k = String(n?.type || "").trim();
        if (!k) return;
        if (!by[k]) {
          by[k] = {
            chiplet: k,
            calls: 0,
            failed: 0,
            seconds: [],
            attempts_total: 0,
            fallback_count: 0,
          };
        }
        by[k].calls += 1;
        if (String(n?.status || "") !== "done") by[k].failed += 1;
        by[k].seconds.push(Number(n?.seconds || 0));
        by[k].attempts_total += Math.max(1, Number(n?.telemetry?.attempts || 1));
        if (isFallbackNodeRun(n)) by[k].fallback_count += 1;
      });
    });
    const items = Object.values(by).map((x) => {
      const calls = Number(x.calls || 0);
      const retries = Math.max(0, Number(x.attempts_total || 0) - calls);
      return {
        chiplet: x.chiplet,
        calls,
        failed: Number(x.failed || 0),
        error_rate: calls > 0 ? Number((Number(x.failed || 0) / calls).toFixed(4)) : 0,
        retry_rate: calls > 0 ? Number((retries / calls).toFixed(4)) : 0,
        fallback_rate: calls > 0 ? Number((Number(x.fallback_count || 0) / calls).toFixed(4)) : 0,
        p95_seconds: percentile(x.seconds, 0.95),
        avg_seconds: calls > 0 ? Number((x.seconds.reduce((a, b) => a + b, 0) / calls).toFixed(3)) : 0,
      };
    }).sort((a, b) => String(a.chiplet).localeCompare(String(b.chiplet)));
    return {
      ok: true,
      total_runs: runs.length,
      items,
      generated_at: nowIso(),
    };
  }

  function listRunHistory(limit = 50) {
    const fp = runHistoryPath();
    if (!fs.existsSync(fp)) return [];
    const lines = fs.readFileSync(fp, "utf8").split(/\r?\n/).filter((x) => x.trim());
    return lines
      .slice(Math.max(0, lines.length - limit))
      .map((x) => {
        try {
          return JSON.parse(x);
        } catch {
          return null;
        }
      })
      .filter(Boolean)
      .reverse();
  }

  function findRunById(runId) {
    const rid = String(runId || "").trim();
    if (!rid) return null;
    const all = listRunHistory(2000);
    return all.find((x) => String(x?.run_id || "") === rid) || null;
  }

  function loadReviewQueue() {
    const fp = reviewQueuePath();
    if (!fs.existsSync(fp)) return [];
    try {
      const arr = JSON.parse(fs.readFileSync(fp, "utf8"));
      return Array.isArray(arr) ? arr : [];
    } catch {
      return [];
    }
  }

  function saveReviewQueue(items) {
    fs.writeFileSync(reviewQueuePath(), `${JSON.stringify(items || [], null, 2)}\n`, "utf8");
  }

  function enqueueReviews(items = []) {
    if (!Array.isArray(items) || !items.length) return;
    const q = loadReviewQueue();
    const byKey = new Map(q.map((x) => [`${x.run_id}::${x.review_key}`, x]));
    for (const it of items) {
      const key = `${it.run_id}::${it.review_key}`;
      byKey.set(key, {
        ...it,
        status: "pending",
      });
    }
    saveReviewQueue(Array.from(byKey.values()));
  }

  function appendReviewHistory(item) {
    try {
      fs.appendFileSync(reviewHistoryPath(), `${JSON.stringify(item)}\n`, "utf8");
    } catch {}
  }

  function listReviewHistory(limit = 200) {
    const fp = reviewHistoryPath();
    if (!fs.existsSync(fp)) return [];
    const lines = fs.readFileSync(fp, "utf8").split(/\r?\n/).filter((x) => x.trim());
    return lines
      .slice(Math.max(0, lines.length - limit))
      .map((x) => {
        try {
          return JSON.parse(x);
        } catch {
          return null;
        }
      })
      .filter(Boolean)
      .reverse();
  }

  function filterReviewHistory(items, filter) {
    const f = filter && typeof filter === "object" ? filter : {};
    const runId = String(f.run_id || "").trim();
    const reviewer = String(f.reviewer || "").trim().toLowerCase();
    const status = String(f.status || "").trim().toLowerCase();
    const dateFrom = String(f.date_from || "").trim();
    const dateTo = String(f.date_to || "").trim();
    return (items || []).filter((x) => {
      if (runId && String(x.run_id || "") !== runId) return false;
      if (reviewer && !String(x.reviewer || "").toLowerCase().includes(reviewer)) return false;
      if (status && String(x.status || "").toLowerCase() !== status) return false;
      if (dateFrom && String(x.decided_at || "") < dateFrom) return false;
      if (dateTo && String(x.decided_at || "") > dateTo) return false;
      return true;
    });
  }

  function buildRunCompare(runA, runB) {
    const a = findRunById(runA);
    const b = findRunById(runB);
    if (!a || !b) return { ok: false, error: "run not found" };
    const nodesA = Array.isArray(a?.result?.node_runs) ? a.result.node_runs : [];
    const nodesB = Array.isArray(b?.result?.node_runs) ? b.result.node_runs : [];
    const mapB = new Map(nodesB.map((n) => [String(n.id || ""), n]));
    const nodeDiff = nodesA.map((na) => {
      const nb = mapB.get(String(na.id || ""));
      const sa = String(na.status || "");
      const sb = String(nb?.status || "");
      const ta = Number(na.seconds || 0);
      const tb = Number(nb?.seconds || 0);
      return {
        id: na.id,
        type: na.type,
        status_a: sa,
        status_b: sb,
        status_changed: sa !== sb,
        seconds_a: ta,
        seconds_b: tb,
        seconds_delta: Number((tb - ta).toFixed(3)),
      };
    });
    const summary = {
      run_a: runA,
      run_b: runB,
      status_a: String(a?.result?.status || ""),
      status_b: String(b?.result?.status || ""),
      ok_a: !!a?.result?.ok,
      ok_b: !!b?.result?.ok,
      node_count_a: nodesA.length,
      node_count_b: nodesB.length,
      changed_nodes: nodeDiff.filter((x) => x.status_changed || Math.abs(Number(x.seconds_delta || 0)) > 0.001).length,
    };
    return { ok: true, summary, node_diff: nodeDiff };
  }

  function renderCompareMarkdown(out) {
    const s = out.summary || {};
    const rows = Array.isArray(out.node_diff) ? out.node_diff : [];
    const lines = [];
    lines.push("# AIWF 运行对比报告");
    lines.push("");
    lines.push(`- 生成时间: ${new Date().toISOString()}`);
    lines.push(`- Run A: ${s.run_a || "-"}`);
    lines.push(`- Run B: ${s.run_b || "-"}`);
    lines.push(`- 变化节点数: ${Number(s.changed_nodes || 0)}`);
    lines.push("");
    lines.push("| 节点 | 状态A | 状态B | 耗时A(s) | 耗时B(s) | Δ(s) |");
    lines.push("|---|---|---:|---:|---:|---:|");
    rows.forEach((r) => {
      lines.push(
        `| ${String(r.id || "")}(${String(r.type || "")}) | ${String(r.status_a || "")} | ${String(r.status_b || "")} | ${Number(r.seconds_a || 0).toFixed(3)} | ${Number(r.seconds_b || 0).toFixed(3)} | ${Number(r.seconds_delta || 0).toFixed(3)} |`
      );
    });
    return `${lines.join("\n")}\n`;
  }

  function renderCompareHtml(out) {
    const s = out.summary || {};
    const rows = Array.isArray(out.node_diff) ? out.node_diff : [];
    const tr = rows.map((r) => {
      const changed = r.status_changed || Math.abs(Number(r.seconds_delta || 0)) > 0.001;
      const bg = changed ? " style=\"background:#fff8f2\"" : "";
      return `<tr${bg}><td>${String(r.id || "")}(${String(r.type || "")})</td><td>${String(r.status_a || "")}</td><td>${String(r.status_b || "")}</td><td>${Number(r.seconds_a || 0).toFixed(3)}</td><td>${Number(r.seconds_b || 0).toFixed(3)}</td><td>${Number(r.seconds_delta || 0).toFixed(3)}</td></tr>`;
    }).join("");
    return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"/><title>AIWF 运行对比报告</title><style>body{font-family:"Segoe UI","Microsoft YaHei",sans-serif;padding:16px;color:#1f2d3d}table{border-collapse:collapse;width:100%}th,td{border:1px solid #d8e1ec;padding:6px 8px;font-size:13px}th{background:#f3f7fd;text-align:left}</style></head><body><h2>AIWF 运行对比报告</h2><p>生成时间: ${new Date().toISOString()}<br/>Run A: ${s.run_a || "-"}<br/>Run B: ${s.run_b || "-"}<br/>变化节点数: ${Number(s.changed_nodes || 0)}</p><table><thead><tr><th>节点</th><th>状态A</th><th>状态B</th><th>耗时A(s)</th><th>耗时B(s)</th><th>Δ(s)</th></tr></thead><tbody>${tr}</tbody></table></body></html>`;
  }

  function renderPreflightMarkdown(rep) {
    const r = rep && typeof rep === "object" ? rep : {};
    const issues = Array.isArray(r.issues) ? r.issues : [];
    const risk = r && typeof r.risk === "object" ? r.risk : null;
    const lines = [];
    lines.push("# AIWF 运行前预检报告");
    lines.push("");
    lines.push(`- 生成时间: ${new Date().toISOString()}`);
    lines.push(`- 预检时间: ${String(r.ts || "") || "-"}`);
    lines.push(`- 是否通过: ${r.ok ? "true" : "false"}`);
    if (risk) lines.push(`- 风险等级: ${String(risk.label || "")} (${Number(risk.score || 0)}/100)`);
    lines.push(`- 问题数: ${issues.length}`);
    lines.push("");
    lines.push("| 级别 | 类型 | 节点ID | 说明 |");
    lines.push("|---|---|---|---|");
    issues.forEach((it) => {
      lines.push(`| ${String(it.level || "")} | ${String(it.kind || "")} | ${String(it.node_id || "")} | ${String(it.message || "").replace(/\|/g, "\\|")} |`);
    });
    return `${lines.join("\n")}\n`;
  }

  function renderTemplateAcceptanceMarkdown(rep) {
    const r = rep && typeof rep === "object" ? rep : {};
    const before = r.before && typeof r.before === "object" ? r.before : {};
    const after = r.after && typeof r.after === "object" ? r.after : {};
    const fix = r.auto_fix && typeof r.auto_fix === "object" ? r.auto_fix : {};
    const lines = [];
    lines.push("# AIWF 模板验收报告");
    lines.push("");
    lines.push(`- 生成时间: ${new Date().toISOString()}`);
    lines.push(`- 模板ID: ${String(r.template_id || "-")}`);
    lines.push(`- 模板名称: ${String(r.template_name || "-")}`);
    lines.push(`- 验收结论: ${r.accepted ? "通过" : "未通过"}`);
    lines.push(`- 预检前: ${before.ok ? "通过" : "未通过"} / 风险 ${Number(before?.risk?.score || 0)}/100`);
    lines.push(`- 预检后: ${after.ok ? "通过" : "未通过"} / 风险 ${Number(after?.risk?.score || 0)}/100`);
    lines.push(`- 自动修复: 重复连线 ${Number(fix.removed_dup_edges || 0)}，自环 ${Number(fix.removed_self_loops || 0)}，断裂连线 ${Number(fix.removed_broken_edges || 0)}，孤立节点 ${Number(fix.removed_isolated_nodes || 0)}`);
    return `${lines.join("\n")}\n`;
  }

  function applyQualityRuleSetToPayload(payload) {
    const p = payload && typeof payload === "object" ? { ...payload } : {};
    const workflow = p.workflow && typeof p.workflow === "object" ? deepClone(p.workflow) : null;
    const qualityRuleSetId = String(p?.quality_rule_set_id || "").trim();
    if (!workflow || !qualityRuleSetId) return p;
    const sets = listQualityRuleCenter();
    const hit = sets.find((x) => String(x?.id || "") === qualityRuleSetId);
    if (!hit || !hit.rules || typeof hit.rules !== "object") return p;
    const targetTypes = new Set(["quality_check_v2", "quality_check_v3", "quality_check_v4"]);
    const nodes = Array.isArray(workflow?.nodes) ? workflow.nodes : [];
    nodes.forEach((n) => {
      const t = String(n?.type || "");
      if (!targetTypes.has(t)) return;
      const cfg = n?.config && typeof n.config === "object" ? n.config : {};
      n.config = {
        ...cfg,
        rules: deepClone(hit.rules),
        rule_set_meta: {
          id: String(hit.id || ""),
          name: String(hit.name || ""),
          version: String(hit.version || "v1"),
        },
      };
    });
    p.workflow = workflow;
    return p;
  }

  function buildRunRegressionAgainstBaseline(runId, baselineId) {
    const cur = findRunById(runId);
    if (!cur) return { ok: false, error: `run not found: ${runId}` };
    const bases = listRunBaselines();
    const hit = bases.find((x) => String(x?.baseline_id || "") === String(baselineId || ""));
    if (!hit) return { ok: false, error: `baseline not found: ${baselineId}` };
    const baseRunId = String(hit?.run_id || "");
    const cmp = buildRunCompare(baseRunId, runId);
    if (!cmp?.ok) return cmp;
    const changed = Array.isArray(cmp.node_diff) ? cmp.node_diff.filter((x) => x.status_changed || Math.abs(Number(x.seconds_delta || 0)) > 0.001) : [];
    const statusFlip = changed.filter((x) => x.status_changed);
    const perfHot = changed.filter((x) => Number(x.seconds_delta || 0) > 0.5);
    return {
      ok: true,
      baseline_id: String(hit.baseline_id || ""),
      baseline_name: String(hit.name || ""),
      baseline_run_id: baseRunId,
      run_id: runId,
      compare: cmp,
      regression: {
        changed_nodes: changed.length,
        status_flip_nodes: statusFlip.length,
        perf_hot_nodes: perfHot.length,
        status_flip: statusFlip,
        perf_hot: perfHot,
      },
    };
  }

  ipcMain.handle("aiwf:openWorkflowStudio", async () => {
    createWorkflowWindow();
    return { ok: true };
  });

  ipcMain.handle("aiwf:runWorkflow", async (_evt, payload, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    const effectivePayload = applyQualityRuleSetToPayload(applySandboxAutoFixPayload(payload || {}));
    const out = attachQualityGate(await runMinimalWorkflow({
      payload: effectivePayload,
      config: merged,
      outputRoot: resolveOutputRoot(merged),
      nodeCache: createNodeCacheApi(),
    }), effectivePayload || {});
    appendDiagnostics(out);
    appendRunHistory(out, effectivePayload, merged);
    extractSandboxViolations(out).forEach((v) => appendSandboxViolationAudit(v, effectivePayload || {}));
    maybeApplySandboxAutoFix(out, effectivePayload || {});
    appendAudit("run_workflow", {
      run_id: String(out?.run_id || ""),
      workflow_id: String(out?.workflow_id || ""),
      ok: !!out?.ok,
      status: String(out?.status || ""),
    });
    if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) enqueueReviews(out.pending_reviews);
    return out;
  });

  ipcMain.handle("aiwf:getWorkflowDiagnostics", async (_evt, opts) => {
    const limit = Number(opts?.limit || 50);
    const safe = Number.isFinite(limit) ? Math.max(1, Math.min(500, Math.floor(limit))) : 50;
    return readDiagnostics(safe);
  });

  ipcMain.handle("aiwf:getWorkflowPerfDashboard", async (_evt, opts) => {
    const limit = Number(opts?.limit || 200);
    const safe = Number.isFinite(limit) ? Math.max(10, Math.min(2000, Math.floor(limit))) : 200;
    return buildPerfDashboard(safe);
  });

  ipcMain.handle("aiwf:listWorkflowRuns", async (_evt, opts) => {
    const limit = Number(opts?.limit || 50);
    const safe = Number.isFinite(limit) ? Math.max(1, Math.min(500, Math.floor(limit))) : 50;
    return { ok: true, items: listRunHistory(safe) };
  });

  ipcMain.handle("aiwf:getWorkflowLineage", async (_evt, req) => {
    try {
      const runId = String(req?.run_id || "").trim();
      const hit = findRunById(runId);
      if (!hit) return { ok: false, error: "run not found" };
      const lineage = hit?.result?.lineage && typeof hit.result.lineage === "object" ? hit.result.lineage : {};
      return { ok: true, run_id: runId, lineage };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:replayWorkflowRun", async (_evt, req, cfg) => {
    try {
      const runId = String(req?.run_id || "").trim();
      const nodeId = String(req?.node_id || "").trim();
      const found = findRunById(runId);
      if (!found) return { ok: false, error: `run not found: ${runId}` };
      const basePayload = found.payload && typeof found.payload === "object" ? found.payload : {};
      const replayPayload = {
        ...basePayload,
        manual_review: req?.manual_review && typeof req.manual_review === "object"
          ? req.manual_review
          : (basePayload.manual_review || {}),
        resume: {
          run_id: runId,
          node_id: nodeId,
          outputs: found?.result?.node_outputs || {},
        },
      };
      const merged = { ...loadConfig(), ...(found.config || {}), ...(cfg || {}) };
      const effectivePayload = applyQualityRuleSetToPayload(applySandboxAutoFixPayload(replayPayload));
      const out = attachQualityGate(await runMinimalWorkflow({
        payload: effectivePayload,
        config: merged,
        outputRoot: resolveOutputRoot(merged),
        nodeCache: createNodeCacheApi(),
      }), effectivePayload || {});
      appendDiagnostics(out);
      appendRunHistory(out, effectivePayload, merged);
      extractSandboxViolations(out).forEach((v) => appendSandboxViolationAudit(v, effectivePayload || {}));
      maybeApplySandboxAutoFix(out, effectivePayload || {});
      if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) enqueueReviews(out.pending_reviews);
      return { ok: true, replay_of: runId, resumed_from: nodeId || null, result: out };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:compareWorkflowRuns", async (_evt, req) => {
    try {
      const runA = String(req?.run_a || "").trim();
      const runB = String(req?.run_b || "").trim();
      return buildRunCompare(runA, runB);
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:listRunBaselines", async () => {
    return { ok: true, items: listRunBaselines() };
  });

  ipcMain.handle("aiwf:saveRunBaseline", async (_evt, req) => {
    try {
      const runId = String(req?.run_id || "").trim();
      if (!runId) return { ok: false, error: "run_id required" };
      const hit = findRunById(runId);
      if (!hit) return { ok: false, error: "run not found" };
      const items = listRunBaselines();
      const baselineId = String(req?.baseline_id || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`);
      const row = {
        baseline_id: baselineId,
        name: String(req?.name || hit?.workflow_id || "baseline"),
        run_id: runId,
        workflow_id: String(hit?.workflow_id || ""),
        created_at: nowIso(),
        notes: String(req?.notes || ""),
      };
      const idx = items.findIndex((x) => String(x?.baseline_id || "") === baselineId);
      if (idx >= 0) items[idx] = row;
      else items.unshift(row);
      saveRunBaselines(items);
      appendAudit("baseline_save", { baseline_id: baselineId, run_id: runId });
      return { ok: true, item: row };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:compareRunWithBaseline", async (_evt, req) => {
    try {
      const runId = String(req?.run_id || "").trim();
      const baselineId = String(req?.baseline_id || "").trim();
      return buildRunRegressionAgainstBaseline(runId, baselineId);
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:exportCompareReport", async (_evt, req) => {
    try {
      const runA = String(req?.run_a || "").trim();
      const runB = String(req?.run_b || "").trim();
      const format = String(req?.format || "md").trim().toLowerCase() === "html" ? "html" : "md";
      const out = buildRunCompare(runA, runB);
      if (!out?.ok) return out;
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        filePath = String(req.path);
      } else {
        const defaultName = `aiwf_compare_${runA.slice(0, 8)}_${runB.slice(0, 8)}.${format}`;
        const pick = await dialog.showSaveDialog({
          title: "导出运行对比报告",
          defaultPath: path.join(app.getPath("documents"), defaultName),
          filters: format === "html" ? [{ name: "HTML", extensions: ["html"] }] : [{ name: "Markdown", extensions: ["md"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      const content = format === "html" ? renderCompareHtml(out) : renderCompareMarkdown(out);
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, content, "utf8");
      return { ok: true, path: filePath, format };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:listTemplateMarketplace", async (_evt, req) => {
    const limit = Number(req?.limit || 500);
    return { ok: true, items: listTemplateMarketplace(limit) };
  });

  ipcMain.handle("aiwf:installTemplatePack", async (_evt, req) => {
    try {
      const inPack = req?.pack && typeof req.pack === "object" ? req.pack : null;
      const fromPath = String(req?.path || "").trim();
      let pack = inPack;
      if (!pack && fromPath) {
        const obj = JSON.parse(fs.readFileSync(fromPath, "utf8"));
        pack = obj && typeof obj === "object" ? obj : null;
      }
      if (!pack) return { ok: false, error: "pack or path required" };
      const templates = Array.isArray(pack.templates) ? pack.templates : [];
      if (!templates.length) return { ok: false, error: "templates required" };
      const id = String(pack.id || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`);
      const item = {
        id,
        name: String(pack.name || id),
        version: String(pack.version || "v1"),
        source: fromPath || "inline",
        templates,
        created_at: nowIso(),
      };
      const items = listTemplateMarketplace(5000);
      const idx = items.findIndex((x) => String(x?.id || "") === id);
      if (idx >= 0) items[idx] = item;
      else items.unshift(item);
      saveTemplateMarketplace(items);
      appendAudit("template_pack_install", { id, name: item.name, templates: templates.length });
      return { ok: true, item };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:removeTemplatePack", async (_evt, req) => {
    try {
      const id = String(req?.id || "").trim();
      if (!id) return { ok: false, error: "id required" };
      const items = listTemplateMarketplace(5000);
      const next = items.filter((x) => String(x?.id || "") !== id);
      saveTemplateMarketplace(next);
      appendAudit("template_pack_remove", { id });
      return { ok: true };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:exportTemplatePack", async (_evt, req) => {
    try {
      const id = String(req?.id || "").trim();
      if (!id) return { ok: false, error: "id required" };
      const hit = listTemplateMarketplace(5000).find((x) => String(x?.id || "") === id);
      if (!hit) return { ok: false, error: "template pack not found" };
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        filePath = String(req.path);
      } else {
        const pick = await dialog.showSaveDialog({
          title: "导出模板包",
          defaultPath: path.join(app.getPath("documents"), `template_pack_${id}.json`),
          filters: [{ name: "JSON", extensions: ["json"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, `${JSON.stringify(hit, null, 2)}\n`, "utf8");
      return { ok: true, path: filePath };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:listQualityRuleSets", async () => {
    return { ok: true, sets: listQualityRuleCenter() };
  });

  ipcMain.handle("aiwf:saveQualityRuleSet", async (_evt, req) => {
    try {
      const set = req?.set && typeof req.set === "object" ? req.set : null;
      if (!set) return { ok: false, error: "set required" };
      const id = String(set.id || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`);
      const row = {
        id,
        name: String(set.name || id),
        version: String(set.version || "v1"),
        rules: set.rules && typeof set.rules === "object" ? set.rules : {},
        scope: String(set.scope || "generic"),
        updated_at: nowIso(),
      };
      const sets = listQualityRuleCenter();
      const idx = sets.findIndex((x) => String(x?.id || "") === id);
      if (idx >= 0) sets[idx] = row;
      else sets.unshift(row);
      saveQualityRuleCenter(sets);
      appendAudit("quality_rule_set_save", { id, name: row.name, scope: row.scope });
      return { ok: true, set: row };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:removeQualityRuleSet", async (_evt, req) => {
    try {
      const id = String(req?.id || "").trim();
      if (!id) return { ok: false, error: "id required" };
      const sets = listQualityRuleCenter().filter((x) => String(x?.id || "") !== id);
      saveQualityRuleCenter(sets);
      appendAudit("quality_rule_set_remove", { id });
      return { ok: true };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:exportWorkflowPreflightReport", async (_evt, req) => {
    try {
      const report = req?.report && typeof req.report === "object" ? req.report : {};
      const format = String(req?.format || "md").trim().toLowerCase() === "json" ? "json" : "md";
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        filePath = String(req.path);
      } else {
        const defaultName = `aiwf_preflight_${Date.now()}.${format}`;
        const pick = await dialog.showSaveDialog({
          title: "导出预检报告",
          defaultPath: path.join(app.getPath("documents"), defaultName),
          filters: format === "json" ? [{ name: "JSON", extensions: ["json"] }] : [{ name: "Markdown", extensions: ["md"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      const content = format === "json"
        ? `${JSON.stringify(report, null, 2)}\n`
        : renderPreflightMarkdown(report);
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, content, "utf8");
      appendAudit("preflight_export", {
        format,
        path: filePath,
        issue_count: Array.isArray(report?.issues) ? report.issues.length : 0,
      });
      return { ok: true, path: filePath, format };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:exportWorkflowTemplateAcceptanceReport", async (_evt, req) => {
    try {
      const report = req?.report && typeof req.report === "object" ? req.report : {};
      const format = String(req?.format || "md").trim().toLowerCase() === "json" ? "json" : "md";
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        filePath = String(req.path);
      } else {
        const defaultName = `aiwf_template_acceptance_${Date.now()}.${format}`;
        const pick = await dialog.showSaveDialog({
          title: "导出模板验收报告",
          defaultPath: path.join(app.getPath("documents"), defaultName),
          filters: format === "json" ? [{ name: "JSON", extensions: ["json"] }] : [{ name: "Markdown", extensions: ["md"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      const content = format === "json"
        ? `${JSON.stringify(report, null, 2)}\n`
        : renderTemplateAcceptanceMarkdown(report);
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, content, "utf8");
      appendAudit("template_acceptance_export", {
        format,
        path: filePath,
        template_id: String(report?.template_id || ""),
        accepted: !!report?.accepted,
      });
      return { ok: true, path: filePath, format };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:listManualReviews", async () => {
    return { ok: true, items: loadReviewQueue() };
  });

  ipcMain.handle("aiwf:listManualReviewHistory", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    const safe = Number.isFinite(limit) ? Math.max(1, Math.min(2000, Math.floor(limit))) : 200;
    const items = listReviewHistory(safe);
    return { ok: true, items: filterReviewHistory(items, req?.filter || {}) };
  });

  ipcMain.handle("aiwf:exportManualReviewHistory", async (_evt, req) => {
    try {
      const items = filterReviewHistory(listReviewHistory(5000), req?.filter || {});
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        filePath = String(req.path);
      } else {
        const out = await dialog.showSaveDialog({
          title: "导出审核历史",
          defaultPath: path.join(app.getPath("documents"), "aiwf_manual_review_history.json"),
          filters: [{ name: "JSON", extensions: ["json"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (out.canceled || !out.filePath) return { ok: false, canceled: true };
        filePath = out.filePath;
      }
      const payload = {
        exported_at: new Date().toISOString(),
        total: items.length,
        items,
      };
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      return { ok: true, path: filePath, total: items.length };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:submitManualReview", async (_evt, req) => {
    try {
      const runId = String(req?.run_id || "").trim();
      const reviewKey = String(req?.review_key || "").trim();
      const approved = !!req?.approved;
      const reviewer = String(req?.reviewer || "reviewer").trim();
      const comment = String(req?.comment || "").trim();
      const q = loadReviewQueue();
      const idx = q.findIndex((x) => String(x.run_id || "") === runId && String(x.review_key || "") === reviewKey);
      if (idx < 0) return { ok: false, error: "review task not found" };
      const task = q[idx];
      q.splice(idx, 1);
      saveReviewQueue(q);
      const hist = {
        ...task,
        approved,
        reviewer,
        comment,
        status: approved ? "approved" : "rejected",
        decided_at: new Date().toISOString(),
      };
      appendReviewHistory(hist);
      let resumed = null;
      const autoResume = req?.auto_resume !== false;
      if (autoResume) {
        const found = findRunById(runId);
        if (found) {
          const basePayload = found.payload && typeof found.payload === "object" ? found.payload : {};
          const replayPayload = {
            ...basePayload,
            manual_review: {
              ...(basePayload.manual_review && typeof basePayload.manual_review === "object" ? basePayload.manual_review : {}),
              [reviewKey]: {
                approved,
                reviewer,
                comment,
              },
            },
            resume: {
              run_id: runId,
              node_id: String(task.node_id || reviewKey || ""),
              outputs: found?.result?.node_outputs || {},
            },
          };
          const merged = { ...loadConfig(), ...(found.config || {}) };
          const effectivePayload = applyQualityRuleSetToPayload(applySandboxAutoFixPayload(replayPayload));
          const out = attachQualityGate(await runMinimalWorkflow({
            payload: effectivePayload,
            config: merged,
            outputRoot: resolveOutputRoot(merged),
            nodeCache: createNodeCacheApi(),
          }), effectivePayload || {});
          appendDiagnostics(out);
          appendRunHistory(out, effectivePayload, merged);
          extractSandboxViolations(out).forEach((v) => appendSandboxViolationAudit(v, effectivePayload || {}));
          maybeApplySandboxAutoFix(out, effectivePayload || {});
          if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) enqueueReviews(out.pending_reviews);
          resumed = out;
        }
      }
      return { ok: true, item: hist, remaining: q.length, resumed };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:enqueueWorkflowTask", async (_evt, req) => {
    try {
      const item = {
        task_id: String((req && req.task_id) || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`),
        label: String((req && req.label) || "workflow_task"),
        payload: applyQualityRuleSetToPayload((req && req.payload) || {}),
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
    } catch (e) {
      return { ok: false, error: String(e) };
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
      const idx = items.findIndex((x) => String(x.task_id || "") === id);
      if (idx < 0) return { ok: false, error: "task not found" };
      const st = String(items[idx].status || "");
      if (st === "running") return { ok: false, error: "task is running; cancel unsupported" };
      items[idx].status = "canceled";
      items[idx].finished_at = nowIso();
      saveWorkflowQueue(items);
      appendAudit("queue_cancel", { task_id: id });
      return { ok: true };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:retryWorkflowTask", async (_evt, req) => {
    try {
      const id = String(req?.task_id || "").trim();
      if (!id) return { ok: false, error: "task_id required" };
      const items = loadWorkflowQueue();
      const idx = items.findIndex((x) => String(x.task_id || "") === id);
      if (idx < 0) return { ok: false, error: "task not found" };
      const cur = items[idx];
      const retry = {
        ...cur,
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
    } catch (e) {
      return { ok: false, error: String(e) };
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
      const hit = items.find((x) => String(x.version_id || "") === versionId);
      if (!hit) return { ok: false, error: "version not found" };
      appendAudit("workflow_restore_version", { version_id: versionId, workflow_name: String(hit.workflow_name || "") });
      return { ok: true, graph: hit.graph, meta: hit };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:compareWorkflowVersions", async (_evt, req) => {
    try {
      const versionA = String(req?.version_a || "").trim();
      const versionB = String(req?.version_b || "").trim();
      return compareVersions(versionA, versionB);
    } catch (e) {
      return { ok: false, error: String(e) };
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
      const idx = list.findIndex((x) => String(x.app_id || "") === appId);
      if (idx >= 0) list[idx] = { ...list[idx], ...item };
      else list.unshift(item);
      saveWorkflowApps(list);
      appendAudit("workflow_publish_app", { app_id: appId, name });
      return { ok: true, item };
    } catch (e) {
      return { ok: false, error: String(e) };
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
      const item = listWorkflowApps(5000).find((x) => String(x.app_id || "") === appId);
      if (!item) return { ok: false, error: "workflow app not found" };
      const params = req?.params && typeof req.params === "object" ? req.params : {};
      const mergedPayload = req?.payload && typeof req.payload === "object" ? { ...req.payload } : {};
      mergedPayload.workflow = applyTemplateParams(item.graph || {}, params);
      if (!mergedPayload.workflow.workflow_id) mergedPayload.workflow.workflow_id = item.workflow_id || "custom";
      const merged = { ...loadConfig(), ...(cfg || {}), ...(req?.cfg || {}) };
      const effectivePayload = applyQualityRuleSetToPayload(applySandboxAutoFixPayload(mergedPayload));
      const out = attachQualityGate(await runMinimalWorkflow({
        payload: effectivePayload,
        config: merged,
        outputRoot: resolveOutputRoot(merged),
        nodeCache: createNodeCacheApi(),
      }), effectivePayload || {});
      appendDiagnostics(out);
      appendRunHistory(out, effectivePayload, merged);
      extractSandboxViolations(out).forEach((v) => appendSandboxViolationAudit(v, effectivePayload || {}));
      maybeApplySandboxAutoFix(out, effectivePayload || {});
      if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) enqueueReviews(out.pending_reviews);
      appendAudit("workflow_run_app", { app_id: appId, run_id: String(out?.run_id || ""), ok: !!out?.ok });
      return { ok: true, app_id: appId, result: out };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:getWorkflowRunTimeline", async (_evt, req) => {
    return runTimeline(String(req?.run_id || "").trim());
  });

  ipcMain.handle("aiwf:getWorkflowFailureSummary", async (_evt, req) => {
    const limit = Number(req?.limit || 400);
    return failureSummary(limit);
  });

  ipcMain.handle("aiwf:getWorkflowSandboxAlerts", async (_evt, req) => {
    const limit = Number(req?.limit || 400);
    const dedupWindowSec = Number.isFinite(Number(req?.dedup_window_sec))
      ? Math.max(0, Math.floor(Number(req?.dedup_window_sec)))
      : sandboxAlertDedupWindowSec(req || {});
    return sandboxAlerts(limit, req?.thresholds || null, dedupWindowSec);
  });

  ipcMain.handle("aiwf:getWorkflowSandboxAlertRules", async () => {
    return { ok: true, rules: loadSandboxAlertRules() };
  });

  ipcMain.handle("aiwf:setWorkflowSandboxAlertRules", async (_evt, req) => {
    try {
      const incoming = req?.rules && typeof req.rules === "object" ? req.rules : {};
      const next = normalizeSandboxAlertRules(incoming);
      saveSandboxAlertRules(next);
      const ver = appendSandboxRuleVersion(next, { reason: "set_rules" });
      appendAudit("sandbox_alert_rules_set", {
        version_id: String(ver?.version_id || ""),
        whitelist_codes: next.whitelist_codes.length,
        whitelist_node_types: next.whitelist_node_types.length,
        whitelist_keys: next.whitelist_keys.length,
        mute_keys: Object.keys(next.mute_until_by_key || {}).length,
      });
      return { ok: true, rules: next };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:muteWorkflowSandboxAlert", async (_evt, req) => {
    try {
      const nodeType = String(req?.node_type || "*").trim().toLowerCase() || "*";
      const nodeId = String(req?.node_id || "*").trim().toLowerCase() || "*";
      const code = String(req?.code || "*").trim().toLowerCase() || "*";
      const minutes = Number(req?.minutes || 60);
      const mins = Number.isFinite(minutes) ? Math.max(1, Math.floor(minutes)) : 60;
      const key = `${nodeType}::${nodeId}::${code}`;
      const rules = loadSandboxAlertRules();
      rules.mute_until_by_key[key] = new Date(Date.now() + mins * 60000).toISOString();
      const next = normalizeSandboxAlertRules(rules);
      saveSandboxAlertRules(next);
      const ver = appendSandboxRuleVersion(next, { reason: "mute", key, minutes: mins });
      appendAudit("sandbox_alert_muted", { key, minutes: mins });
      return { ok: true, key, mute_until: next.mute_until_by_key[key], version_id: String(ver?.version_id || ""), rules: next };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:listWorkflowSandboxRuleVersions", async (_evt, req) => {
    const limit = Number(req?.limit || 100);
    return { ok: true, items: listSandboxRuleVersions(limit) };
  });

  ipcMain.handle("aiwf:compareWorkflowSandboxRuleVersions", async (_evt, req) => {
    return compareSandboxRuleVersions(String(req?.version_a || ""), String(req?.version_b || ""));
  });

  ipcMain.handle("aiwf:rollbackWorkflowSandboxRuleVersion", async (_evt, req) => {
    try {
      const versionId = String(req?.version_id || "").trim();
      if (!versionId) return { ok: false, error: "version_id required" };
      const hit = listSandboxRuleVersions(5000).find((x) => String(x.version_id || "") === versionId);
      if (!hit) return { ok: false, error: "rule version not found" };
      const rules = normalizeSandboxAlertRules(hit.rules || {});
      saveSandboxAlertRules(rules);
      const ver = appendSandboxRuleVersion(rules, { reason: "rollback", from_version_id: versionId });
      appendAudit("sandbox_alert_rules_rollback", { from_version_id: versionId, new_version_id: String(ver?.version_id || "") });
      return { ok: true, rules, version_id: String(ver?.version_id || "") };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:getWorkflowSandboxAutoFixState", async () => {
    return { ok: true, state: loadSandboxAutoFixState() };
  });

  ipcMain.handle("aiwf:listWorkflowSandboxAutoFixActions", async (_evt, req) => {
    const limit = Number(req?.limit || 100);
    const safe = Number.isFinite(limit) ? Math.max(1, Math.min(1000, Math.floor(limit))) : 100;
    const st = loadSandboxAutoFixState();
    const items = Array.isArray(st.last_actions) ? st.last_actions.slice().reverse().slice(0, safe) : [];
    return {
      ok: true,
      items,
      forced_isolation_mode: String(st.forced_isolation_mode || ""),
      forced_until: String(st.forced_until || ""),
    };
  });

  ipcMain.handle("aiwf:exportWorkflowSandboxAuditReport", async (_evt, req) => {
    try {
      const limit = Number(req?.limit || 500);
      const thresholds = req?.thresholds && typeof req.thresholds === "object" ? req.thresholds : {};
      const dedupWindowSec = Number.isFinite(Number(req?.dedup_window_sec))
        ? Math.max(0, Math.floor(Number(req?.dedup_window_sec)))
        : sandboxAlertDedupWindowSec(req || {});
      const format = String(req?.format || "md").trim().toLowerCase() === "json" ? "json" : "md";
      const data = sandboxAlerts(limit, thresholds, dedupWindowSec);
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        filePath = String(req.path);
      } else {
        const defaultName = `aiwf_sandbox_audit_${Date.now()}.${format}`;
        const pick = await dialog.showSaveDialog({
          title: "导出Sandbox审计报告",
          defaultPath: path.join(app.getPath("documents"), defaultName),
          filters: format === "json" ? [{ name: "JSON", extensions: ["json"] }] : [{ name: "Markdown", extensions: ["md"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      if (format === "json") {
        fs.writeFileSync(filePath, `${JSON.stringify({ exported_at: nowIso(), ...data }, null, 2)}\n`, "utf8");
      } else {
        const lines = [];
        lines.push("# AIWF Sandbox 审计报告");
        lines.push("");
        lines.push(`- 导出时间: ${nowIso()}`);
        lines.push(`- 告警总数: ${Number(data.total || 0)}`);
        lines.push(`- 健康等级: ${String(data?.health?.level || "green")}`);
        lines.push(`- 阈值: yellow=${Number(data?.health?.thresholds?.yellow || 1)}, red=${Number(data?.health?.thresholds?.red || 3)}`);
        lines.push("");
        lines.push("| 节点 | 次数 | 最近Run |");
        lines.push("|---|---:|---|");
        (Array.isArray(data.by_node) ? data.by_node : []).forEach((x) => {
          lines.push(`| ${String(x.node_type || "")}(${String(x.node_id || "")}) | ${Number(x.count || 0)} | ${String(x.last_run_id || "").slice(0, 12)} |`);
        });
        lines.push("");
        fs.writeFileSync(filePath, `${lines.join("\n")}\n`, "utf8");
      }
      appendAudit("sandbox_audit_export", { path: filePath, format, total: Number(data.total || 0) });
      return { ok: true, path: filePath, format, total: Number(data.total || 0), health: data.health };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:exportWorkflowSandboxPreset", async (_evt, req) => {
    try {
      const preset = req?.preset && typeof req.preset === "object" ? req.preset : {};
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        filePath = String(req.path);
      } else {
        const pick = await dialog.showSaveDialog({
          title: "导出Sandbox预设",
          defaultPath: path.join(app.getPath("documents"), `aiwf_sandbox_preset_${Date.now()}.json`),
          filters: [{ name: "JSON", extensions: ["json"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      const payload = { exported_at: nowIso(), preset };
      fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      appendAudit("sandbox_preset_export", { path: filePath });
      return { ok: true, path: filePath };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:importWorkflowSandboxPreset", async (_evt, req) => {
    try {
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      let filePaths = [];
      if (req?.mock && req?.path && allowMockIo) {
        filePaths = [String(req.path)];
      } else {
        const out = await dialog.showOpenDialog({
          title: "导入Sandbox预设",
          filters: [{ name: "JSON", extensions: ["json"] }],
          properties: ["openFile"],
        });
        if (out.canceled || !out.filePaths || !out.filePaths.length) return { ok: false, canceled: true };
        filePaths = out.filePaths;
      }
      const p = filePaths[0];
      const obj = JSON.parse(fs.readFileSync(p, "utf8"));
      const preset = obj?.preset && typeof obj.preset === "object" ? obj.preset : {};
      appendAudit("sandbox_preset_import", { path: p });
      return { ok: true, path: p, preset };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:listWorkflowAuditLogs", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    const action = String(req?.action || "");
    return { ok: true, items: listAudit(limit, action) };
  });

  ipcMain.handle("aiwf:listWorkflowQualityGateReports", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    const filter = req?.filter && typeof req.filter === "object" ? req.filter : {};
    return { ok: true, items: listQualityGateReports(limit, filter) };
  });

  ipcMain.handle("aiwf:exportWorkflowQualityGateReports", async (_evt, req) => {
    try {
      const limit = Number(req?.limit || 300);
      const format = String(req?.format || "md").trim().toLowerCase() === "json" ? "json" : "md";
      const filter = req?.filter && typeof req.filter === "object" ? req.filter : {};
      const items = listQualityGateReports(limit, filter);
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        filePath = String(req.path);
      } else {
        const defaultName = `aiwf_quality_gate_${Date.now()}.${format}`;
        const pick = await dialog.showSaveDialog({
          title: "导出质量门禁报告",
          defaultPath: path.join(app.getPath("documents"), defaultName),
          filters: format === "json" ? [{ name: "JSON", extensions: ["json"] }] : [{ name: "Markdown", extensions: ["md"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      if (format === "json") {
        fs.writeFileSync(filePath, `${JSON.stringify({ exported_at: nowIso(), filter, total: items.length, items }, null, 2)}\n`, "utf8");
      } else {
        const lines = [];
        lines.push("# AIWF 质量门禁报告");
        lines.push("");
        lines.push(`- 导出时间: ${nowIso()}`);
        lines.push(`- 数量: ${items.length}`);
        lines.push(`- 筛选 run_id: ${String(filter?.run_id || "(none)")}`);
        lines.push(`- 筛选 status: ${String(filter?.status || "all")}`);
        lines.push("");
        lines.push("| Run | 状态 | 问题 | 时间 |");
        lines.push("|---|---|---|---|");
        items.forEach((it) => {
          const gate = it?.quality_gate && typeof it.quality_gate === "object" ? it.quality_gate : {};
          const status = gate.blocked ? "blocked" : (gate.passed ? "pass" : "unknown");
          const issues = Array.isArray(gate.issues) ? gate.issues.join(";") : "";
          lines.push(`| ${String(it.run_id || "").slice(0, 12)} | ${status} | ${issues} | ${String(it.ts || "")} |`);
        });
        fs.writeFileSync(filePath, `${lines.join("\n")}\n`, "utf8");
      }
      appendAudit("quality_gate_export", { path: filePath, format, total: items.length, filter });
      return { ok: true, path: filePath, total: items.length, format };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:saveWorkflow", async (_evt, graph, name, opts) => {
    try {
      const options = opts && typeof opts === "object" ? opts : {};
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      const safeName = String(name || "workflow")
        .replace(/[\\/:*?"<>|]/g, "_")
        .replace(/\s+/g, "_")
        .slice(0, 80) || "workflow";
      let canceled = false;
      let filePath = "";
      if (options.mock && options.path && allowMockIo) {
        filePath = String(options.path);
      } else {
        const out = await dialog.showSaveDialog({
          title: "保存流程",
          defaultPath: path.join(app.getPath("documents"), `${safeName}.json`),
          filters: [{ name: "Workflow JSON", extensions: ["json"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        canceled = !!out.canceled;
        filePath = out.filePath || "";
      }
      if (canceled || !filePath) return { ok: false, canceled: true };
      const payload = graph && typeof graph === "object" ? graph : {};
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      appendWorkflowVersion({
        version_id: `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`,
        ts: nowIso(),
        workflow_name: String(payload?.name || safeName),
        workflow_id: String(payload?.workflow_id || "custom"),
        path: filePath,
        graph: payload,
      });
      appendAudit("workflow_save", { path: filePath, workflow_name: String(payload?.name || safeName) });
      return { ok: true, path: filePath };
    } catch (e) {
      return { ok: false, canceled: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:loadWorkflow", async (_evt, opts) => {
    try {
      const options = opts && typeof opts === "object" ? opts : {};
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      let canceled = false;
      let filePaths = [];
      if (options.mock && options.path && allowMockIo) {
        filePaths = [String(options.path)];
      } else {
        const out = await dialog.showOpenDialog({
          title: "加载流程",
          filters: [{ name: "Workflow JSON", extensions: ["json"] }],
          properties: ["openFile"],
        });
        canceled = !!out.canceled;
        filePaths = out.filePaths || [];
      }
      if (canceled || !filePaths || !filePaths.length) return { ok: false, canceled: true };
      const p = filePaths[0];
      const obj = JSON.parse(fs.readFileSync(p, "utf8"));
      appendAudit("workflow_load", { path: p });
      return { ok: true, canceled: false, path: p, graph: obj };
    } catch (e) {
      return { ok: false, canceled: false, error: String(e) };
    }
  });
}

module.exports = {
  registerWorkflowIpc,
};


