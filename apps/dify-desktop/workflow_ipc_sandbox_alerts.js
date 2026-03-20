function createWorkflowSandboxSupport(deps) {
  const {
    fs,
    readJsonFile,
    writeJsonFile,
    nowIso,
    appendAudit,
    extractSandboxViolations,
    sandboxAlertRuleVersionsPath,
    sandboxAlertRulesPath,
    sandboxAlertStatePath,
    sandboxAutoFixStatePath,
    workflowAuditPath,
    listRunHistory,
    queueState,
    defaultQueueControl,
    saveQueueControl,
    enqueueReviews,
  } = deps;

  function sandboxAlertCode(v) {
    const text = String(v?.error || "");
    const match = text.match(/(sandbox_(?:limit_exceeded|egress_blocked)(?::[a-z_]+)?)/i);
    return String((match && match[1]) || "sandbox_violation").toLowerCase();
  }

  function normalizeSandboxAlertRules(rules) {
    const src = rules && typeof rules === "object" ? rules : {};
    const toList = (arr) => (Array.isArray(arr) ? arr : [])
      .map((item) => String(item || "").trim().toLowerCase())
      .filter(Boolean);
    const muteSrc = src.mute_until_by_key && typeof src.mute_until_by_key === "object" ? src.mute_until_by_key : {};
    const mute = {};
    Object.keys(muteSrc).forEach((key) => {
      const normalizedKey = String(key || "").trim().toLowerCase();
      const ts = Date.parse(String(muteSrc[key] || ""));
      if (!normalizedKey || !Number.isFinite(ts)) return;
      mute[normalizedKey] = new Date(ts).toISOString();
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
    const filePath = sandboxAlertRuleVersionsPath();
    if (!fs.existsSync(filePath)) return [];
    const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/).filter((item) => item.trim());
    return lines
      .map((item) => {
        try { return JSON.parse(item); } catch { return null; }
      })
      .filter(Boolean)
      .reverse()
      .slice(0, Math.max(1, Math.min(2000, Number(limit || 200))));
  }

  function compareSandboxRuleVersions(versionA, versionB) {
    const items = listSandboxRuleVersions(5000);
    const a = items.find((item) => String(item.version_id || "") === String(versionA || ""));
    const b = items.find((item) => String(item.version_id || "") === String(versionB || ""));
    if (!a || !b) return { ok: false, error: "rule version not found" };
    const rulesA = normalizeSandboxAlertRules(a.rules || {});
    const rulesB = normalizeSandboxAlertRules(b.rules || {});
    const diffList = (key) => {
      const setA = new Set(Array.isArray(rulesA[key]) ? rulesA[key] : []);
      const setB = new Set(Array.isArray(rulesB[key]) ? rulesB[key] : []);
      return {
        added: Array.from(setB).filter((item) => !setA.has(item)),
        removed: Array.from(setA).filter((item) => !setB.has(item)),
      };
    };
    const muteA = rulesA.mute_until_by_key && typeof rulesA.mute_until_by_key === "object" ? rulesA.mute_until_by_key : {};
    const muteB = rulesB.mute_until_by_key && typeof rulesB.mute_until_by_key === "object" ? rulesB.mute_until_by_key : {};
    const muteKeys = Array.from(new Set([...Object.keys(muteA), ...Object.keys(muteB)])).sort();
    const muteChanged = muteKeys
      .filter((key) => String(muteA[key] || "") !== String(muteB[key] || ""))
      .map((key) => ({ key, from: String(muteA[key] || ""), to: String(muteB[key] || "") }));
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
    const state = loadSandboxAutoFixState();
    const until = Date.parse(String(state.forced_until || ""));
    if (state.forced_isolation_mode && Number.isFinite(until) && Date.now() < until) {
      base.chiplet_isolation_enabled = true;
      base.chiplet_isolation_mode = state.forced_isolation_mode;
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
      const response = await fetch(webhook, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!response.ok) {
        appendAudit("sandbox_notify_failed", { status: response.status, webhook });
        return { ok: false, status: response.status };
      }
      appendAudit("sandbox_notify_sent", { webhook, event });
      return { ok: true };
    } catch (error) {
      appendAudit("sandbox_notify_failed", { error: String(error) });
      return { ok: false, error: String(error) };
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
    const state = loadSandboxAutoFixState();
    state.green_streak = Number.isFinite(Number(state.green_streak)) ? Math.max(0, Math.floor(Number(state.green_streak))) : 0;
    const fresh = Array.isArray(state.violation_events)
      ? state.violation_events.filter((item) => Number.isFinite(Number(item?.ts_ms || 0)) && now - Number(item.ts_ms || 0) <= windowSec * 1000)
      : [];
    if (violations.length) {
      state.green_streak = 0;
      violations.forEach((violation) => fresh.push({
        ts_ms: now,
        run_id: String(run?.run_id || ""),
        node_type: String(violation?.node_type || ""),
        node_id: String(violation?.node_id || ""),
        code: sandboxAlertCode(violation),
      }));
    } else {
      state.green_streak += 1;
    }
    state.violation_events = fresh;
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
        state.forced_isolation_mode = forceMode || "process";
        state.forced_until = new Date(now + forceMins * 60000).toISOString();
        actions.push(`force_isolation:${state.forced_isolation_mode}`);
      }
      state.last_actions.push({
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
    } else if (!violations.length && state.green_streak >= recoverGreenRuns) {
      if (doRecoverQueue && queueState.control?.paused) {
        queueState.control = { ...(queueState.control || defaultQueueControl()), paused: false };
        saveQueueControl(queueState.control);
        actions.push("resume_queue");
      }
      if (doRecoverIsolation && state.forced_isolation_mode) {
        state.forced_isolation_mode = "";
        state.forced_until = "";
        actions.push("clear_forced_isolation");
      }
      if (actions.length) {
        state.last_actions.push({
          ts: nowIso(),
          run_id: String(run?.run_id || ""),
          count,
          actions,
          recovered: true,
        });
        appendAudit("sandbox_autofix_recovered", {
          run_id: String(run?.run_id || ""),
          green_streak: state.green_streak,
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
    saveSandboxAutoFixState(state);
    return {
      triggered: actions.length > 0,
      actions,
      count,
      window_sec: windowSec,
      threshold_red: red,
      green_streak: state.green_streak,
      recover_green_runs: recoverGreenRuns,
    };
  }

  function sandboxRuleKeyVariants(v) {
    const nodeType = String(v?.node_type || "").trim().toLowerCase() || "*";
    const nodeId = String(v?.node_id || "").trim().toLowerCase() || "*";
    const code = sandboxAlertCode(v);
    return [
      `${nodeType}::${nodeId}::${code}`,
      `${nodeType}::${nodeId}::*`,
      `${nodeType}::*::${code}`,
      `${nodeType}::*::*`,
      `*::${nodeId}::${code}`,
      `*::${nodeId}::*`,
      `*::*::${code}`,
      `*::*::*`,
    ];
  }

  function sandboxSuppressionReason(v, rules, nowTs = Date.now()) {
    const normalizedRules = normalizeSandboxAlertRules(rules);
    const code = sandboxAlertCode(v);
    const nodeType = String(v?.node_type || "").trim().toLowerCase();
    if (normalizedRules.whitelist_codes.includes(code)) return "whitelist";
    if (nodeType && normalizedRules.whitelist_node_types.includes(nodeType)) return "whitelist";
    const variants = sandboxRuleKeyVariants(v);
    if (variants.some((key) => normalizedRules.whitelist_keys.includes(key))) return "whitelist";
    for (const key of variants) {
      const until = Date.parse(String(normalizedRules.mute_until_by_key?.[key] || ""));
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
    const lastByKey = obj?.last_by_key && typeof obj.last_by_key === "object" ? obj.last_by_key : {};
    return { last_by_key: lastByKey };
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
    const windowSec = Number.isFinite(Number(dedupWindowSec)) ? Math.max(0, Math.floor(Number(dedupWindowSec))) : 0;
    if (windowSec <= 0) return { items, suppressed: 0 };
    const sorted = (Array.isArray(items) ? items : [])
      .slice()
      .sort((a, b) => String(a.ts || "").localeCompare(String(b.ts || "")));
    const lastTsByKey = new Map();
    const kept = [];
    let suppressed = 0;
    sorted.forEach((item) => {
      const code = sandboxAlertCode(item);
      const key = `${String(item.node_type || "")}::${String(item.node_id || "")}::${code}`;
      const current = Date.parse(String(item.ts || ""));
      const previous = lastTsByKey.get(key);
      if (Number.isFinite(current) && Number.isFinite(previous) && current - previous < windowSec * 1000) {
        suppressed += 1;
        return;
      }
      if (Number.isFinite(current)) lastTsByKey.set(key, current);
      kept.push(item);
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
    const officeArtifacts = artifacts.filter((artifact) => /\.(docx|pptx|xlsx)$/i.test(String(artifact?.path || "")));
    officeArtifacts.forEach((artifact) => {
      try {
        const filePath = String(artifact?.path || "");
        if (!filePath || !fs.existsSync(filePath)) {
          issues.push(`office_missing:${filePath}`);
          return;
        }
        const stat = fs.statSync(filePath);
        if (Number(stat.size || 0) < minOfficeBytes) issues.push(`office_too_small:${filePath}`);
      } catch (error) {
        issues.push(`office_stat_failed:${String(error)}`);
      }
    });
    const textArtifacts = artifacts.filter((artifact) => /\.(md|txt|json)$/i.test(String(artifact?.path || "")));
    textArtifacts.slice(0, 20).forEach((artifact) => {
      try {
        const filePath = String(artifact?.path || "");
        if (!filePath || !fs.existsSync(filePath)) return;
        const text = fs.readFileSync(filePath, "utf8");
        const bad = (text.match(/\uFFFD/g) || []).length;
        const rate = text.length > 0 ? bad / text.length : 0;
        if (rate > maxMojibakeRate) issues.push(`mojibake_high:${filePath}:${rate.toFixed(4)}`);
      } catch {}
    });
    if (requireSlot) {
      const nodeOutputs = run?.node_outputs && typeof run.node_outputs === "object" ? run.node_outputs : {};
      const hasBinding = Object.values(nodeOutputs).some((item) => String(item?.binding_path || "").trim().toLowerCase().endsWith(".json"));
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
      .map((run) => ({
        ts: String(run?.ts || ""),
        run_id: String(run?.run_id || ""),
        workflow_id: String(run?.workflow_id || ""),
        quality_gate: run?.result?.quality_gate || null,
        status: String(run?.result?.status || ""),
        ok: !!run?.result?.ok,
      }))
      .filter((item) => item.quality_gate && typeof item.quality_gate === "object")
      .filter((item) => {
        if (runIdLike && !String(item.run_id || "").toLowerCase().includes(runIdLike)) return false;
        if (!statusFilter || statusFilter === "all") return true;
        const blocked = !!item?.quality_gate?.blocked;
        const passed = !!item?.quality_gate?.passed;
        if (statusFilter === "blocked") return blocked;
        if (statusFilter === "pass" || statusFilter === "passed") return passed;
        return true;
      });
  }

  function attachQualityGate(run, payload = {}) {
    const output = run && typeof run === "object" ? run : {};
    const gate = officeQualityGate(output, payload || {});
    output.quality_gate = gate;
    if (gate.blocked) {
      output.ok = false;
      output.status = "quality_blocked";
      const msg = `quality_gate_blocked:${gate.issues.join("|")}`;
      output.error = output.error ? `${String(output.error)}; ${msg}` : msg;
    }
    return output;
  }

  function listAudit(limit = 200, action = "") {
    const filePath = workflowAuditPath();
    if (!fs.existsSync(filePath)) return [];
    const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/).filter((item) => item.trim());
    const actionKey = String(action || "").trim();
    const all = lines
      .slice(Math.max(0, lines.length - Math.max(1, Math.min(5000, Number(limit || 200)))))
      .map((item) => {
        try { return JSON.parse(item); } catch { return null; }
      })
      .filter(Boolean)
      .reverse();
    return actionKey ? all.filter((item) => String(item.action || "") === actionKey) : all;
  }

  function sandboxAlerts(limit = 400, thresholds = null, dedupWindowSec = 0) {
    const runs = listRunHistory(limit);
    const items = [];
    runs.forEach((run) => {
      const result = run?.result || {};
      extractSandboxViolations(result).forEach((violation) => {
        items.push({
          ts: String(run?.ts || ""),
          ...violation,
        });
      });
    });
    const rules = loadSandboxAlertRules();
    const filtered = [];
    let suppressedWhitelist = 0;
    let suppressedMuted = 0;
    items.forEach((item) => {
      const reason = sandboxSuppressionReason(item, rules, Date.now());
      if (!reason) {
        filtered.push(item);
        return;
      }
      if (reason === "whitelist") suppressedWhitelist += 1;
      else if (reason === "muted") suppressedMuted += 1;
    });
    const dedup = dedupSandboxItems(filtered, dedupWindowSec);
    const effectiveItems = Array.isArray(dedup.items) ? dedup.items : [];
    const byNode = {};
    effectiveItems.forEach((item) => {
      const key = `${item.node_type}::${item.node_id}`;
      if (!byNode[key]) byNode[key] = { count: 0, node_type: item.node_type, node_id: item.node_id, last_run_id: "", last_ts: "" };
      byNode[key].count += 1;
      byNode[key].last_run_id = item.run_id;
      byNode[key].last_ts = item.ts;
    });
    const thresholdConfig = thresholds && typeof thresholds === "object" ? thresholds : {};
    const yellow = Number.isFinite(Number(thresholdConfig.yellow)) ? Math.max(1, Math.floor(Number(thresholdConfig.yellow))) : 1;
    const red = Number.isFinite(Number(thresholdConfig.red)) ? Math.max(yellow + 1, Math.floor(Number(thresholdConfig.red))) : Math.max(3, yellow + 1);
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

  return {
    applySandboxAutoFixPayload,
    appendSandboxRuleVersion,
    appendSandboxViolationAudit,
    attachQualityGate,
    compareSandboxRuleVersions,
    listAudit,
    listQualityGateReports,
    listSandboxRuleVersions,
    loadSandboxAlertRules,
    loadSandboxAutoFixState,
    maybeApplySandboxAutoFix,
    normalizeSandboxAlertRules,
    sandboxAlertDedupWindowSec,
    sandboxAlerts,
    saveSandboxAlertRules,
  };
}

module.exports = {
  createWorkflowSandboxSupport,
};
