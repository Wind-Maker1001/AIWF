const GLUE_PROVIDER = "glue_http";
const GLUE_DEFAULT_URL = "http://127.0.0.1:18081";
const {
  createGovernanceControlPlaneSupport,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS,
} = require("./workflow_governance");
const WORKFLOW_SANDBOX_RULES_SCHEMA_VERSION = "workflow_sandbox_rules.v1";
const WORKFLOW_SANDBOX_RULE_VERSION_SCHEMA_VERSION = "workflow_sandbox_rule_version.v1";
const WORKFLOW_SANDBOX_RULE_COMPARE_SCHEMA_VERSION = "workflow_sandbox_rule_compare.v1";

function createWorkflowSandboxRuleStore(deps = {}) {
  const {
    loadConfig = () => ({}),
    sandboxSupport,
    fetchImpl = typeof fetch === "function" ? fetch : null,
    env = process.env,
  } = deps;
  const governance = createGovernanceControlPlaneSupport({ loadConfig, fetchImpl, env, defaultGlueUrl: GLUE_DEFAULT_URL });

  if (!sandboxSupport) throw new Error("sandboxSupport is required");

  function mergedConfig(cfg = null) {
    return { ...loadConfig(), ...(cfg && typeof cfg === "object" ? cfg : {}) };
  }

  function normalizeProvider(value) {
    const raw = String(value || "").trim().toLowerCase();
    if (raw === GLUE_PROVIDER) return GLUE_PROVIDER;
    if (raw === "local_legacy") throw new Error("workflow sandbox rule local_legacy provider has been retired; use glue_http");
    return "";
  }

  function resolveProvider(cfg = null) {
    const merged = mergedConfig(cfg);
    const explicit = normalizeProvider(merged.workflowSandboxRuleProvider || env.AIWF_WORKFLOW_SANDBOX_RULE_PROVIDER);
    if (explicit) return explicit;
    return GLUE_PROVIDER;
  }

  function resolveGlueUrl(cfg = null) {
    return governance.resolveGlueUrl(cfg);
  }

  function headers(apiKey) {
    const out = { "Content-Type": "application/json" };
    const key = String(apiKey || "").trim();
    if (key) out["X-API-Key"] = key;
    return out;
  }

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function normalizeRules(rules) {
    return sandboxSupport.normalizeSandboxAlertRules(rules || {});
  }

  function normalizeRulesEnvelope(rules, existing = null) {
    const source = existing && typeof existing === "object" ? existing : {};
    return {
      schema_version: String(source.schema_version || WORKFLOW_SANDBOX_RULES_SCHEMA_VERSION),
      owner: String(source.owner || "glue-python"),
      source_of_truth: String(source.source_of_truth || "glue-python.governance.workflow_sandbox.rules"),
      provider: GLUE_PROVIDER,
      rules: normalizeRules(rules),
    };
  }

  function normalizeRuleVersionItem(item) {
    const source = item && typeof item === "object" ? item : {};
    const versionId = String(source.version_id || "").trim();
    if (!versionId) throw new Error("rule version_id is required");
    return {
      schema_version: String(source.schema_version || WORKFLOW_SANDBOX_RULE_VERSION_SCHEMA_VERSION),
      provider: GLUE_PROVIDER,
      owner: String(source.owner || "glue-python"),
      source_of_truth: String(source.source_of_truth || "glue-python.governance.workflow_sandbox.rule_versions"),
      version_id: versionId,
      ts: String(source.ts || ""),
      rules: normalizeRules(source.rules || {}),
      meta: clone(source.meta && typeof source.meta === "object" ? source.meta : {}),
    };
  }

  function compareRulesObject(rulesA, rulesB, versionA = "", versionB = "") {
    const a = normalizeRules(rulesA);
    const b = normalizeRules(rulesB);
    const diffList = (key) => {
      const setA = new Set(Array.isArray(a[key]) ? a[key] : []);
      const setB = new Set(Array.isArray(b[key]) ? b[key] : []);
      return {
        added: Array.from(setB).filter((item) => !setA.has(item)),
        removed: Array.from(setA).filter((item) => !setB.has(item)),
      };
    };
    const muteA = a.mute_until_by_key && typeof a.mute_until_by_key === "object" ? a.mute_until_by_key : {};
    const muteB = b.mute_until_by_key && typeof b.mute_until_by_key === "object" ? b.mute_until_by_key : {};
    const muteKeys = Array.from(new Set([...Object.keys(muteA), ...Object.keys(muteB)])).sort();
    const muteChanged = muteKeys
      .filter((key) => String(muteA[key] || "") !== String(muteB[key] || ""))
      .map((key) => ({ key, from: String(muteA[key] || ""), to: String(muteB[key] || "") }));
    return {
      ok: true,
      schema_version: WORKFLOW_SANDBOX_RULE_COMPARE_SCHEMA_VERSION,
      provider: GLUE_PROVIDER,
      owner: "glue-python",
      source_of_truth: "glue-python.governance.workflow_sandbox.rule_versions",
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

  async function parseResponse(resp) {
    const text = await resp.text();
    if (!text) return {};
    try {
      return JSON.parse(text);
    } catch {
      return { ok: false, error: text };
    }
  }

  async function remoteRequest(method, route, body = null, cfg = null) {
    if (typeof fetchImpl !== "function") throw new Error("fetch is not available for glue provider");
    const merged = mergedConfig(cfg);
    const url = `${resolveGlueUrl(merged)}${route}`;
    const resp = await fetchImpl(url, {
      method,
      headers: headers(merged.apiKey),
      body: body ? JSON.stringify(body) : undefined,
    });
    const payload = await parseResponse(resp);
    if (!resp.ok || payload?.ok === false) {
      throw new Error(String(payload?.error || `workflow sandbox rule ${method} failed: http ${resp.status}`));
    }
    return payload;
  }

  async function remoteGetRules(cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_SANDBOX_RULES.capability, { cfg });
    const payload = await remoteRequest("GET", routePrefix, null, cfg);
    return { ok: true, ...normalizeRulesEnvelope(payload?.rules || {}, payload) };
  }

  async function remoteSaveRules(req, cfg = null) {
    const incoming = req?.rules && typeof req.rules === "object" ? req.rules : {};
    const meta = req?.meta && typeof req.meta === "object" ? req.meta : { reason: "set_rules" };
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_SANDBOX_RULES.capability, { cfg });
    const payload = await remoteRequest("PUT", routePrefix, {
      rules: normalizeRules(incoming),
      meta,
    }, cfg);
    return {
      ok: true,
      ...normalizeRulesEnvelope(payload?.rules || incoming, payload),
      version_id: String(payload?.version_id || ""),
    };
  }

  async function remoteListVersions(limit = 100, cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_SANDBOX_RULES.capability, {
      cfg,
      preferredOwnedPrefix: GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS.WORKFLOW_SANDBOX_RULES.RULE_VERSIONS,
    });
    const payload = await remoteRequest(
      "GET",
      `${routePrefix}?limit=${Math.max(1, Math.min(5000, Number(limit || 100)))}`,
      null,
      cfg
    );
    return {
      ok: true,
      provider: GLUE_PROVIDER,
      items: (Array.isArray(payload?.items) ? payload.items : []).map((item) => normalizeRuleVersionItem(item)),
    };
  }

  async function remoteCompareVersions(versionA, versionB, cfg = null) {
    const listed = await remoteListVersions(5000, cfg);
    const items = Array.isArray(listed.items) ? listed.items : [];
    const a = items.find((item) => String(item.version_id || "") === String(versionA || ""));
    const b = items.find((item) => String(item.version_id || "") === String(versionB || ""));
    if (!a || !b) return { ok: false, error: "rule version not found" };
    const out = compareRulesObject(a.rules || {}, b.rules || {}, versionA, versionB);
    return out;
  }

  async function remoteRollback(versionId, cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_SANDBOX_RULES.capability, {
      cfg,
      preferredOwnedPrefix: GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS.WORKFLOW_SANDBOX_RULES.RULE_VERSIONS,
    });
    const payload = await remoteRequest(
      "POST",
      `${routePrefix}/${encodeURIComponent(String(versionId || ""))}/rollback`,
      {},
      cfg
    );
    return {
      ok: true,
      ...normalizeRulesEnvelope(payload?.rules || {}, payload),
      version_id: String(payload?.version_id || ""),
    };
  }

  async function getRuntimeRules(cfg = null) {
    _ = resolveProvider(cfg);
    return remoteGetRules(cfg);
  }

  async function remoteMute(req, cfg = null) {
    const current = await remoteGetRules(cfg);
    if (!current?.ok) return current;
    const nodeType = String(req?.node_type || "*").trim().toLowerCase() || "*";
    const nodeId = String(req?.node_id || "*").trim().toLowerCase() || "*";
    const code = String(req?.code || "*").trim().toLowerCase() || "*";
    const minsRaw = Number(req?.minutes || 60);
    const minutes = Number.isFinite(minsRaw) ? Math.max(1, Math.floor(minsRaw)) : 60;
    const key = `${nodeType}::${nodeId}::${code}`;
    const rules = normalizeRules(current.rules || {});
    rules.mute_until_by_key[key] = new Date(Date.now() + minutes * 60000).toISOString();
    const out = await remoteSaveRules({ rules, meta: { reason: "mute", key, minutes } }, cfg);
    if (!out?.ok) return out;
    return {
      ...out,
      key,
      mute_until: String(rules.mute_until_by_key[key] || ""),
    };
  }

  async function getRules(cfg = null) {
    _ = resolveProvider(cfg);
    return remoteGetRules(cfg);
  }

  async function saveRules(req, cfg = null) {
    _ = resolveProvider(cfg);
    return remoteSaveRules(req, cfg);
  }

  async function muteAlert(req, cfg = null) {
    _ = resolveProvider(cfg);
    return remoteMute(req, cfg);
  }

  async function listVersions(limit = 100, cfg = null) {
    _ = resolveProvider(cfg);
    return remoteListVersions(limit, cfg);
  }

  async function compareVersions(versionA, versionB, cfg = null) {
    _ = resolveProvider(cfg);
    return remoteCompareVersions(versionA, versionB, cfg);
  }

  async function rollbackVersion(versionId, cfg = null) {
    _ = resolveProvider(cfg);
    return remoteRollback(versionId, cfg);
  }

  return {
    compareVersions,
    getRules,
    getRuntimeRules,
    listVersions,
    muteAlert,
    resolveGlueUrl,
    resolveProvider,
    rollbackVersion,
    saveRules,
  };
}

module.exports = {
  GLUE_PROVIDER,
  WORKFLOW_SANDBOX_RULE_COMPARE_SCHEMA_VERSION,
  WORKFLOW_SANDBOX_RULE_VERSION_SCHEMA_VERSION,
  WORKFLOW_SANDBOX_RULES_SCHEMA_VERSION,
  createWorkflowSandboxRuleStore,
};
