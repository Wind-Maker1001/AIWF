const GLUE_PROVIDER = "glue_http";
const {
  createGovernanceGlueStoreSupport,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS,
  GOVERNANCE_DEFAULT_GLUE_URL,
} = require("./workflow_governance");
const {
  workflowStoreRemoteErrorResult,
} = require("./workflow_store_remote_error");
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
  const {
    governance,
    resolveGlueUrl,
    resolveProvider,
    remoteRequest,
  } = createGovernanceGlueStoreSupport({
    loadConfig,
    fetchImpl,
    env,
    defaultGlueUrl: GOVERNANCE_DEFAULT_GLUE_URL,
    providerConfigKey: "workflowSandboxRuleProvider",
    providerEnvKey: "AIWF_WORKFLOW_SANDBOX_RULE_PROVIDER",
    providerLabel: "workflow sandbox rule",
  });

  if (!sandboxSupport) throw new Error("sandboxSupport is required");

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
    try {
      resolveProvider(cfg);
      return await remoteGetRules(cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
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
    try {
      resolveProvider(cfg);
      return await remoteGetRules(cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function saveRules(req, cfg = null) {
    try {
      resolveProvider(cfg);
      return await remoteSaveRules(req, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function muteAlert(req, cfg = null) {
    try {
      resolveProvider(cfg);
      return await remoteMute(req, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function listVersions(limit = 100, cfg = null) {
    try {
      resolveProvider(cfg);
      return await remoteListVersions(limit, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function compareVersions(versionA, versionB, cfg = null) {
    try {
      resolveProvider(cfg);
      return await remoteCompareVersions(versionA, versionB, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function rollbackVersion(versionId, cfg = null) {
    try {
      resolveProvider(cfg);
      return await remoteRollback(versionId, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
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
