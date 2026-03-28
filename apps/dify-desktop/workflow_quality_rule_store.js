const GLUE_PROVIDER = "glue_http";
const QUALITY_RULE_SET_SCHEMA_VERSION = "quality_rule_set.v1";
const {
  createGovernanceGlueStoreSupport,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_DEFAULT_GLUE_URL,
} = require("./workflow_governance");
const {
  workflowStoreRemoteErrorResult,
} = require("./workflow_store_remote_error");

function createWorkflowQualityRuleSetSupport(deps = {}) {
  const {
    loadConfig = () => ({}),
    nowIso = () => new Date().toISOString(),
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
    providerConfigKey: "qualityRuleSetProvider",
    providerEnvKey: "AIWF_QUALITY_RULE_SET_PROVIDER",
    providerLabel: "workflow quality rule set",
  });

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function normalizeQualityRuleSet(item, provider, existing = null) {
    const source = item && typeof item === "object" ? item : {};
    const current = existing && typeof existing === "object" ? existing : {};
    const id = String(source.id || current.id || "").trim();
    if (!id) throw new Error("quality rule set id is required");
    const rules = source.rules !== undefined ? source.rules : current.rules;
    if (!rules || typeof rules !== "object" || Array.isArray(rules)) {
      throw new Error("quality rule set rules must be an object");
    }
    const createdAt = String(source.created_at || current.created_at || nowIso());
    return {
      schema_version: String(source.schema_version || current.schema_version || QUALITY_RULE_SET_SCHEMA_VERSION),
      provider,
      owner: String(source.owner || current.owner || "glue-python"),
      source_of_truth: String(
        source.source_of_truth
          || current.source_of_truth
          || "glue-python.governance.quality_rule_sets"
      ),
      id,
      name: String(source.name || current.name || id).trim() || id,
      version: String(source.version || current.version || "v1").trim() || "v1",
      scope: String(source.scope || current.scope || "workflow").trim() || "workflow",
      rules: clone(rules),
      created_at: createdAt,
      updated_at: String(source.updated_at || nowIso()),
    };
  }

  async function remoteListQualityRuleSets(cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.QUALITY_RULE_SETS.capability, { cfg });
    const payload = await remoteRequest("GET", `${routePrefix}?limit=5000`, null, cfg);
    const sets = Array.isArray(payload?.sets) ? payload.sets : [];
    const items = [];
    for (const item of sets) {
      try {
        items.push(normalizeQualityRuleSet(item, GLUE_PROVIDER, item));
      } catch {}
    }
    return items;
  }

  async function remoteGetQualityRuleSet(setId, cfg = null) {
    const id = String(setId || "").trim();
    if (!id) return null;
    try {
      const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.QUALITY_RULE_SETS.capability, { cfg });
      const payload = await remoteRequest("GET", `${routePrefix}/${encodeURIComponent(id)}`, null, cfg);
      return payload?.set ? normalizeQualityRuleSet(payload.set, GLUE_PROVIDER, payload.set) : null;
    } catch (error) {
      if (/not found/i.test(String(error))) return null;
      throw error;
    }
  }

  async function remoteSaveQualityRuleSet(set, cfg = null) {
    const normalized = normalizeQualityRuleSet(set, GLUE_PROVIDER, set);
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.QUALITY_RULE_SETS.capability, { cfg });
    const payload = await remoteRequest(
      "PUT",
      `${routePrefix}/${encodeURIComponent(normalized.id)}`,
      {
        set: {
          id: normalized.id,
          name: normalized.name,
          version: normalized.version,
          scope: normalized.scope,
          rules: clone(normalized.rules),
        },
      },
      cfg
    );
    return normalizeQualityRuleSet(payload?.set || normalized, GLUE_PROVIDER, payload?.set || normalized);
  }

  async function remoteRemoveQualityRuleSet(setId, cfg = null) {
    const id = String(setId || "").trim();
    if (!id) throw new Error("quality rule set id is required");
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.QUALITY_RULE_SETS.capability, { cfg });
    await remoteRequest("DELETE", `${routePrefix}/${encodeURIComponent(id)}`, null, cfg);
    return true;
  }

  async function listQualityRuleSets(cfg = null) {
    try {
      const provider = resolveProvider(cfg);
      const sets = await remoteListQualityRuleSets(cfg);
      return { ok: true, provider, sets };
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function getQualityRuleSet(setId, cfg = null) {
    _ = resolveProvider(cfg);
    return await remoteGetQualityRuleSet(setId, cfg);
  }

  async function saveQualityRuleSet(req, cfg = null) {
    const set = req?.set && typeof req.set === "object" ? req.set : req;
    if (!set || typeof set !== "object") return { ok: false, error: "set required" };
    try {
      const provider = resolveProvider(cfg);
      const item = await remoteSaveQualityRuleSet(set, cfg);
      return { ok: true, provider, set: item };
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function removeQualityRuleSet(req, cfg = null) {
    const id = String(req?.id || req || "").trim();
    if (!id) return { ok: false, error: "id required" };
    try {
      const provider = resolveProvider(cfg);
      const removed = await remoteRemoveQualityRuleSet(id, cfg);
      if (!removed) return { ok: false, error: `quality rule set not found: ${id}` };
      return { ok: true, provider, id };
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  return {
    getQualityRuleSet,
    listQualityRuleSets,
    removeQualityRuleSet,
    resolveGlueUrl,
    resolveProvider,
    saveQualityRuleSet,
  };
}

module.exports = {
  GLUE_PROVIDER,
  QUALITY_RULE_SET_SCHEMA_VERSION,
  createWorkflowQualityRuleSetSupport,
};
