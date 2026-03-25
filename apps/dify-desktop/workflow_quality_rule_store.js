const GLUE_PROVIDER = "glue_http";
const QUALITY_RULE_SET_SCHEMA_VERSION = "quality_rule_set.v1";
const GLUE_DEFAULT_URL = "http://127.0.0.1:18081";
const { createGovernanceControlPlaneSupport, GOVERNANCE_CAPABILITIES } = require("./workflow_governance");

function createWorkflowQualityRuleSetSupport(deps = {}) {
  const {
    loadConfig = () => ({}),
    nowIso = () => new Date().toISOString(),
    fetchImpl = typeof fetch === "function" ? fetch : null,
    env = process.env,
  } = deps;
  const governance = createGovernanceControlPlaneSupport({ loadConfig, fetchImpl, env, defaultGlueUrl: GLUE_DEFAULT_URL });

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function mergedConfig(cfg = null) {
    return { ...loadConfig(), ...(cfg && typeof cfg === "object" ? cfg : {}) };
  }

  function normalizeProvider(value) {
    const raw = String(value || "").trim().toLowerCase();
    if (raw === GLUE_PROVIDER) return GLUE_PROVIDER;
    if (raw === "local_legacy") throw new Error("workflow quality rule set local_legacy provider has been retired; use glue_http");
    return "";
  }

  function resolveProvider(cfg = null) {
    const merged = mergedConfig(cfg);
    const explicit = normalizeProvider(merged.qualityRuleSetProvider || env.AIWF_QUALITY_RULE_SET_PROVIDER);
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
      owner: String(source.owner || current.owner || (provider === GLUE_PROVIDER ? "glue-python" : "desktop.local_legacy")),
      source_of_truth: String(
        source.source_of_truth
          || current.source_of_truth
          || (provider === GLUE_PROVIDER ? "glue-python.governance.quality_rule_sets" : "desktop.workflow_store.quality_rule_center")
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
      throw new Error(String(payload?.error || `quality rule set ${method} failed: http ${resp.status}`));
    }
    return payload;
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
    const provider = resolveProvider(cfg);
    const sets = await remoteListQualityRuleSets(cfg);
    return { ok: true, provider, sets };
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
      return { ok: false, error: String(error) };
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
      return { ok: false, error: String(error) };
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
