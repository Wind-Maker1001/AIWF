const GLUE_PROVIDER = "glue_http";
const WORKFLOW_APP_SCHEMA_VERSION = "workflow_app_registry_entry.v1";
const GLUE_DEFAULT_URL = "http://127.0.0.1:18081";
const { createGovernanceControlPlaneSupport, GOVERNANCE_CAPABILITIES } = require("./workflow_governance");

function createWorkflowAppRegistryStore(deps = {}) {
  const {
    loadConfig = () => ({}),
    nowIso = () => new Date().toISOString(),
    fetchImpl = typeof fetch === "function" ? fetch : null,
    validateWorkflowGraph = () => {},
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
    if (raw === "local_legacy") throw new Error("workflow app registry local_legacy provider has been retired; use glue_http");
    return "";
  }

  function resolveProvider(cfg = null) {
    const merged = mergedConfig(cfg);
    const explicit = normalizeProvider(merged.workflowAppRegistryProvider || env.AIWF_WORKFLOW_APP_REGISTRY_PROVIDER);
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

  function normalizeWorkflowApp(item, provider, existing = null) {
    const source = item && typeof item === "object" ? item : {};
    const current = existing && typeof existing === "object" ? existing : {};
    const appId = String(source.app_id || current.app_id || "").trim();
    if (!appId) throw new Error("workflow app id is required");
    const graph = source.graph !== undefined ? source.graph : current.graph;
    validateWorkflowGraph(graph);
    const safeGraph = clone(graph);
    return {
      schema_version: String(source.schema_version || current.schema_version || WORKFLOW_APP_SCHEMA_VERSION),
      provider,
      owner: String(source.owner || current.owner || (provider === GLUE_PROVIDER ? "glue-python" : "desktop.local_legacy")),
      source_of_truth: String(
        source.source_of_truth
          || current.source_of_truth
          || (provider === GLUE_PROVIDER ? "glue-python.governance.workflow_apps" : "desktop.workflow_store.workflow_apps")
      ),
      app_id: appId,
      name: String(source.name || current.name || safeGraph?.name || appId).trim() || appId,
      workflow_id: String(source.workflow_id || current.workflow_id || safeGraph?.workflow_id || "").trim() || "custom",
      params_schema: clone(source.params_schema && typeof source.params_schema === "object" ? source.params_schema : (current.params_schema || {})),
      template_policy: clone(source.template_policy && typeof source.template_policy === "object" ? source.template_policy : (current.template_policy || {})),
      graph: safeGraph,
      created_at: String(source.created_at || current.created_at || nowIso()),
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
      throw new Error(String(payload?.error || `workflow app ${method} failed: http ${resp.status}`));
    }
    return payload;
  }

  async function remoteListApps(limit = 200, cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_APPS.capability, { cfg });
    const payload = await remoteRequest(
      "GET",
      `${routePrefix}?limit=${Math.max(1, Math.min(5000, Number(limit || 200)))}`,
      null,
      cfg
    );
    const items = [];
    for (const item of (Array.isArray(payload?.items) ? payload.items : [])) {
      try {
        items.push(normalizeWorkflowApp(item, GLUE_PROVIDER, item));
      } catch {}
    }
    return { ok: true, provider: GLUE_PROVIDER, items };
  }

  async function remoteGetApp(appId, cfg = null) {
    const id = String(appId || "").trim();
    if (!id) return null;
    try {
      const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_APPS.capability, { cfg });
      const payload = await remoteRequest("GET", `${routePrefix}/${encodeURIComponent(id)}`, null, cfg);
      return payload?.item ? normalizeWorkflowApp(payload.item, GLUE_PROVIDER, payload.item) : null;
    } catch (error) {
      if (/workflow app not found/i.test(String(error))) return null;
      throw error;
    }
  }

  async function remotePublishApp(req, cfg = null) {
    const source = req && typeof req === "object" ? req : {};
    const appId = String(source.app_id || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`);
    const normalized = normalizeWorkflowApp({ ...source, app_id: appId }, GLUE_PROVIDER, source);
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_APPS.capability, { cfg });
    const payload = await remoteRequest(
      "PUT",
      `${routePrefix}/${encodeURIComponent(appId)}`,
      {
        app: {
          app_id: normalized.app_id,
          name: normalized.name,
          workflow_id: normalized.workflow_id,
          graph: clone(normalized.graph),
          params_schema: clone(normalized.params_schema),
          template_policy: clone(normalized.template_policy),
        },
      },
      cfg
    );
    return {
      ok: true,
      provider: GLUE_PROVIDER,
      item: normalizeWorkflowApp(payload?.item || normalized, GLUE_PROVIDER, payload?.item || normalized),
    };
  }

  async function listApps(limit = 200, cfg = null) {
    _ = resolveProvider(cfg);
    return remoteListApps(limit, cfg);
  }

  async function getApp(appId, cfg = null) {
    _ = resolveProvider(cfg);
    return remoteGetApp(appId, cfg);
  }

  async function publishApp(req, cfg = null) {
    try {
      _ = resolveProvider(cfg);
      return await remotePublishApp(req, cfg);
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  }

  return {
    getApp,
    listApps,
    publishApp,
    resolveGlueUrl,
    resolveProvider,
  };
}

module.exports = {
  GLUE_PROVIDER,
  WORKFLOW_APP_SCHEMA_VERSION,
  createWorkflowAppRegistryStore,
};
