const GLUE_PROVIDER = "glue_http";
const WORKFLOW_APP_SCHEMA_VERSION = "workflow_app_registry_entry.v1";
const {
  createGovernanceGlueStoreSupport,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_DEFAULT_GLUE_URL,
} = require("./workflow_governance");
const {
  workflowStoreRemoteErrorResult,
} = require("./workflow_store_remote_error");

function createWorkflowAppRegistryStore(deps = {}) {
  const {
    loadConfig = () => ({}),
    nowIso = () => new Date().toISOString(),
    fetchImpl = typeof fetch === "function" ? fetch : null,
    validateWorkflowGraph = () => {},
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
    providerConfigKey: "workflowAppRegistryProvider",
    providerEnvKey: "AIWF_WORKFLOW_APP_REGISTRY_PROVIDER",
    providerLabel: "workflow app registry",
  });

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function normalizeWorkflowApp(item, provider, existing = null) {
    const source = item && typeof item === "object" ? item : {};
    const current = existing && typeof existing === "object" ? existing : {};
    const appId = String(source.app_id || current.app_id || "").trim();
    if (!appId) throw new Error("workflow app id is required");
    const publishedVersionId = String(source.published_version_id || current.published_version_id || "").trim();
    const graph = source.graph !== undefined ? source.graph : current.graph;
    const hasLegacyGraph = !!(graph && typeof graph === "object");
    if (!publishedVersionId && !hasLegacyGraph) throw new Error("workflow app published_version_id is required");
    const safeGraph = hasLegacyGraph ? clone(graph) : null;
    if (!publishedVersionId && safeGraph) validateWorkflowGraph(safeGraph);
    return {
      schema_version: String(source.schema_version || current.schema_version || WORKFLOW_APP_SCHEMA_VERSION),
      provider,
      owner: String(source.owner || current.owner || "glue-python"),
      source_of_truth: String(
        source.source_of_truth
          || current.source_of_truth
          || "glue-python.governance.workflow_apps"
      ),
      app_id: appId,
      name: String(source.name || current.name || safeGraph?.name || appId).trim() || appId,
      workflow_id: String(source.workflow_id || current.workflow_id || safeGraph?.workflow_id || "").trim() || "custom",
      published_version_id: publishedVersionId,
      params_schema: clone(source.params_schema && typeof source.params_schema === "object" ? source.params_schema : (current.params_schema || {})),
      template_policy: clone(source.template_policy && typeof source.template_policy === "object" ? source.template_policy : (current.template_policy || {})),
      created_at: String(source.created_at || current.created_at || nowIso()),
      updated_at: String(source.updated_at || nowIso()),
      ...(safeGraph && !publishedVersionId ? { graph: safeGraph } : {}),
    };
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
          published_version_id: normalized.published_version_id,
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
    try {
      _ = resolveProvider(cfg);
      return await remoteListApps(limit, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
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
      return workflowStoreRemoteErrorResult(error);
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
