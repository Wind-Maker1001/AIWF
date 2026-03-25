const GLUE_PROVIDER = "glue_http";
const GLUE_DEFAULT_URL = "http://127.0.0.1:18081";
const {
  createGovernanceControlPlaneSupport,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS,
} = require("./workflow_governance");
const WORKFLOW_SANDBOX_AUTOFIX_STATE_SCHEMA_VERSION = "workflow_sandbox_autofix_state.v1";
const WORKFLOW_SANDBOX_AUTOFIX_ACTION_SCHEMA_VERSION = "workflow_sandbox_autofix_action.v1";

function createWorkflowSandboxAutoFixStore(deps = {}) {
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
    if (raw === "local_legacy") throw new Error("workflow sandbox autofix local_legacy provider has been retired; use glue_http");
    return "";
  }

  function resolveProvider(cfg = null) {
    const merged = mergedConfig(cfg);
    const explicit = normalizeProvider(merged.workflowSandboxAutoFixProvider || env.AIWF_WORKFLOW_SANDBOX_AUTOFIX_PROVIDER);
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

  function normalizeState(state, existing = null) {
    const source = existing && typeof existing === "object" ? existing : {};
    const current = state && typeof state === "object" ? state : {};
    return {
      schema_version: String(current.schema_version || source.schema_version || WORKFLOW_SANDBOX_AUTOFIX_STATE_SCHEMA_VERSION),
      owner: String(current.owner || source.owner || "glue-python"),
      source_of_truth: String(current.source_of_truth || source.source_of_truth || "glue-python.governance.workflow_sandbox.autofix_state"),
      violation_events: Array.isArray(current.violation_events) ? clone(current.violation_events) : [],
      forced_isolation_mode: String(current.forced_isolation_mode || ""),
      forced_until: String(current.forced_until || ""),
      last_actions: Array.isArray(current.last_actions) ? clone(current.last_actions) : [],
      green_streak: Number(current.green_streak || 0),
    };
  }

  function normalizeActionItem(item) {
    const source = item && typeof item === "object" ? item : {};
    return {
      schema_version: String(source.schema_version || WORKFLOW_SANDBOX_AUTOFIX_ACTION_SCHEMA_VERSION),
      provider: GLUE_PROVIDER,
      owner: String(source.owner || "glue-python"),
      source_of_truth: String(source.source_of_truth || "glue-python.governance.workflow_sandbox.autofix_actions"),
      ts: String(source.ts || ""),
      actions: Array.isArray(source.actions) ? clone(source.actions) : [],
      run_id: String(source.run_id || "").trim(),
      node_id: String(source.node_id || "").trim(),
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
      throw new Error(String(payload?.error || `sandbox autofix ${method} failed: http ${resp.status}`));
    }
    return payload;
  }

  async function remoteGetState(cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_SANDBOX_AUTOFIX.capability, { cfg });
    const payload = await remoteRequest("GET", routePrefix, null, cfg);
    return {
      ok: true,
      provider: GLUE_PROVIDER,
      state: normalizeState(payload?.state, payload?.state),
    };
  }

  async function remoteSaveState(state, cfg = null) {
    const normalizedState = normalizeState(state, state);
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_SANDBOX_AUTOFIX.capability, { cfg });
    const payload = await remoteRequest("PUT", routePrefix, normalizedState, cfg);
    return {
      ok: true,
      provider: GLUE_PROVIDER,
      state: normalizeState(payload?.state, payload?.state || normalizedState),
    };
  }

  async function remoteListActions(limit = 100, cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_SANDBOX_AUTOFIX.capability, {
      cfg,
      preferredOwnedPrefix: GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS.WORKFLOW_SANDBOX_AUTOFIX.AUTOFIX_ACTIONS,
    });
    const payload = await remoteRequest(
      "GET",
      `${routePrefix}?limit=${Math.max(1, Math.min(1000, Number(limit || 100)))}`,
      null,
      cfg
    );
    return {
      ok: true,
      provider: GLUE_PROVIDER,
      items: (Array.isArray(payload?.items) ? payload.items : []).map((item) => normalizeActionItem(item)),
      forced_isolation_mode: String(payload?.forced_isolation_mode || ""),
      forced_until: String(payload?.forced_until || ""),
    };
  }

  async function persistStateMirror(state, cfg = null) {
    const provider = resolveProvider(cfg);
    if (provider !== GLUE_PROVIDER) return { ok: true, provider };
    return await remoteSaveState(state, cfg);
  }

  async function getState(cfg = null) {
    _ = resolveProvider(cfg);
    return remoteGetState(cfg);
  }

  async function applyPayload(payload = {}, cfg = null) {
    _ = resolveProvider(cfg);
    const out = await remoteGetState(cfg);
    return sandboxSupport.applySandboxAutoFixPayload(payload || {}, out?.state || {});
  }

  async function processRunAutoFix(run, payload = {}, cfg = null) {
    _ = resolveProvider(cfg);
    const out = await remoteGetState(cfg);
    return await sandboxSupport.maybeApplySandboxAutoFix(run, payload || {}, {
      state: out?.state || {},
      writeLocalState: false,
      persistState: async (nextState) => await remoteSaveState(nextState, cfg),
    });
  }

  async function listActions(limit = 100, cfg = null) {
    _ = resolveProvider(cfg);
    return remoteListActions(limit, cfg);
  }

  return {
    applyPayload,
    getState,
    listActions,
    persistStateMirror,
    processRunAutoFix,
    resolveGlueUrl,
    resolveProvider,
  };
}

module.exports = {
  GLUE_PROVIDER,
  WORKFLOW_SANDBOX_AUTOFIX_ACTION_SCHEMA_VERSION,
  WORKFLOW_SANDBOX_AUTOFIX_STATE_SCHEMA_VERSION,
  createWorkflowSandboxAutoFixStore,
};
