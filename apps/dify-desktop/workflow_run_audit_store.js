const GLUE_PROVIDER = "glue_http";
const GLUE_DEFAULT_URL = "http://127.0.0.1:18081";
const {
  createGovernanceControlPlaneSupport,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS,
} = require("./workflow_governance");
const WORKFLOW_RUN_ENTRY_SCHEMA_VERSION = "workflow_run_entry.v1";
const WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION = "workflow_audit_event.v1";
const WORKFLOW_RUN_TIMELINE_SCHEMA_VERSION = "workflow_run_timeline.v1";
const WORKFLOW_FAILURE_SUMMARY_SCHEMA_VERSION = "workflow_failure_summary.v1";

function createWorkflowRunAuditStore(deps = {}) {
  const {
    loadConfig = () => ({}),
    fs,
    runHistoryPath,
    workflowAuditPath,
    fetchImpl = typeof fetch === "function" ? fetch : null,
    env = process.env,
  } = deps;
  const governance = createGovernanceControlPlaneSupport({ loadConfig, fetchImpl, env, defaultGlueUrl: GLUE_DEFAULT_URL });

  function mergedConfig(cfg = null) {
    return { ...loadConfig(), ...(cfg && typeof cfg === "object" ? cfg : {}) };
  }

  function normalizeProvider(value) {
    const raw = String(value || "").trim().toLowerCase();
    if (raw === GLUE_PROVIDER) return GLUE_PROVIDER;
    if (raw === "local_legacy") throw new Error("workflow run audit local_legacy provider has been retired; use glue_http");
    return "";
  }

  function resolveProvider(cfg = null) {
    const merged = mergedConfig(cfg);
    const explicit = normalizeProvider(merged.workflowRunAuditProvider || env.AIWF_WORKFLOW_RUN_AUDIT_PROVIDER);
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

  function normalizeRunItem(item) {
    const source = item && typeof item === "object" ? item : {};
    const runId = String(source.run_id || "").trim();
    if (!runId) throw new Error("run_id is required");
    return {
      schema_version: String(source.schema_version || WORKFLOW_RUN_ENTRY_SCHEMA_VERSION),
      provider: GLUE_PROVIDER,
      owner: String(source.owner || "glue-python"),
      source_of_truth: String(source.source_of_truth || "glue-python.governance.workflow_runs"),
      run_id: runId,
      ts: String(source.ts || new Date().toISOString()),
      workflow_id: String(source.workflow_id || "").trim(),
      status: String(source.status || "").trim(),
      ok: !!source.ok,
      payload: clone(source.payload && typeof source.payload === "object" ? source.payload : {}),
      config: clone(source.config && typeof source.config === "object" ? source.config : {}),
      result: clone(source.result && typeof source.result === "object" ? source.result : source),
    };
  }

  function normalizeAuditEvent(item) {
    const source = item && typeof item === "object" ? item : {};
    return {
      schema_version: String(source.schema_version || WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION),
      provider: GLUE_PROVIDER,
      owner: String(source.owner || "glue-python"),
      source_of_truth: String(source.source_of_truth || "glue-python.governance.workflow_audit_events"),
      ts: String(source.ts || new Date().toISOString()),
      action: String(source.action || "unknown").trim() || "unknown",
      detail: clone(source.detail && typeof source.detail === "object" ? source.detail : {}),
    };
  }

  function normalizeTimeline(payload) {
    const source = payload && typeof payload === "object" ? payload : {};
    return {
      schema_version: String(source.schema_version || WORKFLOW_RUN_TIMELINE_SCHEMA_VERSION),
      ok: source.ok !== false,
      provider: String(source.provider || "glue-python"),
      owner: String(source.owner || "glue-python"),
      source_of_truth: String(source.source_of_truth || "glue-python.governance.workflow_runs"),
      run_id: String(source.run_id || "").trim(),
      status: String(source.status || "").trim(),
      timeline: Array.isArray(source.timeline) ? clone(source.timeline) : [],
    };
  }

  function normalizeFailureSummary(payload) {
    const source = payload && typeof payload === "object" ? payload : {};
    return {
      schema_version: String(source.schema_version || WORKFLOW_FAILURE_SUMMARY_SCHEMA_VERSION),
      ok: source.ok !== false,
      provider: String(source.provider || "glue-python"),
      owner: String(source.owner || "glue-python"),
      source_of_truth: String(source.source_of_truth || "glue-python.governance.workflow_runs"),
      total_runs: Number(source.total_runs || 0),
      failed_runs: Number(source.failed_runs || 0),
      by_node: clone(source.by_node && typeof source.by_node === "object" ? source.by_node : {}),
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
      throw new Error(String(payload?.error || `workflow run audit ${method} failed: http ${resp.status}`));
    }
    return payload;
  }

  async function mirrorRun(run, payload, config, cfg = null) {
    const provider = resolveProvider(cfg || config);
    if (provider !== GLUE_PROVIDER) return { ok: true, provider };
    const runId = String(run?.run_id || "").trim();
    if (!runId) return { ok: false, error: "run_id required" };
    const normalizedRun = normalizeRunItem({
      ...run,
      payload: payload && typeof payload === "object" ? payload : {},
      config: config && typeof config === "object" ? config : {},
      result: run && typeof run === "object" ? run : {},
    });
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_RUN_AUDIT.capability, { cfg: cfg || config });
    await remoteRequest("PUT", `${routePrefix}/${encodeURIComponent(runId)}`, {
      run: normalizedRun,
    }, cfg || config);
    return { ok: true, provider };
  }

  async function mirrorAudit(action, detail, cfg = null) {
    const provider = resolveProvider(cfg);
    if (provider !== GLUE_PROVIDER) return { ok: true, provider };
    const event = normalizeAuditEvent({ action, detail });
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_RUN_AUDIT.capability, {
      cfg,
      preferredOwnedPrefix: GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS.WORKFLOW_RUN_AUDIT.WORKFLOW_AUDIT_EVENTS,
    });
    await remoteRequest("POST", routePrefix, {
      event,
    }, cfg);
    return { ok: true, provider };
  }

  async function listRuns(limit = 200, cfg = null) {
    try {
      const provider = resolveProvider(cfg);
      const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_RUN_AUDIT.capability, { cfg });
      const payload = await remoteRequest("GET", `${routePrefix}?limit=${Math.max(1, Math.min(5000, Number(limit || 200)))}`, null, cfg);
      const items = [];
      for (const item of (Array.isArray(payload?.items) ? payload.items : [])) {
        try {
          items.push(normalizeRunItem(item));
        } catch {}
      }
      return { ok: true, provider, items };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  }

  async function getRun(runId, cfg = null) {
    try {
      const provider = resolveProvider(cfg);
      const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_RUN_AUDIT.capability, { cfg });
      const payload = await remoteRequest("GET", `${routePrefix}/${encodeURIComponent(String(runId || "").trim())}`, null, cfg);
      return payload?.item ? normalizeRunItem(payload.item) : null;
    } catch (error) {
      if (/run not found/i.test(String(error))) return null;
      throw error;
    }
  }

  async function getRunTimeline(runId, cfg = null) {
    try {
      _ = resolveProvider(cfg);
      const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_RUN_AUDIT.capability, { cfg });
      const payload = await remoteRequest("GET", `${routePrefix}/${encodeURIComponent(String(runId || "").trim())}/timeline`, null, cfg);
      return normalizeTimeline(payload);
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  }

  async function getFailureSummary(limit = 400, cfg = null) {
    try {
      _ = resolveProvider(cfg);
      const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_RUN_AUDIT.capability, { cfg });
      const payload = await remoteRequest("GET", `${routePrefix}/failure-summary?limit=${Math.max(1, Math.min(5000, Number(limit || 400)))}`, null, cfg);
      return normalizeFailureSummary(payload);
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  }

  async function listAuditLogs(limit = 200, action = "", cfg = null) {
    try {
      _ = resolveProvider(cfg);
      const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_RUN_AUDIT.capability, {
        cfg,
        preferredOwnedPrefix: GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS.WORKFLOW_RUN_AUDIT.WORKFLOW_AUDIT_EVENTS,
      });
      const route = `${routePrefix}?limit=${Math.max(1, Math.min(5000, Number(limit || 200)))}&action=${encodeURIComponent(String(action || "").trim())}`;
      const payload = await remoteRequest("GET", route, null, cfg);
      const items = [];
      for (const item of (Array.isArray(payload?.items) ? payload.items : [])) {
        items.push(normalizeAuditEvent(item));
      }
      return {
        ok: payload?.ok !== false,
        provider: GLUE_PROVIDER,
        owner: String(payload?.owner || "glue-python"),
        source_of_truth: String(payload?.source_of_truth || "glue-python.governance.workflow_audit_events"),
        items,
      };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  }

  return {
    getFailureSummary,
    getRun,
    getRunTimeline,
    listAuditLogs,
    listRuns,
    mirrorAudit,
    mirrorRun,
    resolveGlueUrl,
    resolveProvider,
  };
}

module.exports = {
  GLUE_PROVIDER,
  WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION,
  WORKFLOW_FAILURE_SUMMARY_SCHEMA_VERSION,
  WORKFLOW_RUN_ENTRY_SCHEMA_VERSION,
  WORKFLOW_RUN_TIMELINE_SCHEMA_VERSION,
  createWorkflowRunAuditStore,
};
