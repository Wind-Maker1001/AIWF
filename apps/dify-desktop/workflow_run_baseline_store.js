const GLUE_PROVIDER = "glue_http";
const GLUE_DEFAULT_URL = "http://127.0.0.1:18081";
const { createGovernanceControlPlaneSupport, GOVERNANCE_CAPABILITIES } = require("./workflow_governance");

function createWorkflowRunBaselineStore(deps = {}) {
  const {
    loadConfig = () => ({}),
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
    if (raw === "local_legacy") throw new Error("workflow run baseline local_legacy provider has been retired; use glue_http");
    return "";
  }

  function resolveProvider(cfg = null) {
    const merged = mergedConfig(cfg);
    const explicit = normalizeProvider(merged.runBaselineProvider || env.AIWF_RUN_BASELINE_PROVIDER);
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

  function normalizeBaseline(item, provider, existing = null) {
    const source = item && typeof item === "object" ? item : {};
    const current = existing && typeof existing === "object" ? existing : {};
    const baselineId = String(source.baseline_id || current.baseline_id || "").trim();
    const runId = String(source.run_id || current.run_id || "").trim();
    if (!baselineId) throw new Error("baseline_id is required");
    if (!runId) throw new Error("run_id is required");
    return {
      schema_version: String(source.schema_version || current.schema_version || "run_baseline_entry.v1"),
      provider,
      owner: String(source.owner || current.owner || (provider === GLUE_PROVIDER ? "glue-python" : "desktop.local_legacy")),
      source_of_truth: String(
        source.source_of_truth
          || current.source_of_truth
          || (provider === GLUE_PROVIDER ? "glue-python.governance.run_baselines" : "desktop.workflow_store.run_baselines")
      ),
      baseline_id: baselineId,
      name: String(source.name || current.name || runId).trim() || runId,
      run_id: runId,
      workflow_id: String(source.workflow_id || current.workflow_id || "").trim(),
      created_at: String(source.created_at || current.created_at || new Date().toISOString()),
      notes: String(source.notes || current.notes || "").trim(),
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
      throw new Error(String(payload?.error || `run baseline ${method} failed: http ${resp.status}`));
    }
    return payload;
  }

  async function remoteList(limit = 200, cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.RUN_BASELINES.capability, { cfg });
    const payload = await remoteRequest(
      "GET",
      `${routePrefix}?limit=${Math.max(1, Math.min(5000, Number(limit || 200)))}`,
      null,
      cfg
    );
    const items = [];
    for (const item of (Array.isArray(payload?.items) ? payload.items : [])) {
      try {
        items.push(normalizeBaseline(item, GLUE_PROVIDER, item));
      } catch {}
    }
    return { ok: true, provider: GLUE_PROVIDER, items };
  }

  async function remoteSave(req, cfg = null) {
    const baselineId = String(req?.baseline_id || "").trim();
    if (!baselineId) throw new Error("baseline_id is required");
    const normalized = normalizeBaseline(req, GLUE_PROVIDER, req);
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.RUN_BASELINES.capability, { cfg });
    const payload = await remoteRequest(
      "PUT",
      `${routePrefix}/${encodeURIComponent(baselineId)}`,
      {
        baseline: {
          baseline_id: normalized.baseline_id,
          name: normalized.name,
          run_id: normalized.run_id,
          workflow_id: normalized.workflow_id,
          created_at: normalized.created_at,
          notes: normalized.notes,
        },
      },
      cfg
    );
    return { ok: true, provider: GLUE_PROVIDER, item: normalizeBaseline(payload?.item || normalized, GLUE_PROVIDER, payload?.item || normalized) };
  }

  async function list(limit = 200, cfg = null) {
    try {
      _ = resolveProvider(cfg);
      return await remoteList(limit, cfg);
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  }

  async function save(req, cfg = null) {
    try {
      _ = resolveProvider(cfg);
      return await remoteSave(req, cfg);
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  }

  return {
    list,
    resolveGlueUrl,
    resolveProvider,
    save,
  };
}

module.exports = {
  GLUE_PROVIDER,
  createWorkflowRunBaselineStore,
};
