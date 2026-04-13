const GLUE_PROVIDER = "glue_http";
const {
  createGovernanceGlueStoreSupport,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_DEFAULT_GLUE_URL,
} = require("./workflow_governance");
const {
  workflowStoreRemoteErrorResult,
} = require("./workflow_store_remote_error");

function createWorkflowRunBaselineStore(deps = {}) {
  const {
    loadConfig = () => ({}),
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
    providerConfigKey: "runBaselineProvider",
    providerEnvKey: "AIWF_RUN_BASELINE_PROVIDER",
    providerLabel: "workflow run baseline",
  });

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
      owner: String(source.owner || current.owner || "glue-python"),
      source_of_truth: String(
        source.source_of_truth
          || current.source_of_truth
          || "glue-python.governance.run_baselines"
      ),
      baseline_id: baselineId,
      name: String(source.name || current.name || runId).trim() || runId,
      run_id: runId,
      workflow_id: String(source.workflow_id || current.workflow_id || "").trim(),
      created_at: String(source.created_at || current.created_at || new Date().toISOString()),
      notes: String(source.notes || current.notes || "").trim(),
    };
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
      resolveProvider(cfg);
      return await remoteList(limit, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function save(req, cfg = null) {
    try {
      resolveProvider(cfg);
      return await remoteSave(req, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
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
