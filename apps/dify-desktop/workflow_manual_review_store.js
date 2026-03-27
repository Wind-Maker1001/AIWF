const GLUE_PROVIDER = "glue_http";
const {
  createGovernanceGlueStoreSupport,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_DEFAULT_GLUE_URL,
} = require("./workflow_governance");
const {
  workflowStoreRemoteErrorResult,
} = require("./workflow_store_remote_error");

function createWorkflowManualReviewStore(deps = {}) {
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
    providerConfigKey: "manualReviewProvider",
    providerEnvKey: "AIWF_MANUAL_REVIEW_PROVIDER",
    providerLabel: "workflow manual review",
  });

  function normalizeReviewItem(item, existing = null) {
    const source = item && typeof item === "object" ? item : {};
    const current = existing && typeof existing === "object" ? existing : {};
    const runId = String(source.run_id || current.run_id || "").trim();
    const reviewKey = String(source.review_key || source.node_id || current.review_key || current.node_id || "").trim();
    if (!runId) throw new Error("manual review run_id is required");
    if (!reviewKey) throw new Error("manual review review_key is required");
    return {
      schema_version: String(source.schema_version || current.schema_version || "manual_review_item.v1"),
      provider: "",
      owner: String(source.owner || current.owner || ""),
      source_of_truth: String(source.source_of_truth || current.source_of_truth || ""),
      run_id: runId,
      review_key: reviewKey,
      workflow_id: String(source.workflow_id || current.workflow_id || "").trim(),
      node_id: String(source.node_id || current.node_id || reviewKey).trim(),
      reviewer: String(source.reviewer || current.reviewer || "").trim(),
      comment: String(source.comment || current.comment || "").trim(),
      created_at: String(source.created_at || current.created_at || new Date().toISOString()),
      decided_at: String(source.decided_at || current.decided_at || "").trim(),
      status: String(source.status || current.status || "pending").trim().toLowerCase() || "pending",
      approved: "approved" in source ? !!source.approved : !!current.approved,
    };
  }

  async function remoteListQueue(limit = 200, cfg = null) {
    const safe = Math.max(1, Math.min(5000, Number(limit || 200)));
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.MANUAL_REVIEWS.capability, { cfg });
    const payload = await remoteRequest("GET", `${routePrefix}?limit=${safe}`, null, cfg);
    const items = (Array.isArray(payload?.items) ? payload.items : []).map((item) => {
      const normalized = normalizeReviewItem(item, item);
      normalized.provider = GLUE_PROVIDER;
      normalized.owner = normalized.owner || "glue-python";
      normalized.source_of_truth = normalized.source_of_truth || "glue-python.governance.manual_reviews";
      return normalized;
    });
    return { ok: true, provider: GLUE_PROVIDER, items };
  }

  async function remoteEnqueue(items, cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.MANUAL_REVIEWS.capability, { cfg });
    const payload = await remoteRequest(
      "POST",
      `${routePrefix}/enqueue`,
      { items: Array.isArray(items) ? items : [] },
      cfg
    );
    const normalizedItems = (Array.isArray(payload?.items) ? payload.items : []).map((item) => {
      const normalized = normalizeReviewItem(item, item);
      normalized.provider = GLUE_PROVIDER;
      normalized.owner = normalized.owner || "glue-python";
      normalized.source_of_truth = normalized.source_of_truth || "glue-python.governance.manual_reviews";
      return normalized;
    });
    return { ok: true, provider: GLUE_PROVIDER, items: normalizedItems };
  }

  async function remoteListHistory(limit = 200, filter = {}, cfg = null) {
    const search = new URLSearchParams();
    search.set("limit", String(Math.max(1, Math.min(5000, Number(limit || 200)))));
    Object.entries(filter && typeof filter === "object" ? filter : {}).forEach(([key, value]) => {
      const text = String(value || "").trim();
      if (text) search.set(key, text);
    });
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.MANUAL_REVIEWS.capability, { cfg });
    const payload = await remoteRequest("GET", `${routePrefix}/history?${search.toString()}`, null, cfg);
    const items = (Array.isArray(payload?.items) ? payload.items : []).map((item) => {
      const normalized = normalizeReviewItem(item, item);
      normalized.provider = GLUE_PROVIDER;
      normalized.owner = normalized.owner || "glue-python";
      normalized.source_of_truth = normalized.source_of_truth || "glue-python.governance.manual_reviews";
      return normalized;
    });
    return { ok: true, provider: GLUE_PROVIDER, items };
  }

  async function remoteSubmit(req, cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.MANUAL_REVIEWS.capability, { cfg });
    const payload = await remoteRequest("POST", `${routePrefix}/submit`, {
      run_id: String(req?.run_id || "").trim(),
      review_key: String(req?.review_key || "").trim(),
      approved: !!req?.approved,
      reviewer: String(req?.reviewer || "reviewer").trim(),
      comment: String(req?.comment || "").trim(),
    }, cfg);
    const item = normalizeReviewItem(payload?.item || {}, payload?.item || {});
    item.provider = GLUE_PROVIDER;
    item.owner = item.owner || "glue-python";
    item.source_of_truth = item.source_of_truth || "glue-python.governance.manual_reviews";
    return { ok: true, provider: GLUE_PROVIDER, item, remaining: Number(payload?.remaining || 0) };
  }

  async function listQueue(limit = 200, cfg = null) {
    try {
      _ = resolveProvider(cfg);
      return await remoteListQueue(limit, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function enqueue(items = [], cfg = null) {
    try {
      _ = resolveProvider(cfg);
      return await remoteEnqueue(items, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function listHistory(req = {}, cfg = null) {
    const limit = Number(req?.limit || 200);
    const filter = req?.filter && typeof req.filter === "object" ? req.filter : {};
    try {
      _ = resolveProvider(cfg);
      return await remoteListHistory(limit, filter, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function submit(req = {}, cfg = null) {
    try {
      _ = resolveProvider(cfg);
      return await remoteSubmit(req, cfg);
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  return {
    enqueue,
    listHistory,
    listQueue,
    resolveGlueUrl,
    resolveProvider,
    submit,
  };
}

module.exports = {
  GLUE_PROVIDER,
  createWorkflowManualReviewStore,
};
