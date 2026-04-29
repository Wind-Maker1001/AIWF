const { createWorkflowStoreRemoteError } = require("./workflow_store_remote_error");
const { resolveWorkflowDefinitionPayload } = require("./workflow_graph");

const DEFAULT_ACCEL_URL = "http://127.0.0.1:18082";

function normalizeBaseUrl(url) {
  return String(url || "").trim().replace(/\/$/, "");
}

function parseResponseText(text) {
  const raw = String(text || "").trim();
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    return { ok: false, error: raw };
  }
}

function resolveAccelUrl(loadConfig = () => ({}), env = process.env, cfg = null) {
  const merged = { ...loadConfig(), ...(cfg && typeof cfg === "object" ? cfg : {}) };
  return normalizeBaseUrl(
    merged.accelUrl
    || merged.rustEndpoint
    || env.AIWF_ACCEL_URL
    || DEFAULT_ACCEL_URL,
  );
}

function mapNodeRun(step = {}) {
  return {
    id: String(step?.id || "").trim(),
    type: String(step?.operator || "").trim(),
    status: String(step?.status || "").trim() === "done" ? "done" : String(step?.status || "").trim() || "failed",
    started_at: String(step?.started_at || ""),
    ended_at: String(step?.finished_at || ""),
    seconds: Number(step?.duration_ms || 0) / 1000,
    error: String(step?.error || ""),
    output: step?.output_summary && typeof step.output_summary === "object" ? step.output_summary : {},
    telemetry: { attempts: 1 },
  };
}

function mapArtifacts(finalOutput = {}) {
  const outputs = finalOutput?.outputs && typeof finalOutput.outputs === "object" ? finalOutput.outputs : {};
  const kinds = {
    cleaned_csv: "csv",
    cleaned_parquet: "parquet",
    profile_json: "json",
    xlsx_fin: "xlsx",
    audit_docx: "docx",
    deck_pptx: "pptx",
  };
  return Object.entries(outputs)
    .filter(([, value]) => value && typeof value === "object" && String(value.path || "").trim())
    .map(([key, value]) => ({
      artifact_id: key,
      kind: kinds[key] || key,
      path: String(value.path || ""),
      sha256: String(value.sha256 || ""),
    }));
}

function executionStatus(payload = {}) {
  const execution = payload?.execution && typeof payload.execution === "object" ? payload.execution : {};
  const finalOutput = payload?.final_output && typeof payload.final_output === "object" ? payload.final_output : {};
  return String(payload?.status || finalOutput?.status || execution?.status || "").trim().toLowerCase();
}

function hasStructuredExecutionResult(payload = {}) {
  if (!payload || typeof payload !== "object") return false;
  if (payload.execution && typeof payload.execution === "object") return true;
  if (payload.final_output && typeof payload.final_output === "object") return true;
  const status = executionStatus(payload);
  return !!status;
}

function mapRustDraftExecutionToDesktop(payload = {}, workflowDefinition = {}) {
  const execution = payload?.execution && typeof payload.execution === "object" ? payload.execution : {};
  const finalOutput = payload?.final_output && typeof payload.final_output === "object" ? payload.final_output : {};
  const nodeRuns = Array.isArray(execution?.steps) ? execution.steps.map(mapNodeRun) : [];
  const pendingReviews = Array.isArray(finalOutput?.pending_reviews) ? finalOutput.pending_reviews : [];
  const finalStatus = String(finalOutput?.status || "").trim();
  const blocked = finalStatus === "quality_blocked";
  const pending = finalStatus === "pending_review" || pendingReviews.length > 0;
  const finalOk = typeof finalOutput?.ok === "boolean" ? finalOutput.ok : !!payload?.ok;
  const ok = pending || blocked ? false : (!!payload?.ok && finalOk);
  const status = pending
    ? "pending_review"
    : (blocked ? "quality_blocked" : String(payload?.status || execution?.status || finalStatus || ""));
  return {
    ok,
    operator: String(payload?.operator || "workflow_draft_run_v1"),
    workflow_id: String(payload?.workflow_id || workflowDefinition?.workflow_id || ""),
    run_id: String(payload?.run_id || execution?.run_id || ""),
    status,
    node_runs: nodeRuns,
    node_outputs: execution?.context && typeof execution.context === "object" ? execution.context : {},
    artifacts: mapArtifacts(finalOutput),
    pending_reviews: pendingReviews,
    workflow_definition: workflowDefinition,
    workflow: workflowDefinition,
    workflow_contract: {
      ok: true,
      migrated: false,
      notes: [],
      errors: [],
    },
    compiled_plan: payload?.compiled_plan && typeof payload.compiled_plan === "object" ? payload.compiled_plan : {},
    execution,
    final_output: finalOutput,
    run_request_kind: String(payload?.run_request_kind || "draft"),
    workflow_definition_source: String(payload?.workflow_definition_source || "draft_inline"),
    compatibility_fallback: false,
  };
}

function createWorkflowExecutionSupport(deps = {}) {
  const {
    loadConfig = () => ({}),
    fetchImpl = typeof fetch === "function" ? fetch : null,
    env = process.env,
  } = deps;

  async function executeDraftWorkflowAuthoritatively(options = {}) {
    const payload = options?.payload && typeof options.payload === "object" ? options.payload : {};
    const workflowDefinition = resolveWorkflowDefinitionPayload(payload) || {};
    const cfg = options?.cfg && typeof options.cfg === "object" ? options.cfg : null;
    const baseUrl = resolveAccelUrl(loadConfig, env, cfg);

    if (typeof fetchImpl !== "function") {
      throw createWorkflowStoreRemoteError({
        ok: false,
        error: "workflow draft execution unavailable: fetch is not available",
        error_code: "workflow_draft_execution_unavailable",
      });
    }

    let resp;
    let responsePayload = {};
    try {
      resp = await fetchImpl(`${baseUrl}/operators/workflow_draft_run_v1`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workflow_definition: workflowDefinition,
          run_id: String(payload?.run_id || ""),
          trace_id: String(payload?.trace_id || ""),
          traceparent: String(payload?.traceparent || ""),
          tenant_id: String(payload?.tenant_id || ""),
          job_id: String(payload?.job_id || payload?.workflow_id || ""),
          job_context: payload?.params?.job_context && typeof payload.params.job_context === "object"
            ? payload.params.job_context
            : {},
          params: payload?.params && typeof payload.params === "object" ? payload.params : {},
        }),
      });
      responsePayload = parseResponseText(await resp.text());
    } catch (error) {
      throw createWorkflowStoreRemoteError({
        ok: false,
        error: `workflow draft execution unavailable: ${String(error?.message || error || "unknown error")}`,
        error_code: "workflow_draft_execution_unavailable",
      });
    }

    if (!resp.ok || (responsePayload?.ok === false && !hasStructuredExecutionResult(responsePayload))) {
      throw createWorkflowStoreRemoteError({
        ok: false,
        ...responsePayload,
        error: String(responsePayload?.error || `workflow draft execution invalid: http ${resp.status}`),
        error_code: String(responsePayload?.error_code || "workflow_draft_execution_invalid"),
      });
    }

    responsePayload.run_request_kind = "draft";
    responsePayload.workflow_definition_source = "draft_inline";
    return mapRustDraftExecutionToDesktop(responsePayload, workflowDefinition);
  }

  async function executeReferenceWorkflowAuthoritatively(options = {}) {
    const payload = options?.payload && typeof options.payload === "object" ? options.payload : {};
    const workflowDefinition = resolveWorkflowDefinitionPayload(payload) || {};
    const cfg = options?.cfg && typeof options.cfg === "object" ? options.cfg : null;
    const baseUrl = resolveAccelUrl(loadConfig, env, cfg);

    if (typeof fetchImpl !== "function") {
      throw createWorkflowStoreRemoteError({
        ok: false,
        error: "workflow reference execution unavailable: fetch is not available",
        error_code: "workflow_reference_execution_unavailable",
      });
    }

    let resp;
    let responsePayload = {};
    try {
      resp = await fetchImpl(`${baseUrl}/operators/workflow_reference_run_v1`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workflow_definition: workflowDefinition,
          version_id: String(payload?.version_id || ""),
          published_version_id: String(payload?.published_version_id || ""),
          run_id: String(payload?.run_id || ""),
          trace_id: String(payload?.trace_id || ""),
          traceparent: String(payload?.traceparent || ""),
          tenant_id: String(payload?.tenant_id || ""),
          job_id: String(payload?.job_id || payload?.workflow_id || ""),
          job_context: payload?.params?.job_context && typeof payload.params.job_context === "object"
            ? payload.params.job_context
            : {},
          params: payload?.params && typeof payload.params === "object" ? payload.params : {},
        }),
      });
      responsePayload = parseResponseText(await resp.text());
    } catch (error) {
      throw createWorkflowStoreRemoteError({
        ok: false,
        error: `workflow reference execution unavailable: ${String(error?.message || error || "unknown error")}`,
        error_code: "workflow_reference_execution_unavailable",
      });
    }

    if (!resp.ok || (responsePayload?.ok === false && !hasStructuredExecutionResult(responsePayload))) {
      throw createWorkflowStoreRemoteError({
        ok: false,
        ...responsePayload,
        error: String(responsePayload?.error || `workflow reference execution invalid: http ${resp.status}`),
        error_code: String(responsePayload?.error_code || "workflow_reference_execution_invalid"),
      });
    }

    responsePayload.run_request_kind = "reference";
    responsePayload.workflow_definition_source = "version_reference";
    return mapRustDraftExecutionToDesktop(responsePayload, workflowDefinition);
  }

  return {
    executeReferenceWorkflowAuthoritatively,
    executeDraftWorkflowAuthoritatively,
    mapRustDraftExecutionToDesktop,
    resolveAccelUrl: (cfg = null) => resolveAccelUrl(loadConfig, env, cfg),
  };
}

module.exports = {
  createWorkflowExecutionSupport,
  mapRustDraftExecutionToDesktop,
};
