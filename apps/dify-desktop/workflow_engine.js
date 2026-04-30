const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { runOfflineCleaning } = require("./offline_engine");
const { defaultWorkflowGraph, normalizeWorkflow, topoSort } = require("./workflow_graph");
const {
  nowIso,
  collectFiles,
  readArtifactById,
  summarizeCorpus,
  sha256Text,
  makeNodeRun,
  finalizeNode,
  nodeOutputByType,
} = require("./workflow_utils");
const {
  computeViaRust,
  callExternalAi,
  auditAiText,
  writeWorkflowSummary,
} = require("./workflow_services");
const { WorkflowChipletRegistry } = require("./workflow_chiplets/registry");
const { registerBuiltinWorkflowChiplets } = require("./workflow_chiplets/builtin_chiplets");
const { executeWorkflowDag } = require("./workflow_chiplets/executor");
const { buildEnvelope } = require("./workflow_chiplets/contract");
const { runIsolatedTask } = require("./workflow_chiplets/isolated_worker_host");
const { loadExternalChiplets } = require("./workflow_chiplets/external_loader");
const {
  mergeGovernanceProfile,
  authorizeGraph,
  classifyInputFiles,
  buildLineageSummary,
  evaluateSla,
  initAiBudgetState,
} = require("./workflow_governance");
const { createWorkflowValidationSupport } = require("./workflow_validation_service");

function buildWorkflowDiagnostics(nodeRuns = []) {
  const byType = {};
  const errorKinds = {};
  for (const n of nodeRuns) {
    const t = String(n?.type || "unknown");
    if (!byType[t]) {
      byType[t] = {
        runs: 0,
        failed: 0,
        seconds_total: 0,
        seconds_avg: 0,
        attempts_total: 0,
        rust_v2_used_runs: 0,
        output_bytes_total: 0,
        output_kb_avg: 0,
      };
    }
    byType[t].runs += 1;
    if (String(n?.status || "") !== "done") byType[t].failed += 1;
    byType[t].seconds_total += Number(n?.seconds || 0);
    byType[t].attempts_total += Number(n?.telemetry?.attempts || 1);
    if (n?.output?.rust_v2_used === true) byType[t].rust_v2_used_runs += 1;
    byType[t].output_bytes_total += Number(n?.output_bytes || 0);
    const ek = String(n?.error_kind || "").trim();
    if (ek) errorKinds[ek] = (errorKinds[ek] || 0) + 1;
  }
  Object.values(byType).forEach((v) => {
    v.seconds_avg = v.runs > 0 ? Number((v.seconds_total / v.runs).toFixed(3)) : 0;
    v.output_kb_avg = v.runs > 0 ? Number(((v.output_bytes_total / v.runs) / 1024).toFixed(2)) : 0;
    v.seconds_total = Number(v.seconds_total.toFixed(3));
  });
  return {
    generated_at: new Date().toISOString(),
    node_count: nodeRuns.length,
    chiplets: byType,
    error_kinds: errorKinds,
  };
}

function createWorkflowChipletRegistry(config = {}) {
  const registry = new WorkflowChipletRegistry();
  registerBuiltinWorkflowChiplets(registry, {
    fs,
    path,
    runOfflineCleaning,
    collectFiles,
    readArtifactById,
    summarizeCorpus,
    computeViaRust,
    callExternalAi,
    auditAiText,
    writeWorkflowSummary,
    sha256Text,
    nodeOutputByType,
    runIsolatedTask,
  });
  const external = loadExternalChiplets({
    fs,
    path,
    registry,
    config,
  });
  registry.__external_report = external;
  return registry;
}

async function runMinimalWorkflow({ payload = {}, config = {}, outputRoot, nodeCache = null }) {
  const rawWorkflow =
    payload.workflow_definition && typeof payload.workflow_definition === "object"
      ? payload.workflow_definition
      : (payload.workflow && typeof payload.workflow === "object"
        ? payload.workflow
        : defaultWorkflowGraph());
  const workflowDefinition = {
    ...rawWorkflow,
    workflow_id: String(payload.workflow_id || rawWorkflow.workflow_id || "").trim() || "custom_v1",
    version: String(payload.workflow_version || rawWorkflow.version || "").trim(),
    nodes: Array.isArray(rawWorkflow.nodes) ? rawWorkflow.nodes : [],
    edges: Array.isArray(rawWorkflow.edges) ? rawWorkflow.edges : [],
  };
  const workflowValidationSupport =
    typeof config?.workflowValidationSupport?.validateWorkflowDefinitionAuthoritatively === "function"
      ? config.workflowValidationSupport
      : createWorkflowValidationSupport({ loadConfig: () => (config || {}) });
  let authoritativeValidation;
  try {
    authoritativeValidation = await workflowValidationSupport.validateWorkflowDefinitionAuthoritatively({
      workflowDefinition,
      cfg: config || {},
      allowVersionMigration: true,
      requireNonEmptyNodes: true,
      validationScope: "run",
    });
  } catch (error) {
    const remotePayload =
      error && typeof error === "object" && error.remote_payload && typeof error.remote_payload === "object"
        ? error.remote_payload
        : null;
    const normalizedFallback = normalizeWorkflow({ workflow_definition: workflowDefinition });
    return {
      ok: false,
      workflow_id: normalizedFallback.graph.workflow_id,
      run_id: crypto.randomUUID().replace(/-/g, ""),
      status: String(remotePayload?.error_code || "") === "workflow_validation_unavailable"
        ? "workflow_validation_unavailable"
        : "invalid_graph",
      error: String(remotePayload?.error || error?.message || error || "workflow validation failed"),
      error_code: String(remotePayload?.error_code || "workflow_graph_invalid"),
      graph_contract: String(remotePayload?.graph_contract || ""),
      error_item_contract: String(remotePayload?.error_item_contract || ""),
      error_items: Array.isArray(remotePayload?.error_items) ? remotePayload.error_items : [],
      notes: Array.isArray(remotePayload?.notes) ? remotePayload.notes : [],
      node_runs: normalizedFallback.graph.nodes.map(makeNodeRun),
      workflow: normalizedFallback.graph,
      workflow_contract: {
        ok: false,
        migrated: Array.isArray(remotePayload?.notes) && remotePayload.notes.length > 0,
        notes: Array.isArray(remotePayload?.notes) ? remotePayload.notes : [],
        errors: Array.isArray(remotePayload?.error_items)
          ? remotePayload.error_items.map((item) => String(item?.message || "")).filter(Boolean)
          : [String(remotePayload?.error || error?.message || error || "workflow validation failed")],
      },
    };
  }
  const normalizedWorkflow = normalizeWorkflow({
    workflow_definition: authoritativeValidation?.normalized_workflow_definition && typeof authoritativeValidation.normalized_workflow_definition === "object"
      ? authoritativeValidation.normalized_workflow_definition
      : workflowDefinition,
  });
  const graph = normalizedWorkflow.graph;
  const workflowContract = {
    ok: true,
    migrated:
      normalizedWorkflow.contract.migrated
      || (Array.isArray(authoritativeValidation?.notes) && authoritativeValidation.notes.length > 0),
    notes: Array.from(
      new Set([
        ...(Array.isArray(authoritativeValidation?.notes) ? authoritativeValidation.notes : []),
        ...(Array.isArray(normalizedWorkflow.contract.notes) ? normalizedWorkflow.contract.notes : []),
      ]),
    ),
    errors: [],
  };
  const governance = mergeGovernanceProfile(payload, config);
  const actorRole = String(payload?.actor_role || payload?.actor?.role || "owner");
  const authz = authorizeGraph(graph, actorRole, governance);
  if (!authz.ok) {
    return {
      ok: false,
      workflow_id: graph.workflow_id,
      run_id: crypto.randomUUID().replace(/-/g, ""),
      status: "forbidden_graph",
      error: `governance_forbidden_nodes:${authz.denied_nodes.map((x) => `${x.id}:${x.type}`).join(",")}`,
      governance: {
        actor_role: actorRole,
        authorization: authz,
      },
      node_runs: graph.nodes.map(makeNodeRun),
      workflow: graph,
      workflow_contract: workflowContract,
    };
  }

  const runId = crypto.randomUUID().replace(/-/g, "");
  const nodeRunsById = new Map(graph.nodes.map((n) => [n.id, makeNodeRun(n)]));
  const orderedGraphNodes = topoSort(graph.nodes, graph.edges);
  const orderedNodeRuns = (orderedGraphNodes.length === graph.nodes.length ? orderedGraphNodes : graph.nodes)
    .map((node) => nodeRunsById.get(node.id))
    .filter(Boolean);
  const ctx = {
    payload,
    config,
    outputRoot,
    workflowId: graph.workflow_id,
    runId,
    traceId: String(payload.trace_id || ""),
    orderedNodeRuns,
    files: [],
    cleanResult: null,
    aiCorpusPath: "",
    corpusText: "",
    metrics: null,
    aiText: "",
    audit: null,
    workflowSummaryArtifact: null,
    manualReviewRequests: [],
    nodeCache,
    chipletRegistry: createWorkflowChipletRegistry(config),
    externalChipletsReport: null,
    governance,
    aiBudget: initAiBudgetState(governance),
  };
  ctx.externalChipletsReport = ctx.chipletRegistry?.__external_report || null;

  try {
    await executeWorkflowDag({
      graph,
      registry: ctx.chipletRegistry,
      ctx,
      buildEnvelope,
      onNodeStart: (node) => {
        const run = nodeRunsById.get(node.id);
        run.started_at = nowIso();
      },
      onNodeSuccess: (node, output) => {
        const run = nodeRunsById.get(node.id);
        finalizeNode(run, true, output, null);
      },
      onNodeFailure: (node, err) => {
        const run = nodeRunsById.get(node.id);
        finalizeNode(run, false, null, err);
      },
    });

    const artifacts = [...(ctx.cleanResult?.artifacts || [])];
    if (ctx.workflowSummaryArtifact) artifacts.push(ctx.workflowSummaryArtifact);
    const nodeOutputs = ctx.nodeOutputs && typeof ctx.nodeOutputs === "object" ? ctx.nodeOutputs : {};
    const inputClasses = classifyInputFiles(ctx.files || []);
    const lineage = buildLineageSummary(nodeOutputs);
    const sla = evaluateSla(orderedNodeRuns, governance);
    const templateValidation = Object.entries(nodeOutputs)
      .map(([nodeId, out]) => ({
        node_id: String(nodeId || ""),
        validation_path: String(out?.validation_path || ""),
        warnings: Array.isArray(out?.warnings) ? out.warnings : [],
      }))
      .filter((x) => x.validation_path || x.warnings.length > 0);
    return {
      ok: true,
      workflow_id: graph.workflow_id,
      run_id: runId,
      status: "passed",
      mode: config.mode || "offline_local",
      node_runs: orderedNodeRuns,
      metrics: ctx.metrics || summarizeCorpus(ctx.corpusText || ""),
      audit: ctx.audit || { passed: true, reasons: [] },
      artifacts,
      clean_job_id: ctx.cleanResult?.job_id || null,
      warnings: ctx.cleanResult?.warnings || [],
      node_outputs: nodeOutputs,
      template_validation: templateValidation,
      pending_reviews: ctx.manualReviewRequests || [],
      workflow: graph,
      workflow_contract: workflowContract,
      diagnostics: buildWorkflowDiagnostics(orderedNodeRuns),
      governance: {
        actor_role: actorRole,
        authorization: authz,
        input_classes: inputClasses,
        ai_budget: ctx.aiBudget || initAiBudgetState(governance),
      },
      lineage,
      sla,
      external_chiplets: ctx.externalChipletsReport || null,
      isolation: {
        enabled: payload?.chiplet_isolation_enabled !== false && config?.chiplet_isolation_enabled !== false,
        mode: String(payload?.chiplet_isolation_mode || config?.chiplet_isolation_mode || process.env.AIWF_CHIPLET_ISOLATION_MODE || "high_risk"),
        isolated_types: Array.isArray(payload?.chiplet_isolated_types)
          ? payload.chiplet_isolated_types
          : (Array.isArray(config?.chiplet_isolated_types) ? config.chiplet_isolated_types : []),
      },
    };
  } catch (e) {
    const emsg = String(e && e.stack ? e.stack : e);
    const pending = emsg.includes("manual_review_pending:");
    return {
      ok: false,
      workflow_id: graph.workflow_id,
      run_id: runId,
      status: pending ? "pending_review" : "failed",
      error: emsg,
      node_runs: orderedNodeRuns,
      clean_job_id: ctx.cleanResult?.job_id || null,
      node_outputs: ctx.nodeOutputs || {},
      pending_reviews: ctx.manualReviewRequests || [],
      workflow: graph,
      workflow_contract: workflowContract,
      governance: {
        actor_role: actorRole,
        authorization: authz,
        input_classes: classifyInputFiles(ctx.files || []),
        ai_budget: ctx.aiBudget || initAiBudgetState(governance),
      },
      lineage: buildLineageSummary(ctx.nodeOutputs || {}),
      sla: evaluateSla(orderedNodeRuns, governance),
    };
  }
}

module.exports = { runMinimalWorkflow };
