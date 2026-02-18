const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { runOfflineCleaning } = require("./offline_engine");
const { normalizeWorkflow, validateGraph } = require("./workflow_graph");
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

function buildWorkflowDiagnostics(nodeRuns = []) {
  const byType = {};
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
      };
    }
    byType[t].runs += 1;
    if (String(n?.status || "") !== "done") byType[t].failed += 1;
    byType[t].seconds_total += Number(n?.seconds || 0);
    byType[t].attempts_total += Number(n?.telemetry?.attempts || 1);
    if (n?.output?.rust_v2_used === true) byType[t].rust_v2_used_runs += 1;
  }
  Object.values(byType).forEach((v) => {
    v.seconds_avg = v.runs > 0 ? Number((v.seconds_total / v.runs).toFixed(3)) : 0;
    v.seconds_total = Number(v.seconds_total.toFixed(3));
  });
  return {
    generated_at: new Date().toISOString(),
    node_count: nodeRuns.length,
    chiplets: byType,
  };
}

function createWorkflowChipletRegistry() {
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
  return registry;
}

async function runMinimalWorkflow({ payload = {}, config = {}, outputRoot }) {
  const graph = normalizeWorkflow(payload);
  const validation = validateGraph(graph);
  if (!validation.ok) {
    return {
      ok: false,
      workflow_id: graph.workflow_id,
      run_id: crypto.randomUUID().replace(/-/g, ""),
      status: "invalid_graph",
      error: validation.errors.join("; "),
      node_runs: graph.nodes.map(makeNodeRun),
    };
  }

  const runId = crypto.randomUUID().replace(/-/g, "");
  const nodeRunsById = new Map(graph.nodes.map((n) => [n.id, makeNodeRun(n)]));
  const orderedNodeRuns = validation.ordered.map((n) => nodeRunsById.get(n.id));
  const ctx = {
    payload,
    config,
    outputRoot,
    workflowId: graph.workflow_id,
    runId,
    orderedNodeRuns,
    files: [],
    cleanResult: null,
    aiCorpusPath: "",
    corpusText: "",
    metrics: null,
    aiText: "",
    audit: null,
    workflowSummaryArtifact: null,
    chipletRegistry: createWorkflowChipletRegistry(),
  };

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
      workflow: graph,
      diagnostics: buildWorkflowDiagnostics(orderedNodeRuns),
    };
  } catch (e) {
    return {
      ok: false,
      workflow_id: graph.workflow_id,
      run_id: runId,
      status: "failed",
      error: String(e && e.stack ? e.stack : e),
      node_runs: orderedNodeRuns,
      clean_job_id: ctx.cleanResult?.job_id || null,
      workflow: graph,
    };
  }
}

module.exports = { runMinimalWorkflow };
