import { IO_CONTRACT_COMPATIBLE_OPERATORS } from "./preflight-rust-helpers.js";
import { getWorkflowContractResolutionHint } from "./workflow-contract.js";

function buildGraphValidationIssues(valid = {}) {
  const issues = [];
  const errorItems = Array.isArray(valid.error_items) ? valid.error_items : [];
  const warningItems = Array.isArray(valid.warning_items) ? valid.warning_items : [];
  const classifyKind = (message) => /unregistered node types/i.test(String(message || ""))
    ? "unknown_node_type"
    : "graph";
  const buildIssue = (level, msg, item = null) => {
    const message = String(msg || "");
    const errorCode = String(item?.code || "").trim();
    const errorPath = String(item?.path || "").trim();
    const errorContract = String(item?.error_contract || "").trim();
    const kind = classifyKind(message) === "unknown_node_type"
      ? "unknown_node_type"
      : (errorContract ? "graph_contract" : classifyKind(message));
    if (kind === "unknown_node_type") {
      return {
        level,
        kind,
        message,
        error_code: errorCode || "unknown_node_type",
        error_path: errorPath || "workflow.nodes",
        contract_boundary: "node_catalog_truth",
        resolution_hint: "replace node type or sync Rust manifest / local node policy",
        action_text: "定位节点",
      };
    }
    return {
      level,
      kind,
      message,
      error_code: errorCode,
      error_path: errorPath,
      error_contract: errorContract,
      resolution_hint: errorContract ? getWorkflowContractResolutionHint({ code: errorCode, path: errorPath, message }) : "",
    };
  };
  (valid.errors || []).forEach((msg, index) => issues.push(buildIssue("error", msg, errorItems[index] || null)));
  (valid.warnings || []).forEach((msg, index) => issues.push(buildIssue("warning", msg, warningItems[index] || null)));
  return issues;
}

function collectRustPreflightContext(graph, els = {}) {
  const endpoint = String(els.rustEndpoint?.value || "").trim().replace(/\/$/, "");
  const rustRequired = !!els.rustRequired?.checked;
  const rustNodes = (Array.isArray(graph?.nodes) ? graph.nodes : []).filter((n) =>
    IO_CONTRACT_COMPATIBLE_OPERATORS.has(String(n?.type || ""))
  );
  const firstInputFile = String((els.inputFiles?.value || "").split(/\r?\n/).map((s) => s.trim()).find(Boolean) || "");
  return {
    endpoint,
    rustRequired,
    rustNodes,
    firstInputFile,
  };
}

function autoFixWorkflowGraph(graph = {}) {
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes.slice() : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges.slice() : [];
  const nodeIds = new Set(nodes.map((n) => String(n?.id || "")).filter(Boolean));
  const cleanedEdges = [];
  const edgeSeen = new Set();
  let removedDup = 0;
  let removedSelf = 0;
  let removedBroken = 0;
  const dupEdges = [];
  const selfLoops = [];
  const brokenEdges = [];

  edges.forEach((e) => {
    const from = String(e?.from || "").trim();
    const to = String(e?.to || "").trim();
    if (!from || !to || !nodeIds.has(from) || !nodeIds.has(to)) {
      removedBroken += 1;
      brokenEdges.push({ from, to });
      return;
    }
    if (from === to) {
      removedSelf += 1;
      selfLoops.push({ from, to });
      return;
    }
    const key = `${from}=>${to}`;
    if (edgeSeen.has(key)) {
      removedDup += 1;
      dupEdges.push({ from, to });
      return;
    }
    edgeSeen.add(key);
    cleanedEdges.push(e);
  });

  const inDegree = new Map(nodes.map((n) => [String(n.id || ""), 0]));
  const outDegree = new Map(nodes.map((n) => [String(n.id || ""), 0]));
  cleanedEdges.forEach((e) => {
    const from = String(e?.from || "");
    const to = String(e?.to || "");
    outDegree.set(from, (outDegree.get(from) || 0) + 1);
    inDegree.set(to, (inDegree.get(to) || 0) + 1);
  });

  let removedIsolated = 0;
  const isolatedNodes = [];
  const cleanedNodes = nodes.filter((n) => {
    const id = String(n?.id || "");
    if (nodes.length <= 1) return true;
    const isolated = (inDegree.get(id) || 0) === 0 && (outDegree.get(id) || 0) === 0;
    if (isolated) {
      removedIsolated += 1;
      isolatedNodes.push({ id, type: String(n?.type || "") });
    }
    return !isolated;
  });

  const moved = removedDup + removedSelf + removedBroken + removedIsolated;
  return {
    graph: { ...graph, nodes: cleanedNodes, edges: cleanedEdges },
    summary: {
      changed: moved > 0,
      removed_dup_edges: removedDup,
      removed_self_loops: removedSelf,
      removed_broken_edges: removedBroken,
      removed_isolated_nodes: removedIsolated,
      dup_edges: dupEdges,
      self_loops: selfLoops,
      broken_edges: brokenEdges,
      isolated_nodes: isolatedNodes,
    },
  };
}

export {
  autoFixWorkflowGraph,
  buildGraphValidationIssues,
  collectRustPreflightContext,
};
