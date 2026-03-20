import { IO_CONTRACT_COMPATIBLE_OPERATORS } from "./preflight-rust-helpers.js";

function buildGraphValidationIssues(valid = {}) {
  const issues = [];
  (valid.errors || []).forEach((msg) => issues.push({ level: "error", kind: "graph", message: String(msg) }));
  (valid.warnings || []).forEach((msg) => issues.push({ level: "warning", kind: "graph", message: String(msg) }));
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
