function defaultWorkflowGraph() {
  return {
    workflow_id: "minimal_v1",
    nodes: [
      { id: "n1", type: "ingest_files" },
      { id: "n2", type: "clean_md" },
      { id: "n3", type: "compute_rust" },
      { id: "n4", type: "ai_refine" },
      { id: "n5", type: "ai_audit" },
      { id: "n6", type: "md_output" },
    ],
    edges: [
      { from: "n1", to: "n2" },
      { from: "n2", to: "n3" },
      { from: "n3", to: "n4" },
      { from: "n4", to: "n5" },
      { from: "n5", to: "n6" },
    ],
  };
}

function normalizeWorkflow(payload = {}) {
  const wf = payload.workflow && Array.isArray(payload.workflow.nodes)
    ? payload.workflow
    : defaultWorkflowGraph();
  const nodes = Array.isArray(wf.nodes)
    ? wf.nodes.map((n, i) => ({
      id: String(n.id || `n${i + 1}`),
      type: String(n.type || ""),
      config: n.config && typeof n.config === "object" ? n.config : {},
    }))
    : [];
  const edges = Array.isArray(wf.edges)
    ? wf.edges.map((e) => ({
      from: String(e.from || ""),
      to: String(e.to || ""),
      when: typeof e.when === "undefined" ? null : e.when,
    }))
    : [];
  return {
    workflow_id: String(payload.workflow_id || wf.workflow_id || "custom_v1"),
    nodes,
    edges,
  };
}

function topoSort(nodes, edges) {
  const map = new Map(nodes.map((n) => [n.id, n]));
  const indeg = new Map(nodes.map((n) => [n.id, 0]));
  const out = new Map(nodes.map((n) => [n.id, []]));
  for (const e of edges) {
    if (!map.has(e.from) || !map.has(e.to)) continue;
    indeg.set(e.to, (indeg.get(e.to) || 0) + 1);
    out.get(e.from).push(e.to);
  }
  const q = [];
  for (const [id, d] of indeg.entries()) {
    if (d === 0) q.push(id);
  }
  const result = [];
  while (q.length) {
    const id = q.shift();
    result.push(map.get(id));
    for (const to of out.get(id) || []) {
      indeg.set(to, (indeg.get(to) || 0) - 1);
      if (indeg.get(to) === 0) q.push(to);
    }
  }
  return result;
}

function validateGraph(graph) {
  const errors = [];
  const idSet = new Set();
  for (const n of graph.nodes) {
    if (!n.id) errors.push("node missing id");
    if (idSet.has(n.id)) errors.push(`duplicate node id: ${n.id}`);
    idSet.add(n.id);
    if (!n.type) errors.push(`node ${n.id} missing type`);
  }
  for (const e of graph.edges) {
    if (!idSet.has(e.from)) errors.push(`edge from does not exist: ${e.from}`);
    if (!idSet.has(e.to)) errors.push(`edge to does not exist: ${e.to}`);
    if (typeof e.when !== "undefined" && e.when !== null) {
      const t = typeof e.when;
      if (t !== "object" && t !== "string" && t !== "boolean") {
        errors.push(`edge.when type unsupported: ${e.from}->${e.to}`);
      }
    }
  }
  const ordered = topoSort(graph.nodes, graph.edges);
  if (ordered.length !== graph.nodes.length) errors.push("workflow has cycle; only DAG is supported");
  return { ok: errors.length === 0, errors, ordered };
}

module.exports = {
  defaultWorkflowGraph,
  normalizeWorkflow,
  topoSort,
  validateGraph,
};
