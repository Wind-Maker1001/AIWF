const {
  WORKFLOW_SCHEMA_VERSION,
  normalizeWorkflowContract,
  validateWorkflowTopLevel,
} = require("./workflow_contract");
const {
  findUnknownWorkflowNodeTypes,
} = require("./workflow_node_catalog_contract");

function defaultWorkflowGraph() {
  return {
    workflow_id: "minimal_v1",
    version: WORKFLOW_SCHEMA_VERSION,
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
  const rawWorkflow =
    payload.workflow && typeof payload.workflow === "object"
      ? payload.workflow
      : defaultWorkflowGraph();
  const contract = normalizeWorkflowContract(
    {
      ...rawWorkflow,
      workflow_id: String(payload.workflow_id || rawWorkflow.workflow_id || ""),
      version: String(payload.workflow_version || rawWorkflow.version || ""),
    },
    { allowVersionMigration: true },
  );

  const base = contract.graph || {
    ...rawWorkflow,
    workflow_id: String(payload.workflow_id || rawWorkflow.workflow_id || "custom_v1"),
    version: String(payload.workflow_version || rawWorkflow.version || WORKFLOW_SCHEMA_VERSION),
    nodes: Array.isArray(rawWorkflow.nodes) ? rawWorkflow.nodes : [],
    edges: Array.isArray(rawWorkflow.edges) ? rawWorkflow.edges : [],
  };

  const nodes = Array.isArray(base.nodes)
    ? base.nodes.map((n, i) => ({
      id: String(n.id || `n${i + 1}`),
      type: String(n.type || ""),
      config: n.config && typeof n.config === "object" ? n.config : {},
    }))
    : [];
  const edges = Array.isArray(base.edges)
    ? base.edges.map((e) => ({
      from: String(e.from || ""),
      to: String(e.to || ""),
      when: typeof e.when === "undefined" ? null : e.when,
    }))
    : [];

  return {
    graph: {
      ...base,
      workflow_id: String(base.workflow_id || "custom_v1"),
      version: String(base.version || WORKFLOW_SCHEMA_VERSION),
      nodes,
      edges,
    },
    contract: {
      ok: contract.ok,
      migrated: contract.migrated,
      notes: Array.isArray(contract.notes) ? [...contract.notes] : [],
      errors: Array.isArray(contract.errors) ? [...contract.errors] : [],
    },
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
  const topLevel = validateWorkflowTopLevel(graph, { requireNonEmptyNodes: true });
  const errors = [...topLevel.errors];
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges : [];
  const idSet = new Set();
  const unknownNodeTypes = findUnknownWorkflowNodeTypes(graph);
  if (unknownNodeTypes.length > 0) {
    errors.push(`workflow contains unregistered node types: ${unknownNodeTypes.join(", ")}`);
  }

  for (const n of nodes) {
    if (!n.id) errors.push("node missing id");
    if (idSet.has(n.id)) errors.push(`duplicate node id: ${n.id}`);
    idSet.add(n.id);
    if (!n.type) errors.push(`node ${n.id} missing type`);
  }
  for (const e of edges) {
    if (!idSet.has(e.from)) errors.push(`edge from does not exist: ${e.from}`);
    if (!idSet.has(e.to)) errors.push(`edge to does not exist: ${e.to}`);
    if (typeof e.when !== "undefined" && e.when !== null) {
      const t = typeof e.when;
      if (t !== "object" && t !== "string" && t !== "boolean") {
        errors.push(`edge.when type unsupported: ${e.from}->${e.to}`);
      }
    }
  }
  const ordered = topoSort(nodes, edges);
  if (ordered.length !== nodes.length) errors.push("workflow has cycle; only DAG is supported");
  return { ok: errors.length === 0, errors, ordered };
}

module.exports = {
  defaultWorkflowGraph,
  normalizeWorkflow,
  topoSort,
  validateGraph,
};
