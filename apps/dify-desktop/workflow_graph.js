const {
  WORKFLOW_SCHEMA_VERSION,
  WORKFLOW_VERSION_MIGRATION_NOTE,
} = require("./workflow_contract");

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
  const notes = [];
  const base = {
    ...rawWorkflow,
    workflow_id: String(payload.workflow_id || rawWorkflow.workflow_id || "custom_v1"),
    version: String(payload.workflow_version || rawWorkflow.version || ""),
    nodes: Array.isArray(rawWorkflow.nodes) ? rawWorkflow.nodes : [],
    edges: Array.isArray(rawWorkflow.edges) ? rawWorkflow.edges : [],
  };
  if (!String(base.version || "").trim()) {
    base.version = WORKFLOW_SCHEMA_VERSION;
    notes.push(WORKFLOW_VERSION_MIGRATION_NOTE);
  }

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
      ok: true,
      migrated: notes.length > 0,
      notes,
      errors: [],
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
  const errors = [];
  const errorItems = [];
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges : [];
  const idSet = new Set();

  if (!Array.isArray(graph?.nodes)) {
    const message = "workflow.nodes must be an array";
    errors.push(message);
    errorItems.push({ path: "workflow.nodes", code: "type_array", message });
  }
  if (!Array.isArray(graph?.edges)) {
    const message = "workflow.edges must be an array";
    errors.push(message);
    errorItems.push({ path: "workflow.edges", code: "type_array", message });
  }
  if (Array.isArray(graph?.nodes) && graph.nodes.length === 0) {
    const message = "workflow.nodes must contain at least one node";
    errors.push(message);
    errorItems.push({ path: "workflow.nodes", code: "array_min_items", message });
  }

  for (let index = 0; index < nodes.length; index += 1) {
    const n = nodes[index];
    const nodeId = String(n?.id || "").trim();
    const nodeType = String(n?.type || "").trim();
    if (!nodeId) {
      const message = `workflow.nodes[${index}].id is required`;
      errors.push(message);
      errorItems.push({ path: `workflow.nodes[${index}].id`, code: "required", message });
      continue;
    }
    if (idSet.has(nodeId)) {
      const message = `duplicate node id: ${nodeId}`;
      errors.push(message);
      errorItems.push({ path: `workflow.nodes[${index}].id`, code: "duplicate_node_id", message });
    }
    idSet.add(nodeId);
    if (!nodeType) {
      const message = `workflow.nodes[${index}].type is required`;
      errors.push(message);
      errorItems.push({ path: `workflow.nodes[${index}].type`, code: "required", message });
    }
  }
  for (let index = 0; index < edges.length; index += 1) {
    const e = edges[index];
    const from = String(e?.from || "").trim();
    const to = String(e?.to || "").trim();
    if (!from) {
      const message = `workflow.edges[${index}].from is required`;
      errors.push(message);
      errorItems.push({ path: `workflow.edges[${index}].from`, code: "required", message });
    } else if (!idSet.has(from)) {
      const message = `edge from does not exist: ${from}`;
      errors.push(message);
      errorItems.push({ path: `workflow.edges[${index}].from`, code: "edge_missing_from_node", message });
    }
    if (!to) {
      const message = `workflow.edges[${index}].to is required`;
      errors.push(message);
      errorItems.push({ path: `workflow.edges[${index}].to`, code: "required", message });
    } else if (!idSet.has(to)) {
      const message = `edge to does not exist: ${to}`;
      errors.push(message);
      errorItems.push({ path: `workflow.edges[${index}].to`, code: "edge_missing_to_node", message });
    }
    if (typeof e.when !== "undefined" && e.when !== null) {
      const t = typeof e.when;
      if (t !== "object" && t !== "string" && t !== "boolean") {
        const message = `edge.when type unsupported: ${from}->${to}`;
        errors.push(message);
        errorItems.push({ path: `workflow.edges[${index}].when`, code: "edge_invalid_when_type", message });
      }
    }
  }
  const ordered = topoSort(nodes, edges);
  if (ordered.length !== nodes.length) {
    const message = "workflow has cycle; only DAG is supported";
    errors.push(message);
    errorItems.push({ path: "workflow.edges", code: "graph_cycle", message });
  }
  return { ok: errors.length === 0, errors, error_items: errorItems, ordered };
}

module.exports = {
  defaultWorkflowGraph,
  normalizeWorkflow,
  topoSort,
  validateGraph,
};
