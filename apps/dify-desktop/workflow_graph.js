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

function resolveWorkflowDefinitionPayload(payload = {}, options = {}) {
  const { fallbackToDefault = false } = options;
  const source = payload && typeof payload === "object" ? payload : {};
  const candidate =
    source.workflow_definition && typeof source.workflow_definition === "object"
      ? source.workflow_definition
      : (source.workflow && typeof source.workflow === "object" ? source.workflow : null);
  if (candidate && typeof candidate === "object") {
    return candidate;
  }
  return fallbackToDefault ? defaultWorkflowGraph() : null;
}

function normalizeWorkflowPayloadShape(payload = {}, workflowDefinition = null, options = {}) {
  const { keepLegacyWorkflowAlias = false } = options;
  const source = payload && typeof payload === "object" ? { ...payload } : {};
  const normalizedWorkflowDefinition =
    workflowDefinition && typeof workflowDefinition === "object"
      ? workflowDefinition
      : resolveWorkflowDefinitionPayload(source, { fallbackToDefault: false });
  if (normalizedWorkflowDefinition && typeof normalizedWorkflowDefinition === "object") {
    source.workflow_definition = normalizedWorkflowDefinition;
    if (keepLegacyWorkflowAlias) {
      source.workflow = normalizedWorkflowDefinition;
    } else {
      delete source.workflow;
    }
  }
  return source;
}

function normalizeWorkflow(payload = {}) {
  const rawWorkflow = resolveWorkflowDefinitionPayload(payload, { fallbackToDefault: true });
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

module.exports = {
  defaultWorkflowGraph,
  normalizeWorkflow,
  normalizeWorkflowPayloadShape,
  resolveWorkflowDefinitionPayload,
  topoSort,
};
