import { WORKFLOW_SCHEMA_VERSION } from "./workflow-contract.js";

export function validateGraph(graph) {
  const errors = [];
  const warnings = [];
  const errorItems = [];
  const warningItems = [];
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges : [];

  if (!String(graph?.workflow_id || "").trim()) {
    const message = "workflow.workflow_id is required";
    errors.push(message);
    errorItems.push({ path: "workflow.workflow_id", code: "required", message });
  }
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
  if (!String(graph?.version || "").trim()) {
    const message = `workflow.version missing; runtime will normalize to ${WORKFLOW_SCHEMA_VERSION}`;
    warnings.push(message);
    warningItems.push({ path: "workflow.version", code: "version_migration_pending", message });
  }
  if (Array.isArray(graph?.nodes) && graph.nodes.length === 0) {
    const message = "workflow.nodes must contain at least one node";
    errors.push(message);
    errorItems.push({ path: "workflow.nodes", code: "array_min_items", message });
  }

  const idSet = new Set();
  const inDegree = new Map();
  const outDegree = new Map();
  for (let index = 0; index < nodes.length; index += 1) {
    const node = nodes[index];
    const id = String(node?.id || "").trim();
    const type = String(node?.type || "").trim();
    if (!id) {
      const message = `workflow.nodes[${index}].id is required`;
      errors.push(message);
      errorItems.push({ path: `workflow.nodes[${index}].id`, code: "required", message });
      continue;
    }
    if (idSet.has(id)) {
      const message = `duplicate node id: ${id}`;
      errors.push(message);
      errorItems.push({ path: `workflow.nodes[${index}].id`, code: "duplicate_node_id", message });
    }
    idSet.add(id);
    if (!type) {
      const message = `workflow.nodes[${index}].type is required`;
      errors.push(message);
      errorItems.push({ path: `workflow.nodes[${index}].type`, code: "required", message });
    }
    if (!inDegree.has(id)) inDegree.set(id, 0);
    if (!outDegree.has(id)) outDegree.set(id, 0);
  }

  const edgeSet = new Set();
  for (let index = 0; index < edges.length; index += 1) {
    const edge = edges[index];
    const from = String(edge?.from || "").trim();
    const to = String(edge?.to || "").trim();
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
    if (from && to) {
      const edgeKey = `${from}=>${to}`;
      if (edgeSet.has(edgeKey)) {
        const message = `duplicate edge: ${from} -> ${to}`;
        warnings.push(message);
        warningItems.push({ path: "workflow.edges", code: "duplicate_edge", message });
      }
      edgeSet.add(edgeKey);
      if (from === to) {
        const message = `self loop edge: ${from}`;
        errors.push(message);
        errorItems.push({ path: "workflow.edges", code: "self_loop", message });
      }
      if (outDegree.has(from)) outDegree.set(from, (outDegree.get(from) || 0) + 1);
      if (inDegree.has(to)) inDegree.set(to, (inDegree.get(to) || 0) + 1);
    }
    if (typeof edge?.when !== "undefined" && edge.when !== null) {
      const type = typeof edge.when;
      if (type !== "boolean" && type !== "string" && type !== "object") {
        const message = `edge.when type unsupported: ${from}->${to}`;
        errors.push(message);
        errorItems.push({ path: `workflow.edges[${index}].when`, code: "edge_invalid_when_type", message });
      }
    }
  }

  if (nodes.length > 0) {
    const starts = nodes.filter((node) => (inDegree.get(String(node?.id || "")) || 0) === 0);
    const ends = nodes.filter((node) => (outDegree.get(String(node?.id || "")) || 0) === 0);
    if (starts.length === 0) {
      const message = "graph has no start node";
      errors.push(message);
      errorItems.push({ path: "workflow.nodes", code: "graph_missing_start", message });
    }
    if (ends.length === 0) {
      const message = "graph has no end node";
      errors.push(message);
      errorItems.push({ path: "workflow.nodes", code: "graph_missing_end", message });
    }
    if (nodes.length > 1) {
      const isolated = nodes.filter((node) => {
        const id = String(node?.id || "");
        return id && (inDegree.get(id) || 0) === 0 && (outDegree.get(id) || 0) === 0;
      });
      isolated.forEach((node) => {
        const message = `isolated node: ${String(node.id)}`;
        warnings.push(message);
        warningItems.push({ path: "workflow.nodes", code: "isolated_node", message });
      });
    }
  }

  const cycle = hasCycle(nodes, edges);
  if (cycle) {
    const message = "workflow has cycle; only DAG is supported";
    errors.push(message);
    errorItems.push({ path: "workflow.edges", code: "graph_cycle", message });
  }
  return { ok: errors.length === 0, errors, warnings, error_items: errorItems, warning_items: warningItems };
}

export function topoSort(nodes, edges) {
  const map = new Map(nodes.map((node) => [node.id, node]));
  const indeg = new Map(nodes.map((node) => [node.id, 0]));
  const out = new Map(nodes.map((node) => [node.id, []]));
  for (const edge of edges) {
    if (!map.has(edge.from) || !map.has(edge.to)) continue;
    indeg.set(edge.to, (indeg.get(edge.to) || 0) + 1);
    out.get(edge.from).push(edge.to);
  }
  const queue = [];
  for (const [id, degree] of indeg.entries()) {
    if (degree === 0) queue.push(id);
  }
  const ordered = [];
  while (queue.length) {
    const id = queue.shift();
    ordered.push(map.get(id));
    for (const to of out.get(id) || []) {
      indeg.set(to, (indeg.get(to) || 0) - 1);
      if (indeg.get(to) === 0) queue.push(to);
    }
  }
  return ordered;
}

function hasCycle(nodes, edges) {
  const ordered = topoSort(nodes, edges);
  return ordered.length !== nodes.length;
}
