import {
  NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY,
  WORKFLOW_CONTRACT_AUTHORITY,
  buildValidationErrorItems,
  validateWorkflowTopLevel,
} from "./workflow-contract.js";
import { findUnknownWorkflowNodeTypes } from "./node-catalog-contract.js";

export function validateGraph(graph) {
  const errors = [];
  const warnings = [];
  const errorItems = [];
  const warningItems = [];
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges : [];
  const contract = validateWorkflowTopLevel(graph, { requireNonEmptyNodes: true });
  errors.push(...contract.errors);
  errorItems.push(
    ...buildValidationErrorItems(contract.errors).map((item) => ({
      ...item,
      graph_contract: WORKFLOW_CONTRACT_AUTHORITY,
      error_contract: NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY,
    })),
  );
  const unknownNodeTypes = findUnknownWorkflowNodeTypes(graph);
  if (unknownNodeTypes.length > 0) {
    const message = `workflow contains unregistered node types: ${unknownNodeTypes.join(", ")}`;
    errors.push(message);
    errorItems.push({
      path: "workflow.nodes",
      code: "unknown_node_type",
      message,
      contract_boundary: "node_catalog_truth",
    });
  }

  const idSet = new Set();
  const inDegree = new Map();
  const outDegree = new Map();
  for (const n of nodes) {
    const id = String(n?.id || "").trim();
    if (!id) {
      const message = "存在缺少 id 的节点";
      errors.push(message);
      errorItems.push({ path: "workflow.nodes", code: "node_missing_id", message });
      continue;
    }
    if (idSet.has(id)) {
      const message = `节点ID重复: ${id}`;
      errors.push(message);
      errorItems.push({ path: "workflow.nodes", code: "duplicate_node_id", message });
    }
    idSet.add(id);
    if (!inDegree.has(id)) inDegree.set(id, 0);
    if (!outDegree.has(id)) outDegree.set(id, 0);
  }

  const edgeSet = new Set();
  for (const e of edges) {
    const from = String(e?.from || "").trim();
    const to = String(e?.to || "").trim();
    if (!idSet.has(from)) {
      const message = `连线 from 不存在: ${from || "(empty)"}`;
      errors.push(message);
      errorItems.push({ path: "workflow.edges", code: "edge_missing_from_node", message });
    }
    if (!idSet.has(to)) {
      const message = `连线 to 不存在: ${to || "(empty)"}`;
      errors.push(message);
      errorItems.push({ path: "workflow.edges", code: "edge_missing_to_node", message });
    }
    if (from && to) {
      const edgeKey = `${from}=>${to}`;
      if (edgeSet.has(edgeKey)) {
        const message = `存在重复连线: ${from} -> ${to}`;
        warnings.push(message);
        warningItems.push({ path: "workflow.edges", code: "duplicate_edge", message });
      }
      edgeSet.add(edgeKey);
      if (from === to) {
        const message = `存在自环连线: ${from}`;
        errors.push(message);
        errorItems.push({ path: "workflow.edges", code: "self_loop", message });
      }
      if (outDegree.has(from)) outDegree.set(from, (outDegree.get(from) || 0) + 1);
      if (inDegree.has(to)) inDegree.set(to, (inDegree.get(to) || 0) + 1);
    }
    if (typeof e?.when !== "undefined" && e.when !== null) {
      const t = typeof e.when;
      if (t !== "boolean" && t !== "string" && t !== "object") {
        const message = `连线 when 类型不支持: ${from}->${to}`;
        errors.push(message);
        errorItems.push({ path: "workflow.edges", code: "edge_invalid_when_type", message });
      }
    }
  }

  if (nodes.length > 0) {
    const starts = nodes.filter((n) => (inDegree.get(String(n?.id || "")) || 0) === 0);
    const ends = nodes.filter((n) => (outDegree.get(String(n?.id || "")) || 0) === 0);
    if (starts.length === 0) {
      const message = "流程没有起点节点";
      errors.push(message);
      errorItems.push({ path: "workflow.nodes", code: "graph_missing_start", message });
    }
    if (ends.length === 0) {
      const message = "流程没有终点节点";
      errors.push(message);
      errorItems.push({ path: "workflow.nodes", code: "graph_missing_end", message });
    }
    if (nodes.length > 1) {
      const isolated = nodes.filter((n) => {
        const id = String(n?.id || "");
        return id && (inDegree.get(id) || 0) === 0 && (outDegree.get(id) || 0) === 0;
      });
      isolated.forEach((n) => {
        const message = `存在孤立节点: ${String(n.id)}`;
        warnings.push(message);
        warningItems.push({ path: "workflow.nodes", code: "isolated_node", message });
      });
    }
  }

  const cycle = hasCycle(nodes, edges);
  if (cycle) {
    const message = "流程存在环，当前仅支持DAG";
    errors.push(message);
    errorItems.push({ path: "workflow.edges", code: "graph_cycle", message });
  }
  return { ok: errors.length === 0, errors, warnings, error_items: errorItems, warning_items: warningItems };
}

export function topoSort(nodes, edges) {
  const map = new Map(nodes.map((n) => [n.id, n]));
  const indeg = new Map(nodes.map((n) => [n.id, 0]));
  const out = new Map(nodes.map((n) => [n.id, []]));
  for (const e of edges) {
    if (!map.has(e.from) || !map.has(e.to)) continue;
    indeg.set(e.to, (indeg.get(e.to) || 0) + 1);
    out.get(e.from).push(e.to);
  }
  const q = [];
  for (const [id, d] of indeg.entries()) if (d === 0) q.push(id);
  const ordered = [];
  while (q.length) {
    const id = q.shift();
    ordered.push(map.get(id));
    for (const to of out.get(id) || []) {
      indeg.set(to, (indeg.get(to) || 0) - 1);
      if (indeg.get(to) === 0) q.push(to);
    }
  }
  return ordered;
}

function hasCycle(nodes, edges) {
  const ordered = topoSort(nodes, edges);
  return ordered.length !== nodes.length;
}
