import { defaultNodeConfig } from "./defaults.js";
import { WORKFLOW_SCHEMA_VERSION } from "./workflow-contract.js";

function deepClone(v) {
  return JSON.parse(JSON.stringify(v));
}

function nextNodeIdFromNodes(nodes = []) {
  const ids = (Array.isArray(nodes) ? nodes : [])
    .map((n) => Number(String(n?.id || "").replace(/^n/, "")))
    .filter((n) => Number.isFinite(n));
  const next = (ids.length ? Math.max(...ids) : 0) + 1;
  return `n${next}`;
}

function normalizeStoreNode(node, index = 0) {
  return {
    id: String(node?.id || `n${index + 1}`),
    type: String(node?.type || "ingest_files"),
    x: Number.isFinite(Number(node?.x)) ? Math.round(Number(node.x)) : 40 + index * 30,
    y: Number.isFinite(Number(node?.y)) ? Math.round(Number(node.y)) : 40 + index * 20,
    config:
      node?.config && typeof node.config === "object"
        ? deepClone(node.config)
        : defaultNodeConfig(String(node?.type || "ingest_files")),
  };
}

function normalizeImportedGraph(graph) {
  return normalizeImportedGraphWithContract(graph).graph;
}

function normalizeImportedGraphWithContract(graph) {
  const source = graph && typeof graph === "object" ? deepClone(graph) : {};
  const notes = [];
  const migrated = !String(source.version || "").trim();
  if (migrated) {
    notes.push(`workflow.version migrated to ${WORKFLOW_SCHEMA_VERSION}`);
  }
  const g = {
    ...source,
    workflow_id: String(source.workflow_id || "custom_v1"),
    version: String(source.version || WORKFLOW_SCHEMA_VERSION),
    name: String(source.name || "Custom Workflow"),
    nodes: Array.isArray(source.nodes) ? source.nodes : [],
    edges: Array.isArray(source.edges) ? source.edges : [],
  };
  const nodes = Array.isArray(g.nodes) ? g.nodes.map(normalizeStoreNode) : [];
  const idSet = new Set(nodes.map((n) => n.id));
  const edges = Array.isArray(g.edges)
    ? g.edges
        .map((e) => ({
          from: String(e?.from || ""),
          to: String(e?.to || ""),
          when: typeof e?.when === "undefined" ? null : deepClone(e.when),
        }))
        .filter((e) => e.from && e.to && idSet.has(e.from) && idSet.has(e.to) && e.from !== e.to)
    : [];

  return {
    graph: {
      ...g,
      nodes,
      edges,
    },
    contract: {
      migrated,
      notes,
      errors: [],
    },
  };
}

function wouldGraphCreateCycle(nodes = [], edges = [], from, to) {
  const out = new Map();
  for (const n of Array.isArray(nodes) ? nodes : []) out.set(n.id, []);
  for (const e of Array.isArray(edges) ? edges : []) {
    if (!out.has(e.from)) out.set(e.from, []);
    out.get(e.from).push(e.to);
  }
  if (!out.has(from)) out.set(from, []);
  out.get(from).push(to);
  const stack = [to];
  const seen = new Set();
  while (stack.length) {
    const cur = stack.pop();
    if (cur === from) return true;
    if (seen.has(cur)) continue;
    seen.add(cur);
    const next = out.get(cur) || [];
    for (const n of next) stack.push(n);
  }
  return false;
}

export {
  deepClone,
  nextNodeIdFromNodes,
  normalizeImportedGraph,
  normalizeImportedGraphWithContract,
  normalizeStoreNode,
  wouldGraphCreateCycle,
};
