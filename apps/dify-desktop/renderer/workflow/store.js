import { defaultNodeConfig, defaultWorkflowGraph } from "./defaults.js";
import {
  deepClone,
  nextNodeIdFromNodes,
  normalizeImportedGraph,
  wouldGraphCreateCycle,
} from "./store-support.js";

export function createWorkflowStore() {
  const state = {
    graph: defaultWorkflowGraph(),
    linkFrom: null,
  };

  function reset() {
    state.graph = defaultWorkflowGraph();
    state.linkFrom = null;
  }

  function clear() {
    state.graph = { workflow_id: "custom", name: "空流程", nodes: [], edges: [] };
    state.linkFrom = null;
  }

  function nextNodeId() {
    return nextNodeIdFromNodes(state.graph.nodes);
  }

  function addNode(type, x = 40, y = 40, config) {
    const id = nextNodeId();
    const nodeType = String(type || "ingest_files");
    const cfg =
      config && typeof config === "object"
        ? deepClone(config)
        : defaultNodeConfig(nodeType);
    state.graph.nodes.push({ id, type: nodeType, x: Math.round(x), y: Math.round(y), config: cfg });
    return id;
  }

  function moveNode(id, x, y) {
    const n = state.graph.nodes.find((item) => item.id === id);
    if (!n) return;
    n.x = Math.round(x);
    n.y = Math.round(y);
  }

  function getNode(id) {
    const sid = String(id || "");
    if (!sid) return null;
    return state.graph.nodes.find((item) => item.id === sid) || null;
  }

  function updateNodeConfig(id, config) {
    const node = getNode(id);
    if (!node || !config || typeof config !== "object") return false;
    node.config = deepClone(config);
    return true;
  }

  function removeNode(id) {
    state.graph.nodes = state.graph.nodes.filter((n) => n.id !== id);
    state.graph.edges = state.graph.edges.filter((e) => e.from !== id && e.to !== id);
    if (state.linkFrom === id) state.linkFrom = null;
  }

  function hasEdge(from, to) {
    return state.graph.edges.some((e) => e.from === from && e.to === to);
  }

  function getEdge(from, to) {
    const a = String(from || "");
    const b = String(to || "");
    if (!a || !b) return null;
    return state.graph.edges.find((e) => e.from === a && e.to === b) || null;
  }

  function wouldCreateCycle(from, to) {
    return wouldGraphCreateCycle(state.graph.nodes, state.graph.edges, from, to);
  }

  function unlink(from, to) {
    state.graph.edges = state.graph.edges.filter((e) => !(e.from === from && e.to === to));
  }

  function linkToFrom(from, to, when = null) {
    if (!from || !to) return { ok: false, reason: "empty" };
    if (from === to) return { ok: false, reason: "self" };
    if (hasEdge(from, to)) return { ok: false, reason: "duplicate" };
    if (wouldCreateCycle(from, to)) return { ok: false, reason: "cycle" };
    state.graph.edges.push({ from, to, when: when === undefined ? null : when });
    return { ok: true };
  }

  function updateEdgeWhen(from, to, when) {
    const edge = getEdge(from, to);
    if (!edge) return false;
    edge.when = when === undefined ? null : deepClone(when);
    return true;
  }

  function setWorkflowName(name) {
    state.graph.name = String(name || "").trim() || "自定义流程";
  }

  function exportGraph() {
    return deepClone(state.graph);
  }

  function importGraph(graph) {
    state.graph = normalizeImportedGraph(graph);
    state.linkFrom = null;
  }

  return {
    state,
    reset,
    clear,
    addNode,
    moveNode,
    getNode,
    getEdge,
    updateNodeConfig,
    removeNode,
    hasEdge,
    unlink,
    linkToFrom,
    updateEdgeWhen,
    setWorkflowName,
    exportGraph,
    importGraph,
  };
}
