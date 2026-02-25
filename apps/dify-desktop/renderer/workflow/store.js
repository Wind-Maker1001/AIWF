import { defaultNodeConfig, defaultWorkflowGraph } from "./defaults.js";

function deepClone(v) {
  return JSON.parse(JSON.stringify(v));
}

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
    const ids = state.graph.nodes
      .map((n) => Number(String(n.id).replace(/^n/, "")))
      .filter((n) => Number.isFinite(n));
    const next = (ids.length ? Math.max(...ids) : 0) + 1;
    return `n${next}`;
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
    const out = new Map();
    for (const n of state.graph.nodes) out.set(n.id, []);
    for (const e of state.graph.edges) {
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
    const g = graph && typeof graph === "object" ? graph : {};
    const nodes = Array.isArray(g.nodes)
      ? g.nodes.map((n, i) => ({
          id: String(n?.id || `n${i + 1}`),
          type: String(n?.type || "ingest_files"),
          x: Number.isFinite(Number(n?.x)) ? Math.round(Number(n.x)) : 40 + i * 30,
          y: Number.isFinite(Number(n?.y)) ? Math.round(Number(n.y)) : 40 + i * 20,
          config:
            n?.config && typeof n.config === "object"
              ? deepClone(n.config)
              : defaultNodeConfig(String(n?.type || "ingest_files")),
        }))
      : [];
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

    state.graph = {
      workflow_id: String(g.workflow_id || "custom_v1"),
      name: String(g.name || "自定义流程"),
      nodes,
      edges,
    };
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
