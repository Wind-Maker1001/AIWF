export function validateGraph(graph) {
  const errors = [];
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges : [];
  if (nodes.length === 0) errors.push("流程没有节点");
  const idSet = new Set();
  for (const n of nodes) {
    if (!n?.id) errors.push("存在缺少 id 的节点");
    if (idSet.has(n.id)) errors.push(`节点ID重复: ${n.id}`);
    idSet.add(n.id);
  }
  for (const e of edges) {
    if (!idSet.has(e.from)) errors.push(`连线 from 不存在: ${e.from}`);
    if (!idSet.has(e.to)) errors.push(`连线 to 不存在: ${e.to}`);
  }
  const cycle = hasCycle(nodes, edges);
  if (cycle) errors.push("流程存在环，当前仅支持 DAG");
  return { ok: errors.length === 0, errors };
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

