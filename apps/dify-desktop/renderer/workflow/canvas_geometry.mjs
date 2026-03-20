function getVisibleDisplayRect(canvas, extra = 0) {
  const margin = Math.max(0, Number(extra || 0));
  return {
    x: canvas.canvasWrap.scrollLeft - margin,
    y: canvas.canvasWrap.scrollTop - margin,
    w: canvas.canvasWrap.clientWidth + margin * 2,
    h: canvas.canvasWrap.clientHeight + margin * 2,
  };
}

function getGraphBounds(canvas, nodeWidth, nodeHeight) {
  const nodes = Array.isArray(canvas.store?.state?.graph?.nodes) ? canvas.store.state.graph.nodes : [];
  if (!nodes.length) return null;
  let minLeft = Number.POSITIVE_INFINITY;
  let minTop = Number.POSITIVE_INFINITY;
  let maxRight = Number.NEGATIVE_INFINITY;
  let maxBottom = Number.NEGATIVE_INFINITY;
  nodes.forEach((node) => {
    const x = Number(node?.x || 0);
    const y = Number(node?.y || 0);
    minLeft = Math.min(minLeft, x);
    minTop = Math.min(minTop, y);
    maxRight = Math.max(maxRight, x + nodeWidth);
    maxBottom = Math.max(maxBottom, y + nodeHeight);
  });
  if (!Number.isFinite(minLeft) || !Number.isFinite(minTop) || !Number.isFinite(maxRight) || !Number.isFinite(maxBottom)) {
    return null;
  }
  return { minLeft, minTop, maxRight, maxBottom };
}

function portCenter(canvas, portEl) {
  if (!portEl) return null;
  const wrapRect = canvas.canvasWrap.getBoundingClientRect();
  const rect = portEl.getBoundingClientRect();
  return {
    x: rect.left - wrapRect.left + canvas.canvasWrap.scrollLeft + rect.width / 2,
    y: rect.top - wrapRect.top + canvas.canvasWrap.scrollTop + rect.height / 2,
  };
}

function edgePoints(canvas, fromId, toId) {
  const fromPorts = canvas.portByNodeId.get(fromId);
  const toPorts = canvas.portByNodeId.get(toId);
  const a = portCenter(canvas, fromPorts?.outPort);
  const b = portCenter(canvas, toPorts?.inPort);
  if (!a || !b) return null;
  return { a, b };
}

function edgeObstacles(canvas, ignoreIds = [], nodeWidth, nodeHeight) {
  const ignore = new Set(ignoreIds);
  const output = [];
  for (const node of canvas.store.state.graph.nodes) {
    if (ignore.has(node.id)) continue;
    const p = canvas.worldToDisplay(Number(node.x || 0), Number(node.y || 0));
    output.push({ x: p.x, y: p.y, w: nodeWidth * canvas.zoom, h: nodeHeight * canvas.zoom });
  }
  return output;
}

function edgeObstacleCandidates(canvas, ignoreIds = [], a, b, nodeWidth, nodeHeight, rectIntersects) {
  const ignore = new Set(ignoreIds);
  const output = [];
  const deps = new Set();
  const margin = 220 * canvas.zoom;
  const x1 = Math.min(a.x, b.x) - margin;
  const y1 = Math.min(a.y, b.y) - margin;
  const x2 = Math.max(a.x, b.x) + margin;
  const y2 = Math.max(a.y, b.y) + margin;
  const corridor = { x: x1, y: y1, w: x2 - x1, h: y2 - y1 };
  for (const node of canvas.store.state.graph.nodes) {
    if (ignore.has(node.id)) continue;
    const p = canvas.worldToDisplay(Number(node.x || 0), Number(node.y || 0));
    const box = { x: p.x, y: p.y, w: nodeWidth * canvas.zoom, h: nodeHeight * canvas.zoom };
    if (!rectIntersects(corridor, box)) continue;
    output.push(box);
    deps.add(node.id);
  }
  return { obstacles: output, deps };
}

function buildGraphShapeSig(canvas, graph) {
  const nodes = graph.nodes
    .map((node) => String(node.id))
    .sort()
    .join("|");
  const edges = graph.edges
    .map((edge) => `${edge.from}->${edge.to}`)
    .sort()
    .join("|");
  return `${canvas.zoom.toFixed(3)}|${nodes}|${edges}`;
}

function invalidateEdgesForNodes(canvas, nodeIds = []) {
  const dirty = new Set((nodeIds || []).map((item) => String(item)));
  if (!dirty.size || !canvas.routeCache.size) return;
  for (const [edgeId, entry] of canvas.routeCache.entries()) {
    const deps = Array.isArray(entry?.deps) ? entry.deps : [];
    if (
      dirty.has(String(entry?.from || "")) ||
      dirty.has(String(entry?.to || "")) ||
      deps.some((id) => dirty.has(String(id)))
    ) {
      canvas.routeCache.delete(edgeId);
    }
  }
}

export {
  buildGraphShapeSig,
  edgeObstacleCandidates,
  edgeObstacles,
  edgePoints,
  getGraphBounds,
  getVisibleDisplayRect,
  invalidateEdgesForNodes,
  portCenter,
};
