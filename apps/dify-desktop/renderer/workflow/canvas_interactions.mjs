function snap(v, grid, enabled) {
  if (!enabled) return Math.round(v);
  return Math.round(v / grid) * grid;
}

function bindCanvasEvents(ctx) {
  window.addEventListener("mousemove", (evt) => {
    ctx.lastClient = { x: evt.clientX, y: evt.clientY };
    if (ctx.drag) ctx.onDragMove(evt);
    if (ctx.marquee) ctx.onMarqueeMove(evt);
    if (ctx.linking) ctx.requestRender(false);
  });

  window.addEventListener("mouseup", (evt) => {
    if (ctx.drag) ctx.onDragEnd();
    if (ctx.marquee) ctx.onMarqueeEnd(evt);
    if (ctx.linking) ctx.finishLinkByEvent(evt);
  });

  ctx.canvasWrap.addEventListener("scroll", () => {
    if (ctx.linking) ctx.requestRender(false);
    ctx.renderMinimap();
  });

  ctx.canvasSurface.addEventListener("mousedown", (evt) => {
    if (evt.button !== 0) return;
    const target = evt.target;
    if (target.closest && (target.closest(".node") || target.closest(".port") || target.closest(".edge-hit") || target.closest(".minimap-wrap"))) return;
    const s = ctx.clientToSurface(evt.clientX, evt.clientY);
    ctx.marquee = { x0: s.x, y0: s.y, x1: s.x, y1: s.y };
    ctx.clearSelection();
    ctx.requestRender(false);
  });
}

function bindMinimapEvents(ctx) {
  if (!ctx.minimapCanvas) return;
  ctx.minimapCanvas.addEventListener("mousedown", (evt) => {
    const rect = ctx.minimapCanvas.getBoundingClientRect();
    const mx = evt.clientX - rect.left;
    const my = evt.clientY - rect.top;
    const worldW = ctx.surfaceWidth;
    const worldH = ctx.surfaceHeight;
    const worldX = (mx / rect.width) * worldW - ctx.offsetX;
    const worldY = (my / rect.height) * worldH - ctx.offsetY;
    const sx = (worldX + ctx.offsetX) * ctx.zoom - ctx.canvasWrap.clientWidth / 2;
    const sy = (worldY + ctx.offsetY) * ctx.zoom - ctx.canvasWrap.clientHeight / 2;
    ctx.canvasWrap.scrollLeft = Math.max(0, sx);
    ctx.canvasWrap.scrollTop = Math.max(0, sy);
    ctx.render();
  });
}

function renderGuides(ctx, createEl) {
  if (!ctx.guideLayer) return;
  ctx.guideLayer.innerHTML = "";
  if (ctx.guides.x !== null) {
    const v = createEl("div", "guide-line v");
    v.style.left = `${(ctx.guides.x + ctx.offsetX) * ctx.zoom}px`;
    ctx.guideLayer.append(v);
  }
  if (ctx.guides.y !== null) {
    const h = createEl("div", "guide-line h");
    h.style.top = `${(ctx.guides.y + ctx.offsetY) * ctx.zoom}px`;
    ctx.guideLayer.append(h);
  }
}

function renderMarquee(ctx, createEl) {
  if (!ctx.guideLayer || !ctx.marquee) return;
  const x = Math.min(ctx.marquee.x0, ctx.marquee.x1);
  const y = Math.min(ctx.marquee.y0, ctx.marquee.y1);
  const w = Math.abs(ctx.marquee.x1 - ctx.marquee.x0);
  const h = Math.abs(ctx.marquee.y1 - ctx.marquee.y0);
  const box = createEl("div", "marquee-box");
  box.style.left = `${x}px`;
  box.style.top = `${y}px`;
  box.style.width = `${w}px`;
  box.style.height = `${h}px`;
  ctx.guideLayer.append(box);
}

function onMarqueeMove(ctx, evt) {
  if (!ctx.marquee) return;
  const s = ctx.clientToSurface(evt.clientX, evt.clientY);
  ctx.marquee.x1 = s.x;
  ctx.marquee.y1 = s.y;
  ctx.requestRender(false);
}

function onMarqueeEnd(ctx, nodeW, nodeH, evt) {
  if (!ctx.marquee) return;
  if (evt && Number.isFinite(evt.clientX) && Number.isFinite(evt.clientY)) {
    const s = ctx.clientToSurface(evt.clientX, evt.clientY);
    ctx.marquee.x1 = s.x;
    ctx.marquee.y1 = s.y;
  }
  const x = Math.min(ctx.marquee.x0, ctx.marquee.x1);
  const y = Math.min(ctx.marquee.y0, ctx.marquee.y1);
  const w = Math.abs(ctx.marquee.x1 - ctx.marquee.x0);
  const h = Math.abs(ctx.marquee.y1 - ctx.marquee.y0);
  const x2 = x + w;
  const y2 = y + h;
  ctx.clearSelection();
  for (const n of ctx.store.state.graph.nodes) {
    const p = ctx.worldToDisplay(Number(n.x || 0), Number(n.y || 0));
    const nx2 = p.x + nodeW * ctx.zoom;
    const ny2 = p.y + nodeH * ctx.zoom;
    const inter = !(nx2 < x || p.x > x2 || ny2 < y || p.y > y2);
    if (inter) ctx.selectedIds.add(n.id);
  }
  ctx.marquee = null;
  ctx.render();
}

function renderMinimap(ctx, nodeW, nodeH) {
  if (!ctx.minimapCanvas) return;
  const g = ctx.minimapCanvas.getContext("2d");
  if (!g) return;

  const w = ctx.minimapCanvas.width;
  const h = ctx.minimapCanvas.height;
  g.clearRect(0, 0, w, h);
  g.fillStyle = "#eaf1f9";
  g.fillRect(0, 0, w, h);

  const sx = w / ctx.surfaceWidth;
  const sy = h / ctx.surfaceHeight;

  const graph = ctx.store.state.graph;
  const nodeMap = new Map(graph.nodes.map((n) => [n.id, n]));
  g.strokeStyle = "#93afd0";
  g.lineWidth = 1;
  for (const e of graph.edges) {
    const from = nodeMap.get(e.from);
    const to = nodeMap.get(e.to);
    if (!from || !to) continue;
    const ax = from.x + nodeW;
    const ay = from.y + nodeH / 2;
    const bx = to.x;
    const by = to.y + nodeH / 2;
    g.beginPath();
    g.moveTo((ax + ctx.offsetX) * sx, (ay + ctx.offsetY) * sy);
    g.lineTo((bx + ctx.offsetX) * sx, (by + ctx.offsetY) * sy);
    g.stroke();
  }

  g.fillStyle = "#4d7eb0";
  for (const n of graph.nodes) {
    const x = (n.x + ctx.offsetX) * sx;
    const y = (n.y + ctx.offsetY) * sy;
    g.fillRect(x, y, Math.max(3, nodeW * sx), Math.max(3, nodeH * sy));
  }

  const vx = ctx.canvasWrap.scrollLeft / ctx.zoom;
  const vy = ctx.canvasWrap.scrollTop / ctx.zoom;
  const vw = ctx.canvasWrap.clientWidth / ctx.zoom;
  const vh = ctx.canvasWrap.clientHeight / ctx.zoom;
  g.strokeStyle = "#f59e0b";
  g.lineWidth = 1.2;
  g.strokeRect(vx * sx, vy * sy, vw * sx, vh * sy);
}

function onDragStart(ctx, evt, nodeId) {
  evt.preventDefault();
  evt.stopPropagation();
  const node = ctx.store.state.graph.nodes.find((n) => n.id === nodeId);
  if (!node) return;
  if (!ctx.isSelected(nodeId)) ctx.selectOne(nodeId);
  const pos = ctx.clientToWorld(evt.clientX, evt.clientY);
  const selected = Array.from(ctx.selectedIds);
  const startMap = new Map();
  for (const id of selected) {
    const n = ctx.store.state.graph.nodes.find((it) => it.id === id);
    if (n) startMap.set(id, { x: Number(n.x || 0), y: Number(n.y || 0) });
  }
  ctx.drag = {
    id: nodeId,
    offsetX: pos.x - Number(node.x || 0),
    offsetY: pos.y - Number(node.y || 0),
    selected,
    startMap,
  };
}

function onDragMove(ctx, evt) {
  if (!ctx.drag) return;
  const pos = ctx.clientToWorld(evt.clientX, evt.clientY);
  const x = pos.x - ctx.drag.offsetX;
  const y = pos.y - ctx.drag.offsetY;

  const aligned = ctx.applyAlignment(x, y, ctx.drag.id);
  const anchorX = snap(aligned.x, ctx.gridSize, ctx.snapEnabled);
  const anchorY = snap(aligned.y, ctx.gridSize, ctx.snapEnabled);
  const anchorStart = ctx.drag.startMap.get(ctx.drag.id) || { x: 0, y: 0 };
  const dx = anchorX - anchorStart.x;
  const dy = anchorY - anchorStart.y;

  let maxX = anchorX;
  let maxY = anchorY;
  for (const id of ctx.drag.selected) {
    const s = ctx.drag.startMap.get(id);
    if (!s) continue;
    const nx = snap(s.x + dx, ctx.gridSize, ctx.snapEnabled);
    const ny = snap(s.y + dy, ctx.gridSize, ctx.snapEnabled);
    ctx.store.moveNode(id, nx, ny);
    maxX = Math.max(maxX, nx);
    maxY = Math.max(maxY, ny);
  }
  const ox = ctx.offsetX;
  const oy = ctx.offsetY;
  ctx.ensureViewport({ x: maxX, y: maxY });
  if (ox !== ctx.offsetX || oy !== ctx.offsetY) {
    ctx.requestRender(false);
    return;
  }
  ctx.updateNodePositionsFast(ctx.drag.selected);
  ctx.renderEdges();
  ctx.renderGuides();
}

function onDragEnd(ctx) {
  const moved = ctx.drag ? [...ctx.drag.selected] : [];
  ctx.drag = null;
  ctx.guides.x = null;
  ctx.guides.y = null;
  ctx.invalidateEdgesForNodes(moved);
  ctx.render();
}

function onLinkStart(ctx, evt, fromId) {
  evt.preventDefault();
  evt.stopPropagation();
  ctx.lastClient = { x: evt.clientX, y: evt.clientY };
  ctx.linking = { from: fromId };
  markLinkTargets(ctx);
  ctx.requestRender(false);
}

function linkErrorMessage(reason) {
  if (reason === "self") return "不能连接到自身节点";
  if (reason === "duplicate") return "连线已存在";
  if (reason === "cycle") return "不允许形成环路（仅支持 DAG）";
  return "连线失败";
}

function finishLinkByEvent(ctx, evt) {
  if (!ctx.linking) return;

  const direct = evt.target && evt.target.closest ? evt.target.closest(".port.in") : null;
  let toId = direct ? String(direct.dataset.nodeId || "") : "";

  if (!toId) {
    const hit = document.elementFromPoint(evt.clientX, evt.clientY);
    const hitPort = hit && hit.closest ? hit.closest(".port.in") : null;
    toId = hitPort ? String(hitPort.dataset.nodeId || "") : "";
  }

  if (toId && toId !== ctx.linking.from) {
    if (ctx.store.hasEdge(ctx.linking.from, toId)) {
      ctx.store.unlink(ctx.linking.from, toId);
      ctx.onChange();
      ctx.onWarn(`已取消连线: ${ctx.linking.from} -> ${toId}`);
    } else {
      const res = ctx.store.linkToFrom(ctx.linking.from, toId);
      if (res && res.ok) ctx.onChange();
      else ctx.onWarn(linkErrorMessage(res?.reason));
    }
  }

  ctx.linking = null;
  clearLinkTargets(ctx);
  ctx.requestRender(false);
}

function markLinkTargets(ctx) {
  for (const [id, port] of ctx.inputPortByNodeId.entries()) {
    if (!ctx.linking || id === ctx.linking.from) continue;
    port.classList.add("target");
  }
}

function clearLinkTargets(ctx) {
  for (const port of ctx.inputPortByNodeId.values()) {
    port.classList.remove("target");
  }
}

export {
  bindCanvasEvents,
  bindMinimapEvents,
  renderGuides,
  renderMarquee,
  onMarqueeMove,
  onMarqueeEnd,
  renderMinimap,
  onDragStart,
  onDragMove,
  onDragEnd,
  onLinkStart,
  linkErrorMessage,
  finishLinkByEvent,
  markLinkTargets,
  clearLinkTargets,
};
