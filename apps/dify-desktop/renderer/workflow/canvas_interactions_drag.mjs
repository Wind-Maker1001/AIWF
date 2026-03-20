import {
  snap,
  storePointer,
} from './canvas_interactions_touch.mjs';

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
  const selectedIds = [];
  for (const n of ctx.store.state.graph.nodes) {
    const p = ctx.worldToDisplay(Number(n.x || 0), Number(n.y || 0));
    const nx2 = p.x + nodeW * ctx.zoom;
    const ny2 = p.y + nodeH * ctx.zoom;
    const inter = !(nx2 < x || p.x > x2 || ny2 < y || p.y > y2);
    if (inter) selectedIds.push(n.id);
  }
  ctx.setSelectedIds(selectedIds);
  ctx.marquee = null;
  ctx.render();
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
  if (Number.isFinite(evt.pointerId)) {
    storePointer(ctx, evt);
  }
  ctx.drag = {
    id: nodeId,
    offsetX: pos.x - Number(node.x || 0),
    offsetY: pos.y - Number(node.y || 0),
    selected,
    startMap,
    pointerId: Number.isFinite(evt.pointerId) ? evt.pointerId : null,
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
  let moved = false;
  for (const id of ctx.drag.selected) {
    const s = ctx.drag.startMap.get(id);
    if (!s) continue;
    const nx = snap(s.x + dx, ctx.gridSize, ctx.snapEnabled);
    const ny = snap(s.y + dy, ctx.gridSize, ctx.snapEnabled);
    if (nx === s.x && ny === s.y) continue;
    ctx.store.moveNode(id, nx, ny);
    moved = true;
    maxX = Math.max(maxX, nx);
    maxY = Math.max(maxY, ny);
  }
  if (!moved) return;
  const ox = ctx.offsetX;
  const oy = ctx.offsetY;
  ctx.ensureViewport({ x: maxX, y: maxY });
  if (ox !== ctx.offsetX || oy !== ctx.offsetY) {
    ctx.requestRender(false);
    return;
  }
  ctx.updateNodePositionsFast(ctx.drag.selected);
  ctx.requestEdgeFrame();
}

function onDragEnd(ctx) {
  const moved = ctx.drag ? [...ctx.drag.selected] : [];
  ctx.drag = null;
  ctx.guides.x = null;
  ctx.guides.y = null;
  ctx.invalidateEdgesForNodes(moved);
  ctx.render();
}

export {
  onDragEnd,
  onDragMove,
  onDragStart,
  onMarqueeEnd,
  onMarqueeMove,
  renderGuides,
  renderMarquee,
};
