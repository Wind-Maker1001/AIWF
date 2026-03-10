function snap(v, grid, enabled) {
  if (!enabled) return Math.round(v);
  return Math.round(v / grid) * grid;
}

function isTouchLike(evt) {
  const pointerType = String(evt?.pointerType || "");
  return pointerType === "touch" || pointerType === "pen";
}

function isPrimaryPointer(evt) {
  if (isTouchLike(evt)) return evt.isPrimary !== false;
  return evt.button === 0;
}

function storePointer(ctx, evt) {
  if (!Number.isFinite(evt?.pointerId)) return;
  ctx.activePointers.set(evt.pointerId, {
    pointerId: evt.pointerId,
    clientX: Number(evt.clientX || 0),
    clientY: Number(evt.clientY || 0),
    pointerType: String(evt.pointerType || "mouse"),
  });
}

function dropPointer(ctx, evt) {
  if (!Number.isFinite(evt?.pointerId)) return;
  ctx.activePointers.delete(evt.pointerId);
}

function touchPointers(ctx) {
  return Array.from(ctx.activePointers.values()).filter((p) => p.pointerType === "touch" || p.pointerType === "pen");
}

function distance(a, b) {
  const dx = Number(a?.clientX || 0) - Number(b?.clientX || 0);
  const dy = Number(a?.clientY || 0) - Number(b?.clientY || 0);
  return Math.hypot(dx, dy);
}

function midpoint(a, b) {
  return {
    clientX: (Number(a?.clientX || 0) + Number(b?.clientX || 0)) / 2,
    clientY: (Number(a?.clientY || 0) + Number(b?.clientY || 0)) / 2,
  };
}

function beginTouchPan(ctx, pointer) {
  if (!pointer) return;
  ctx.touchPan = {
    pointerId: pointer.pointerId,
    startClientX: pointer.clientX,
    startClientY: pointer.clientY,
    startScrollLeft: ctx.canvasWrap.scrollLeft,
    startScrollTop: ctx.canvasWrap.scrollTop,
    moved: false,
  };
}

function beginTouchPinch(ctx) {
  const points = touchPointers(ctx);
  if (points.length < 2) return;
  const [a, b] = points;
  const startDistance = distance(a, b);
  if (startDistance < 8) return;
  ctx.touchPan = null;
  ctx.touchPinch = {
    pointerIds: [a.pointerId, b.pointerId],
    startDistance,
    startZoom: ctx.zoom,
    startMid: midpoint(a, b),
    startScrollLeft: ctx.canvasWrap.scrollLeft,
    startScrollTop: ctx.canvasWrap.scrollTop,
  };
}

function maybeRefreshTouchGesture(ctx) {
  if (ctx.drag || ctx.linking || ctx.marquee) return;
  const points = touchPointers(ctx);
  if (points.length >= 2) {
    const ids = points.slice(0, 2).map((p) => p.pointerId).sort((a, b) => a - b).join("|");
    const current = Array.isArray(ctx.touchPinch?.pointerIds)
      ? ctx.touchPinch.pointerIds.slice().sort((a, b) => a - b).join("|")
      : "";
    if (!ctx.touchPinch || ids !== current) beginTouchPinch(ctx);
    return;
  }
  ctx.touchPinch = null;
  if (points.length === 1 && !ctx.touchPan) beginTouchPan(ctx, points[0]);
}

function updateTouchPan(ctx, evt) {
  if (!ctx.touchPan || ctx.touchPan.pointerId !== evt.pointerId) return false;
  const dx = evt.clientX - ctx.touchPan.startClientX;
  const dy = evt.clientY - ctx.touchPan.startClientY;
  if (Math.abs(dx) > 3 || Math.abs(dy) > 3) ctx.touchPan.moved = true;
  ctx.canvasWrap.scrollLeft = Math.max(0, ctx.touchPan.startScrollLeft - dx);
  ctx.canvasWrap.scrollTop = Math.max(0, ctx.touchPan.startScrollTop - dy);
  ctx.requestMinimap();
  ctx.requestEdgeFrame();
  return true;
}

function updateTouchPinch(ctx) {
  if (!ctx.touchPinch || !Array.isArray(ctx.touchPinch.pointerIds) || ctx.touchPinch.pointerIds.length < 2) return false;
  const a = ctx.activePointers.get(ctx.touchPinch.pointerIds[0]);
  const b = ctx.activePointers.get(ctx.touchPinch.pointerIds[1]);
  if (!a || !b) return false;
  const nextDistance = distance(a, b);
  if (nextDistance < 8) return true;
  const mid = midpoint(a, b);
  const nextZoom = ctx.touchPinch.startZoom * (nextDistance / ctx.touchPinch.startDistance);
  ctx.setZoom(nextZoom, mid);
  const dx = mid.clientX - ctx.touchPinch.startMid.clientX;
  const dy = mid.clientY - ctx.touchPinch.startMid.clientY;
  ctx.canvasWrap.scrollLeft = Math.max(0, ctx.touchPinch.startScrollLeft - dx);
  ctx.canvasWrap.scrollTop = Math.max(0, ctx.touchPinch.startScrollTop - dy);
  ctx.requestMinimap();
  return true;
}

function jumpMinimap(ctx, clientX, clientY) {
  const rect = ctx.minimapCanvas.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0) return;
  const mx = clientX - rect.left;
  const my = clientY - rect.top;
  const worldW = ctx.surfaceWidth;
  const worldH = ctx.surfaceHeight;
  const worldX = (mx / rect.width) * worldW - ctx.offsetX;
  const worldY = (my / rect.height) * worldH - ctx.offsetY;
  const sx = (worldX + ctx.offsetX) * ctx.zoom - ctx.canvasWrap.clientWidth / 2;
  const sy = (worldY + ctx.offsetY) * ctx.zoom - ctx.canvasWrap.clientHeight / 2;
  ctx.canvasWrap.scrollLeft = Math.max(0, sx);
  ctx.canvasWrap.scrollTop = Math.max(0, sy);
  ctx.render();
}

function bindCanvasEvents(ctx) {
  window.addEventListener("pointermove", (evt) => {
    ctx.lastClient = { x: evt.clientX, y: evt.clientY };
    storePointer(ctx, evt);

    if (ctx.touchPinch && updateTouchPinch(ctx)) return;
    if (ctx.touchPan && updateTouchPan(ctx, evt)) return;
    if (ctx.drag && (!Number.isFinite(ctx.drag.pointerId) || ctx.drag.pointerId === evt.pointerId)) {
      ctx.onDragMove(evt);
      return;
    }
    if (ctx.marquee && (!Number.isFinite(ctx.marquee.pointerId) || ctx.marquee.pointerId === evt.pointerId)) {
      ctx.onMarqueeMove(evt);
      return;
    }
    if (ctx.linking && (!Number.isFinite(ctx.linking.pointerId) || ctx.linking.pointerId === evt.pointerId)) {
      ctx.requestRender(false);
    }
  });

  const finishPointer = (evt) => {
    if (ctx.drag && (!Number.isFinite(ctx.drag.pointerId) || ctx.drag.pointerId === evt.pointerId)) ctx.onDragEnd();
    if (ctx.marquee && (!Number.isFinite(ctx.marquee.pointerId) || ctx.marquee.pointerId === evt.pointerId)) ctx.onMarqueeEnd(evt);
    if (ctx.linking && (!Number.isFinite(ctx.linking.pointerId) || ctx.linking.pointerId === evt.pointerId)) ctx.finishLinkByEvent(evt);
    if (ctx.touchPan && ctx.touchPan.pointerId === evt.pointerId) ctx.touchPan = null;
    if (ctx.touchPinch && Array.isArray(ctx.touchPinch.pointerIds) && ctx.touchPinch.pointerIds.includes(evt.pointerId)) ctx.touchPinch = null;
    dropPointer(ctx, evt);
    maybeRefreshTouchGesture(ctx);
  };

  window.addEventListener("pointerup", finishPointer);
  window.addEventListener("pointercancel", finishPointer);

  ctx.canvasWrap.addEventListener("scroll", () => {
    if (ctx.linking) ctx.requestRender(false);
    ctx.requestMinimap();
  });

  ctx.canvasSurface.addEventListener("pointerdown", (evt) => {
    ctx.lastClient = { x: evt.clientX, y: evt.clientY };
    storePointer(ctx, evt);

    const target = evt.target;
    if (target.closest && (target.closest(".node") || target.closest(".port") || target.closest(".edge-hit") || target.closest(".minimap-wrap"))) {
      maybeRefreshTouchGesture(ctx);
      return;
    }

    if (ctx.onEdgeSelect) ctx.onEdgeSelect(null);
    if (isTouchLike(evt)) {
      ctx.clearSelection();
      beginTouchPan(ctx, {
        pointerId: evt.pointerId,
        clientX: evt.clientX,
        clientY: evt.clientY,
      });
      ctx.requestRender(false);
      return;
    }

    if (!isPrimaryPointer(evt)) return;
    const s = ctx.clientToSurface(evt.clientX, evt.clientY);
    ctx.marquee = { x0: s.x, y0: s.y, x1: s.x, y1: s.y, pointerId: evt.pointerId };
    ctx.clearSelection();
    ctx.requestRender(false);
  });
}

function bindMinimapEvents(ctx) {
  if (!ctx.minimapCanvas) return;
  const finish = () => { ctx.minimapPointerId = null; };
  ctx.minimapCanvas.addEventListener("pointerdown", (evt) => {
    if (!isPrimaryPointer(evt)) return;
    ctx.minimapPointerId = evt.pointerId;
    jumpMinimap(ctx, evt.clientX, evt.clientY);
  });
  ctx.minimapCanvas.addEventListener("pointermove", (evt) => {
    if (ctx.minimapPointerId !== evt.pointerId) return;
    jumpMinimap(ctx, evt.clientX, evt.clientY);
  });
  ctx.minimapCanvas.addEventListener("pointerup", finish);
  ctx.minimapCanvas.addEventListener("pointercancel", finish);
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

function renderMinimap(ctx, nodeW, nodeH) {
  if (!ctx.minimapCanvas) return;
  const metrics = ctx.ensureMinimapBitmap ? ctx.ensureMinimapBitmap() : null;
  const g = ctx.minimapCanvas.getContext("2d");
  if (!g) return;

  const w = Number(metrics?.cssW || ctx.minimapCanvas.width || 180);
  const h = Number(metrics?.cssH || ctx.minimapCanvas.height || 120);
  const dpr = Number(metrics?.dpr || 1);
  g.setTransform(dpr, 0, 0, dpr, 0, 0);
  g.clearRect(0, 0, w, h);
  g.fillStyle = "#ecf4fe";
  g.fillRect(0, 0, w, h);

  const sx = w / ctx.surfaceWidth;
  const sy = h / ctx.surfaceHeight;

  const graph = ctx.store.state.graph;
  const nodeMap = new Map(graph.nodes.map((n) => [n.id, n]));
  g.strokeStyle = "#8aa8cd";
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

  g.fillStyle = "#0f6cbd";
  for (const n of graph.nodes) {
    const x = (n.x + ctx.offsetX) * sx;
    const y = (n.y + ctx.offsetY) * sy;
    g.fillRect(x, y, Math.max(3, nodeW * sx), Math.max(3, nodeH * sy));
  }

  const vx = ctx.canvasWrap.scrollLeft / ctx.zoom;
  const vy = ctx.canvasWrap.scrollTop / ctx.zoom;
  const vw = ctx.canvasWrap.clientWidth / ctx.zoom;
  const vh = ctx.canvasWrap.clientHeight / ctx.zoom;
  g.strokeStyle = "#ffb900";
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

function onLinkStart(ctx, evt, fromId) {
  evt.preventDefault();
  evt.stopPropagation();
  ctx.lastClient = { x: evt.clientX, y: evt.clientY };
  if (Number.isFinite(evt.pointerId)) storePointer(ctx, evt);
  ctx.linking = {
    from: fromId,
    pointerId: Number.isFinite(evt.pointerId) ? evt.pointerId : null,
  };
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
