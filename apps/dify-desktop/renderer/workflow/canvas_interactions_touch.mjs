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

export {
  beginTouchPan,
  beginTouchPinch,
  distance,
  dropPointer,
  isPrimaryPointer,
  isTouchLike,
  jumpMinimap,
  maybeRefreshTouchGesture,
  midpoint,
  snap,
  storePointer,
  touchPointers,
  updateTouchPan,
  updateTouchPinch,
};
