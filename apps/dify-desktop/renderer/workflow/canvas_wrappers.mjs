function clientToSurface(canvas, clientX, clientY, viewportClientToSurface) {
  return viewportClientToSurface(canvas, clientX, clientY);
}

function clientToWorld(canvas, clientX, clientY, viewportClientToWorld) {
  return viewportClientToWorld(canvas, clientX, clientY);
}

function worldToDisplay(canvas, x, y, viewportWorldToDisplay) {
  return viewportWorldToDisplay(canvas, x, y);
}

function bindCanvasEvents(canvas, impl) {
  impl(canvas);
}

function bindMinimapEvents(canvas, impl) {
  impl(canvas);
}

function ensureViewport(canvas, extraPoint, impl, expandPad, viewPad, nodeWidth, nodeHeight) {
  impl(canvas, extraPoint, expandPad, viewPad, nodeWidth, nodeHeight);
}

function ensureMinimapBitmap(canvas, impl) {
  return impl(canvas);
}

function renderNodes(canvas, impl, nodeWidth, nodeHeight, createElement) {
  impl(canvas, nodeWidth, nodeHeight, createElement);
}

function fitToView(canvas, padding, getGraphBounds, fitWorldRectToViewport, minZoom, maxZoom) {
  const bounds = getGraphBounds();
  if (!bounds || canvas.canvasWrap.clientWidth <= 0 || canvas.canvasWrap.clientHeight <= 0) return false;
  const fit = fitWorldRectToViewport(bounds, canvas.canvasWrap.clientWidth, canvas.canvasWrap.clientHeight, padding, minZoom, maxZoom);
  const rect = canvas.canvasWrap.getBoundingClientRect();
  canvas.setZoom(fit.zoom, {
    clientX: rect.left + canvas.canvasWrap.clientWidth / 2,
    clientY: rect.top + canvas.canvasWrap.clientHeight / 2,
    anchorWorld: { x: fit.centerX, y: fit.centerY },
  });
  return true;
}

function updateNodePositionsFast(canvas, ids, impl) {
  impl(canvas, ids);
}

function invalidateAllRoutes(canvas) {
  canvas.routeCache.clear();
}

function renderEdges(canvas, impl) {
  impl(canvas);
}

function applyAlignment(canvas, x, y, nodeId, impl, nodeWidth, nodeHeight) {
  return impl(canvas, x, y, nodeId, nodeWidth, nodeHeight);
}

function renderGuides(canvas, impl, createElement) {
  impl(canvas, createElement);
}

function renderMarquee(canvas, impl, createElement) {
  impl(canvas, createElement);
}

function onMarqueeMove(canvas, evt, impl) {
  impl(canvas, evt);
}

function onMarqueeEnd(canvas, evt, impl, nodeWidth, nodeHeight) {
  impl(canvas, nodeWidth, nodeHeight, evt);
}

function renderMinimap(canvas, impl, nodeWidth, nodeHeight) {
  impl(canvas, nodeWidth, nodeHeight);
}

function alignSelected(canvas, mode, impl) {
  return impl(canvas, mode);
}

function onDragStart(canvas, evt, nodeId, impl) {
  impl(canvas, evt, nodeId);
}

function onDragMove(canvas, evt, impl) {
  impl(canvas, evt);
}

function onDragEnd(canvas, impl) {
  impl(canvas);
}

function onLinkStart(canvas, evt, fromId, impl) {
  impl(canvas, evt, fromId);
}

function linkErrorMessage(reason, impl) {
  return impl(reason);
}

function finishLinkByEvent(canvas, evt, impl) {
  impl(canvas, evt);
}

function markLinkTargets(canvas, impl) {
  impl(canvas);
}

function clearLinkTargets(canvas, impl) {
  impl(canvas);
}

export {
  alignSelected,
  applyAlignment,
  bindCanvasEvents,
  bindMinimapEvents,
  clearLinkTargets,
  clientToSurface,
  clientToWorld,
  ensureMinimapBitmap,
  ensureViewport,
  finishLinkByEvent,
  fitToView,
  invalidateAllRoutes,
  linkErrorMessage,
  markLinkTargets,
  onDragEnd,
  onDragMove,
  onDragStart,
  onLinkStart,
  onMarqueeEnd,
  onMarqueeMove,
  renderEdges,
  renderGuides,
  renderMarquee,
  renderMinimap,
  renderNodes,
  updateNodePositionsFast,
  worldToDisplay,
};
