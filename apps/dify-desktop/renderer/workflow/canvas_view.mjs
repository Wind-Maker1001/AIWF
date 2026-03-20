function setSnap(canvas, enabled) {
  canvas.snapEnabled = !!enabled;
}

function setZoom(canvas, zoom, focusClient, clampZoomValue, minZoom, maxZoom, placeWorldPointAtClient) {
  const nextZoom = clampZoomValue(zoom, minZoom, maxZoom);
  const rect = focusClient && canvas.canvasWrap ? canvas.canvasWrap.getBoundingClientRect() : null;
  const hasFocus = rect && Number.isFinite(focusClient?.clientX) && Number.isFinite(focusClient?.clientY);
  const worldFocus = hasFocus
    ? {
      x: Number.isFinite(focusClient?.anchorWorld?.x) ? Number(focusClient.anchorWorld.x) : canvas.clientToWorld(focusClient.clientX, focusClient.clientY).x,
      y: Number.isFinite(focusClient?.anchorWorld?.y) ? Number(focusClient.anchorWorld.y) : canvas.clientToWorld(focusClient.clientX, focusClient.clientY).y,
    }
    : null;

  if (Math.abs(nextZoom - canvas.zoom) < 0.0001) {
    if (worldFocus) {
      placeWorldPointAtClient(canvas, worldFocus.x, worldFocus.y, focusClient.clientX, focusClient.clientY);
      canvas.requestRender(true);
    }
    return;
  }

  canvas.zoom = nextZoom;
  canvas.invalidateAllRoutes();
  canvas.renderCore(false);
  if (worldFocus && rect) {
    placeWorldPointAtClient(canvas, worldFocus.x, worldFocus.y, focusClient.clientX, focusClient.clientY);
  }
  canvas.requestRender(true);
}

function getZoom(canvas) {
  return canvas.zoom;
}

function render(canvas) {
  canvas.renderCore(true);
}

function renderCore(canvas, withMinimap) {
  canvas.ensureViewport();
  canvas.renderNodes();
  canvas.renderEdges();
  canvas.renderRouteDebug();
  canvas.renderGuides();
  canvas.renderMarquee();
  if (withMinimap) canvas.renderMinimap();
}

export {
  getZoom,
  render,
  renderCore,
  setSnap,
  setZoom,
};
