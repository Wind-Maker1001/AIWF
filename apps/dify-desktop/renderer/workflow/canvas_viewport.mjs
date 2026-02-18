function clientToSurface(ctx, clientX, clientY) {
  const rect = ctx.canvasWrap.getBoundingClientRect();
  return {
    x: clientX - rect.left + ctx.canvasWrap.scrollLeft,
    y: clientY - rect.top + ctx.canvasWrap.scrollTop,
  };
}

function clientToWorld(ctx, clientX, clientY) {
  const s = clientToSurface(ctx, clientX, clientY);
  return {
    x: (s.x / ctx.zoom) - ctx.offsetX,
    y: (s.y / ctx.zoom) - ctx.offsetY,
  };
}

function worldToDisplay(ctx, x, y) {
  return {
    x: (x + ctx.offsetX) * ctx.zoom,
    y: (y + ctx.offsetY) * ctx.zoom,
  };
}

function ensureViewport(ctx, extraPoint, expandPad, viewPad, nodeW, nodeH) {
  const nodes = ctx.store.state.graph.nodes;
  let minX = 0;
  let minY = 0;
  let maxX = Math.max(ctx.canvasWrap.clientWidth, 1200);
  let maxY = Math.max(ctx.canvasWrap.clientHeight, 900);

  for (const n of nodes) {
    const nx = Number(n.x || 0);
    const ny = Number(n.y || 0);
    minX = Math.min(minX, nx - expandPad);
    minY = Math.min(minY, ny - expandPad);
    maxX = Math.max(maxX, nx + nodeW + expandPad);
    maxY = Math.max(maxY, ny + nodeH + expandPad);
  }

  if (extraPoint) {
    const ex = Number(extraPoint.x || 0);
    const ey = Number(extraPoint.y || 0);
    minX = Math.min(minX, ex - expandPad);
    minY = Math.min(minY, ey - expandPad);
    maxX = Math.max(maxX, ex + nodeW + expandPad);
    maxY = Math.max(maxY, ey + nodeH + expandPad);
  }

  const oldOffsetX = ctx.offsetX;
  const oldOffsetY = ctx.offsetY;

  ctx.worldMinX = minX;
  ctx.worldMinY = minY;
  ctx.offsetX = -ctx.worldMinX + viewPad;
  ctx.offsetY = -ctx.worldMinY + viewPad;
  ctx.surfaceWidth = Math.max(ctx.canvasWrap.clientWidth + 240, maxX - minX + viewPad * 2);
  ctx.surfaceHeight = Math.max(ctx.canvasWrap.clientHeight + 240, maxY - minY + viewPad * 2);

  const dispW = ctx.surfaceWidth * ctx.zoom;
  const dispH = ctx.surfaceHeight * ctx.zoom;
  ctx.canvasSurface.style.width = `${dispW}px`;
  ctx.canvasSurface.style.height = `${dispH}px`;
  ctx.canvasSurface.style.backgroundSize = `${ctx.gridSize * ctx.zoom}px ${ctx.gridSize * ctx.zoom}px`;

  const dx = ctx.offsetX - oldOffsetX;
  const dy = ctx.offsetY - oldOffsetY;
  if (Math.abs(dx) > 0.1 || Math.abs(dy) > 0.1) {
    ctx.canvasWrap.scrollLeft += dx * ctx.zoom;
    ctx.canvasWrap.scrollTop += dy * ctx.zoom;
  }
}

export {
  clientToSurface,
  clientToWorld,
  worldToDisplay,
  ensureViewport,
};
