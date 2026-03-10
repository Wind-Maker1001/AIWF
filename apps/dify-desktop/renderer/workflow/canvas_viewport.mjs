import { MINIMAP_CSS_W, MINIMAP_CSS_H } from './canvas_consts.mjs';

function clampZoomValue(zoom, minZoom, maxZoom) {
  const z = Number(zoom);
  if (!Number.isFinite(z)) return Number(minZoom || 1);
  return Math.max(Number(minZoom || 1), Math.min(Number(maxZoom || z), z));
}

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

function placeWorldPointAtClient(ctx, worldX, worldY, clientX, clientY) {
  const rect = ctx.canvasWrap.getBoundingClientRect();
  const localX = Number(clientX || 0) - rect.left;
  const localY = Number(clientY || 0) - rect.top;
  const surfaceX = (Number(worldX || 0) + ctx.offsetX) * ctx.zoom;
  const surfaceY = (Number(worldY || 0) + ctx.offsetY) * ctx.zoom;
  const maxLeft = Math.max(0, (ctx.canvasSurface.scrollWidth || ctx.canvasSurface.clientWidth || 0) - ctx.canvasWrap.clientWidth);
  const maxTop = Math.max(0, (ctx.canvasSurface.scrollHeight || ctx.canvasSurface.clientHeight || 0) - ctx.canvasWrap.clientHeight);
  ctx.canvasWrap.scrollLeft = Math.max(0, Math.min(maxLeft, surfaceX - localX));
  ctx.canvasWrap.scrollTop = Math.max(0, Math.min(maxTop, surfaceY - localY));
}

function fitWorldRectToViewport(bounds, viewportWidth, viewportHeight, padding, minZoom, maxZoom) {
  const safePadding = Math.max(24, Number(padding || 0));
  const width = Math.max(1, Number(bounds?.maxRight || 0) - Number(bounds?.minLeft || 0));
  const height = Math.max(1, Number(bounds?.maxBottom || 0) - Number(bounds?.minTop || 0));
  const availableWidth = Math.max(80, Number(viewportWidth || 0) - safePadding * 2);
  const availableHeight = Math.max(80, Number(viewportHeight || 0) - safePadding * 2);
  const scale = Math.min(availableWidth / width, availableHeight / height);
  return {
    zoom: clampZoomValue(scale, minZoom, maxZoom),
    centerX: Number(bounds?.minLeft || 0) + width / 2,
    centerY: Number(bounds?.minTop || 0) + height / 2,
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

function ensureMinimapBitmap(ctx) {
  const canvas = ctx.minimapCanvas;
  if (!canvas) return null;
  const cssW = Math.max(1, Number(canvas.clientWidth || MINIMAP_CSS_W));
  const cssH = Math.max(1, Number(canvas.clientHeight || MINIMAP_CSS_H));
  const dpr = Math.max(1, Math.min(3, Number(window.devicePixelRatio || 1)));
  const targetW = Math.round(cssW * dpr);
  const targetH = Math.round(cssH * dpr);
  if (canvas.width !== targetW) canvas.width = targetW;
  if (canvas.height !== targetH) canvas.height = targetH;
  return { cssW, cssH, dpr };
}

export {
  clientToSurface,
  clientToWorld,
  worldToDisplay,
  clampZoomValue,
  placeWorldPointAtClient,
  fitWorldRectToViewport,
  ensureViewport,
  ensureMinimapBitmap,
};
