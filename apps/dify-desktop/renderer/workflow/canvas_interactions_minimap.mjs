import {
  isPrimaryPointer,
  jumpMinimap,
} from './canvas_interactions_touch.mjs';

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

export {
  bindMinimapEvents,
  renderMinimap,
};
