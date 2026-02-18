import { NODE_W, NODE_H } from "./canvas_consts.mjs";

const DISTRIBUTE_GAP = 24;

function applyAlignment(ctx, x, y, nodeId, nodeW, nodeH) {
  const threshold = 8;
  let sx = x;
  let sy = y;
  let bestX = null;
  let bestY = null;

  const candidates = [
    { key: "left", self: x },
    { key: "center", self: x + nodeW / 2 },
  ];
  const yc = [
    { key: "top", self: y },
    { key: "middle", self: y + nodeH / 2 },
  ];

  for (const n of ctx.store.state.graph.nodes) {
    if (n.id === nodeId) continue;
    const nx = Number(n.x || 0);
    const ny = Number(n.y || 0);
    const xTargets = { left: nx, center: nx + nodeW / 2 };
    const yTargets = { top: ny, middle: ny + nodeH / 2 };

    for (const c of candidates) {
      const t = xTargets[c.key];
      const d = Math.abs(c.self - t);
      if (d <= threshold && (!bestX || d < bestX.dist)) {
        bestX = { dist: d, target: t, key: c.key };
      }
    }
    for (const c of yc) {
      const t = yTargets[c.key];
      const d = Math.abs(c.self - t);
      if (d <= threshold && (!bestY || d < bestY.dist)) {
        bestY = { dist: d, target: t, key: c.key };
      }
    }
  }

  if (bestX) {
    sx = bestX.key === "left" ? bestX.target : bestX.target - nodeW / 2;
    ctx.guides.x = bestX.target;
  } else {
    ctx.guides.x = null;
  }

  if (bestY) {
    sy = bestY.key === "top" ? bestY.target : bestY.target - nodeH / 2;
    ctx.guides.y = bestY.target;
  } else {
    ctx.guides.y = null;
  }

  return { x: sx, y: sy };
}

function enforceNonOverlap(ctx, nodes, axis) {
  const list = [...nodes].sort((a, b) => {
    if (axis === "x") {
      const dx = Number(a.x || 0) - Number(b.x || 0);
      if (dx !== 0) return dx;
      return Number(a.y || 0) - Number(b.y || 0);
    }
    const dy = Number(a.y || 0) - Number(b.y || 0);
    if (dy !== 0) return dy;
    return Number(a.x || 0) - Number(b.x || 0);
  });

  const minStep = axis === "x" ? (NODE_W + DISTRIBUTE_GAP) : (NODE_H + DISTRIBUTE_GAP);
  for (let i = 1; i < list.length; i += 1) {
    const prev = list[i - 1];
    const cur = list[i];
    const prevPos = axis === "x" ? Number(prev.x || 0) : Number(prev.y || 0);
    const curPos = axis === "x" ? Number(cur.x || 0) : Number(cur.y || 0);
    const minPos = prevPos + minStep;
    if (curPos >= minPos) continue;
    if (axis === "x") ctx.store.moveNode(cur.id, minPos, Number(cur.y || 0));
    else ctx.store.moveNode(cur.id, Number(cur.x || 0), minPos);
  }
}

function alignSelected(ctx, mode) {
  const ids = Array.from(ctx.selectedIds);
  if (ids.length < 2) {
    ctx.onWarn("请先选择至少两个节点");
    return { ok: false, reason: "min_2" };
  }
  const nodes = ids
    .map((id) => ctx.store.state.graph.nodes.find((n) => n.id === id))
    .filter(Boolean);
  if (nodes.length < 2) return { ok: false, reason: "min_2" };
  if ((mode === "hspace" || mode === "vspace") && nodes.length < 3) {
    ctx.onWarn("水平/垂直分布至少需要三个节点");
    return { ok: false, reason: "min_3_distribute" };
  }

  const before = new Map(nodes.map((n) => [n.id, { x: Number(n.x || 0), y: Number(n.y || 0) }]));

  if (mode === "left") {
    const x = Math.min(...nodes.map((n) => Number(n.x || 0)));
    nodes.forEach((n) => ctx.store.moveNode(n.id, x, Number(n.y || 0)));
    if (ctx.arrangePolicy?.preventOverlapOnAlign) enforceNonOverlap(ctx, nodes, "y");
  } else if (mode === "top") {
    const y = Math.min(...nodes.map((n) => Number(n.y || 0)));
    nodes.forEach((n) => ctx.store.moveNode(n.id, Number(n.x || 0), y));
    if (ctx.arrangePolicy?.preventOverlapOnAlign) enforceNonOverlap(ctx, nodes, "x");
  } else if (mode === "hspace") {
    const sorted = [...nodes].sort((a, b) => (Number(a.x || 0) + NODE_W / 2) - (Number(b.x || 0) + NODE_W / 2));
    const rawMinCenter = Number(sorted[0].x || 0) + NODE_W / 2;
    const rawMaxCenter = Number(sorted[sorted.length - 1].x || 0) + NODE_W / 2;
    const rawSpan = rawMaxCenter - rawMinCenter;
    const minStep = NODE_W + DISTRIBUTE_GAP;
    const minSpan = minStep * (sorted.length - 1);
    const span = Math.max(rawSpan, minSpan);
    const mid = (rawMinCenter + rawMaxCenter) / 2;
    const minCenter = mid - span / 2;
    const step = span / (sorted.length - 1);
    sorted.forEach((n, i) => {
      const center = minCenter + i * step;
      ctx.store.moveNode(n.id, Math.round(center - NODE_W / 2), Number(n.y || 0));
    });
    enforceNonOverlap(ctx, nodes, "x");
  } else if (mode === "vspace") {
    const sorted = [...nodes].sort((a, b) => (Number(a.y || 0) + NODE_H / 2) - (Number(b.y || 0) + NODE_H / 2));
    const rawMinCenter = Number(sorted[0].y || 0) + NODE_H / 2;
    const rawMaxCenter = Number(sorted[sorted.length - 1].y || 0) + NODE_H / 2;
    const rawSpan = rawMaxCenter - rawMinCenter;
    const minStep = NODE_H + DISTRIBUTE_GAP;
    const minSpan = minStep * (sorted.length - 1);
    const span = Math.max(rawSpan, minSpan);
    const mid = (rawMinCenter + rawMaxCenter) / 2;
    const minCenter = mid - span / 2;
    const step = span / (sorted.length - 1);
    sorted.forEach((n, i) => {
      const center = minCenter + i * step;
      ctx.store.moveNode(n.id, Number(n.x || 0), Math.round(center - NODE_H / 2));
    });
    enforceNonOverlap(ctx, nodes, "y");
  } else {
    ctx.onWarn(`不支持的对齐模式: ${mode}`);
    return { ok: false, reason: "unknown_mode" };
  }

  let moved = 0;
  for (const n of nodes) {
    const b = before.get(n.id);
    const nx = Number(n.x || 0);
    const ny = Number(n.y || 0);
    if (!b || b.x !== nx || b.y !== ny) moved += 1;
  }
  ctx.invalidateEdgesForNodes(ids);
  ctx.onChange();
  return { ok: true, moved, total: nodes.length, mode };
}

export {
  applyAlignment,
  alignSelected,
};
