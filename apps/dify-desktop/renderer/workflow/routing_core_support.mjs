function segmentIntersectsRect(x1, y1, x2, y2, r) {
  const pad = 1;
  const rx1 = r.x - pad;
  const ry1 = r.y - pad;
  const rx2 = r.x + r.w + pad;
  const ry2 = r.y + r.h + pad;
  if (x1 === x2) {
    if (x1 < rx1 || x1 > rx2) return false;
    const a = Math.min(y1, y2);
    const b = Math.max(y1, y2);
    return !(b < ry1 || a > ry2);
  }
  if (y1 === y2) {
    if (y1 < ry1 || y1 > ry2) return false;
    const a = Math.min(x1, x2);
    const b = Math.max(x1, x2);
    return !(b < rx1 || a > rx2);
  }
  return false;
}

function polylineBlocked(points, obstacles) {
  for (let i = 1; i < points.length; i += 1) {
    const [x1, y1] = points[i - 1];
    const [x2, y2] = points[i];
    for (const r of obstacles || []) {
      if (segmentIntersectsRect(x1, y1, x2, y2, r)) return true;
    }
  }
  return false;
}

function simplifyPoints(points) {
  if (!points || points.length <= 2) return points || [];
  const out = [points[0]];
  for (let i = 1; i < points.length - 1; i += 1) {
    const [x0, y0] = out[out.length - 1];
    const [x1, y1] = points[i];
    const [x2, y2] = points[i + 1];
    const collinear = (x0 === x1 && x1 === x2) || (y0 === y1 && y1 === y2);
    if (!collinear) out.push([x1, y1]);
  }
  out.push(points[points.length - 1]);
  return out;
}

function directLanePoints(a, b) {
  const midX = Math.round((a.x + b.x) / 2);
  return [[a.x, a.y], [midX, a.y], [midX, b.y], [b.x, b.y]];
}

function orthogonalLaneCandidatePoints(a, b, obstacles = []) {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const signX = dx >= 0 ? 1 : -1;
  const direct = directLanePoints(a, b);
  if (!polylineBlocked(direct, obstacles)) return direct;

  const spanX = Math.abs(dx);
  const spanY = Math.abs(dy);
  const baseDist = Math.max(70, Math.min(170, spanX * 0.35));
  const outDists = [baseDist, Math.max(56, Math.round(baseDist * 0.72)), Math.round(baseDist * 1.18)];
  const laneShift = [70, 110, 150, 190, 250];
  const laneSet = new Set([Math.round(a.y), Math.round(b.y), Math.round((a.y + b.y) / 2)]);
  for (const s of laneShift) {
    laneSet.add(Math.round(a.y - s));
    laneSet.add(Math.round(a.y + s));
    laneSet.add(Math.round(b.y - s));
    laneSet.add(Math.round(b.y + s));
  }
  if (spanY < 28) {
    laneSet.add(Math.round(a.y - 290));
    laneSet.add(Math.round(a.y + 290));
  }

  let best = null;
  let bestCost = Number.POSITIVE_INFINITY;
  for (const dOut of outDists) {
    const outX = Math.round(a.x + signX * dOut);
    const inX = Math.round(b.x - signX * dOut);
    for (const laneY of laneSet) {
      const via = [[a.x, a.y], [outX, a.y], [outX, laneY], [inX, laneY], [inX, b.y], [b.x, b.y]];
      if (polylineBlocked(via, obstacles)) continue;
      const cost = Math.abs(outX - a.x) + Math.abs(laneY - a.y) + Math.abs(inX - outX) + Math.abs(b.y - laneY) + Math.abs(b.x - inX);
      if (cost < bestCost) {
        bestCost = cost;
        best = via;
      }
    }
  }
  return best;
}

function orthogonalLanePoints(a, b, obstacles = []) {
  return orthogonalLaneCandidatePoints(a, b, obstacles) || directLanePoints(a, b);
}

function inflateObstacles(obstacles = [], pad = 0) {
  const p = Math.max(0, Number(pad || 0));
  if (p <= 0) return obstacles || [];
  return (obstacles || []).map((r) => ({
    x: r.x - p,
    y: r.y - p,
    w: r.w + p * 2,
    h: r.h + p * 2,
  }));
}

function keyOf(x, y) {
  return `${x},${y}`;
}

function stateKey(x, y, dir) {
  return `${x},${y},${dir}`;
}

function heapPush(heap, item) {
  heap.push(item);
  let i = heap.length - 1;
  while (i > 0) {
    const p = (i - 1) >> 1;
    if (heap[p].f <= heap[i].f) break;
    [heap[p], heap[i]] = [heap[i], heap[p]];
    i = p;
  }
}

function heapPop(heap) {
  if (!heap.length) return null;
  const top = heap[0];
  const last = heap.pop();
  if (heap.length && last) {
    heap[0] = last;
    let i = 0;
    for (;;) {
      const l = i * 2 + 1;
      const r = i * 2 + 2;
      let s = i;
      if (l < heap.length && heap[l].f < heap[s].f) s = l;
      if (r < heap.length && heap[r].f < heap[s].f) s = r;
      if (s === i) break;
      [heap[i], heap[s]] = [heap[s], heap[i]];
      i = s;
    }
  }
  return top;
}

export {
  directLanePoints,
  heapPop,
  heapPush,
  inflateObstacles,
  keyOf,
  orthogonalLaneCandidatePoints,
  orthogonalLanePoints,
  polylineBlocked,
  segmentIntersectsRect,
  simplifyPoints,
  stateKey,
};
