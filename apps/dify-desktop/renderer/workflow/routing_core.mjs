const ROUTE_GRID = 24;
const DIR_RIGHT = 0;
const DIR_LEFT = 1;

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

function aStarOrthogonal(a, b, obstacles = [], options = {}) {
  const grid = Number(options.grid || ROUTE_GRID);
  const margin = Number(options.margin || 180);
  const startDir = Number.isInteger(options.startDir) ? options.startDir : -1;
  const endDir = Number.isInteger(options.endDir) ? options.endDir : -1;
  const guardMax = Number(options.guardMax || 24000);
  let minX = Math.min(a.x, b.x) - margin;
  let minY = Math.min(a.y, b.y) - margin;
  let maxX = Math.max(a.x, b.x) + margin;
  let maxY = Math.max(a.y, b.y) + margin;

  for (const r of obstacles) {
    minX = Math.min(minX, r.x - margin);
    minY = Math.min(minY, r.y - margin);
    maxX = Math.max(maxX, r.x + r.w + margin);
    maxY = Math.max(maxY, r.y + r.h + margin);
  }

  const toGrid = (x, y) => ({
    gx: Math.round((x - minX) / grid),
    gy: Math.round((y - minY) / grid),
  });
  const toCoord = (gx, gy) => [Math.round(minX + gx * grid), Math.round(minY + gy * grid)];
  const maxGX = Math.ceil((maxX - minX) / grid);
  const maxGY = Math.ceil((maxY - minY) / grid);

  const start = toGrid(a.x, a.y);
  const goal = toGrid(b.x, b.y);

  const blocked = new Set();
  for (const r of obstacles) {
    const x1 = Math.floor((r.x - minX) / grid);
    const y1 = Math.floor((r.y - minY) / grid);
    const x2 = Math.ceil((r.x + r.w - minX) / grid);
    const y2 = Math.ceil((r.y + r.h - minY) / grid);
    for (let gx = x1; gx <= x2; gx += 1) {
      for (let gy = y1; gy <= y2; gy += 1) {
        blocked.add(keyOf(gx, gy));
      }
    }
  }
  blocked.delete(keyOf(start.gx, start.gy));
  blocked.delete(keyOf(goal.gx, goal.gy));

  const h = (x, y) => Math.abs(x - goal.gx) + Math.abs(y - goal.gy);
  const open = [];
  heapPush(open, { x: start.gx, y: start.gy, dir: -1, f: h(start.gx, start.gy), g: 0 });
  const gScore = new Map([[stateKey(start.gx, start.gy, -1), 0]]);
  const came = new Map();

  const dirs = [
    [1, 0],
    [-1, 0],
    [0, 1],
    [0, -1],
  ];

  let guard = 0;
  while (open.length && guard < guardMax) {
    guard += 1;
    const cur = heapPop(open);
    if (!cur) break;
    if (cur.x === goal.gx && cur.y === goal.gy) {
      if (endDir >= 0 && cur.dir >= 0 && cur.dir !== endDir) continue;
      const rev = [];
      let k = stateKey(cur.x, cur.y, cur.dir);
      while (k) {
        const [px, py] = String(k).split(",").map((v) => Number(v));
        rev.push(toCoord(px, py));
        k = came.get(k);
      }
      const pts = rev.reverse();
      if (!pts.length) return null;
      pts[0] = [Math.round(a.x), Math.round(a.y)];
      pts[pts.length - 1] = [Math.round(b.x), Math.round(b.y)];
      return simplifyPoints(pts);
    }

    for (let di = 0; di < dirs.length; di += 1) {
      const [dx, dy] = dirs[di];
      const nx = cur.x + dx;
      const ny = cur.y + dy;
      if (nx < 0 || ny < 0 || nx > maxGX || ny > maxGY) continue;
      const nk = keyOf(nx, ny);
      if (blocked.has(nk)) continue;
      const turnPenalty = cur.dir >= 0 && cur.dir !== di ? 0.35 : 0;
      const reversePenalty = cur.dir >= 0 && ((cur.dir + 2) % 4 === di) ? 0.8 : 0;
      const fromStart = cur.x === start.gx && cur.y === start.gy;
      const startPenalty = startDir >= 0 && fromStart && di !== startDir ? 2 : 0;
      const toGoal = nx === goal.gx && ny === goal.gy;
      const goalPenalty = endDir >= 0 && toGoal && di !== endDir ? 1 : 0;
      const ng = cur.g + 1 + turnPenalty + reversePenalty + startPenalty + goalPenalty;
      const sk = stateKey(nx, ny, di);
      const old = gScore.get(sk);
      if (old !== undefined && ng >= old) continue;
      came.set(sk, stateKey(cur.x, cur.y, cur.dir));
      gScore.set(sk, ng);
      heapPush(open, { x: nx, y: ny, dir: di, g: ng, f: ng + h(nx, ny) });
    }
  }
  return null;
}

function routedPath(a, b, obstacles = [], options = {}) {
  const routeGrid = Number(options.routeGrid || ROUTE_GRID);
  const attempts = options.attempts || [
    { grid: routeGrid, margin: 180, startDir: DIR_RIGHT, endDir: DIR_LEFT, pad: 8 },
    { grid: routeGrid, margin: 220, startDir: -1, endDir: -1, pad: 3 },
    { grid: routeGrid, margin: 220, startDir: -1, endDir: -1, pad: 0 },
    { grid: routeGrid, margin: 300, startDir: -1, endDir: -1, pad: 0 },
  ];

  for (const opts of attempts) {
    const obs = inflateObstacles(obstacles, opts.pad);
    const pts = aStarOrthogonal(a, b, obs, opts);
    if (pts && pts.length >= 2) return { points: pts, mode: "astar" };
  }

  const obstacleCount = (obstacles || []).length;
  const midX = Math.round((a.x + b.x) / 2);
  const midY = Math.round((a.y + b.y) / 2);
  const dx = Math.max(120, Math.round(Math.abs(a.x - b.x) * 0.25));
  const dy = Math.max(90, Math.round(Math.abs(a.y - b.y) * 0.25));
  const rawCandidates = [
    { x: midX, y: a.y },
    { x: midX, y: b.y },
    { x: midX + dx, y: a.y },
    { x: midX + dx, y: b.y },
    { x: midX - dx, y: a.y },
    { x: midX - dx, y: b.y },
    { x: a.x + dx, y: midY },
    { x: b.x - dx, y: midY },
    { x: a.x, y: a.y - dy },
    { x: a.x, y: a.y + dy },
    { x: b.x, y: b.y - dy },
    { x: b.x, y: b.y + dy },
  ];
  const seen = new Set();
  const candidates = [];
  for (const wp of rawCandidates) {
    const k = `${Math.round(wp.x)},${Math.round(wp.y)}`;
    if (seen.has(k)) continue;
    seen.add(k);
    candidates.push({ x: Math.round(wp.x), y: Math.round(wp.y) });
    if (candidates.length >= (obstacleCount > 180 ? 8 : 12)) break;
  }

  if (obstacleCount > 260) {
    const lane = orthogonalLaneCandidatePoints(a, b, obstacles);
    if (lane && lane.length >= 2) return { points: lane, mode: "segmented" };
    const denseAttempts = [
      ...attempts,
      { grid: routeGrid, margin: 360, startDir: -1, endDir: -1, pad: 0 },
      { grid: routeGrid, margin: 420, startDir: -1, endDir: -1, pad: 0 },
    ];
    for (const opts of denseAttempts) {
      const obs = inflateObstacles(obstacles, opts.pad);
      for (const wp of candidates.slice(0, 10)) {
        const p1 = aStarOrthogonal(a, wp, obs, { ...opts, endDir: -1 });
        const p2 = p1 ? aStarOrthogonal(wp, b, obs, { ...opts, startDir: -1 }) : null;
        if (!p1 || !p2 || p1.length < 2 || p2.length < 2) continue;
        const merged = [...p1, ...p2.slice(1)];
        const simp = simplifyPoints(merged);
        return { points: simp, mode: "segmented" };
      }
    }
    for (const opts of denseAttempts.slice(0, 4)) {
      const obs = inflateObstacles(obstacles, opts.pad);
      const denseCands = candidates.slice(0, 6);
      for (let i = 0; i < denseCands.length; i += 1) {
        for (let j = 0; j < denseCands.length; j += 1) {
          if (i === j) continue;
          const w1 = denseCands[i];
          const w2 = denseCands[j];
          const p1 = aStarOrthogonal(a, w1, obs, { ...opts, endDir: -1 });
          const p2 = p1 ? aStarOrthogonal(w1, w2, obs, { ...opts, startDir: -1, endDir: -1 }) : null;
          const p3 = p2 ? aStarOrthogonal(w2, b, obs, { ...opts, startDir: -1 }) : null;
          if (!p1 || !p2 || !p3 || p1.length < 2 || p2.length < 2 || p3.length < 2) continue;
          const merged = [...p1, ...p2.slice(1), ...p3.slice(1)];
          const simp = simplifyPoints(merged);
          if (simp.length >= 2) return { points: simp, mode: "segmented" };
        }
      }
    }
    return { points: directLanePoints(a, b), mode: "fallback" };
  }

  const segAttempts = obstacleCount > 180 ? attempts.slice(0, 2) : attempts;
  for (const opts of segAttempts) {
    const obs = inflateObstacles(obstacles, opts.pad);
    for (const wp of candidates) {
      const p1 = aStarOrthogonal(a, wp, obs, { ...opts, endDir: -1 });
      const p2 = p1 ? aStarOrthogonal(wp, b, obs, { ...opts, startDir: -1 }) : null;
      if (!p1 || !p2 || p1.length < 2 || p2.length < 2) continue;
      const merged = [...p1, ...p2.slice(1)];
      const simp = simplifyPoints(merged);
      return { points: simp, mode: "segmented" };
    }
  }

  const lane = orthogonalLaneCandidatePoints(a, b, obstacles);
  if (lane && lane.length >= 2) return { points: lane, mode: "segmented" };
  return { points: directLanePoints(a, b), mode: "fallback" };
}

export {
  ROUTE_GRID,
  DIR_RIGHT,
  DIR_LEFT,
  directLanePoints,
  orthogonalLaneCandidatePoints,
  orthogonalLanePoints,
  routedPath,
};
