import {
  heapPop,
  heapPush,
  inflateObstacles,
  keyOf,
  simplifyPoints,
  stateKey,
} from './routing_core_support.mjs';

function aStarOrthogonal(a, b, obstacles = [], options = {}) {
  const grid = Number(options.grid || 24);
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

function buildRouteWaypointCandidates(a, b, obstacleCount = 0) {
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
  return candidates;
}

function buildRouteAttempts(routeGrid, options = {}) {
  return options.attempts || [
    { grid: routeGrid, margin: 180, startDir: 0, endDir: 1, pad: 8 },
    { grid: routeGrid, margin: 220, startDir: -1, endDir: -1, pad: 3 },
    { grid: routeGrid, margin: 220, startDir: -1, endDir: -1, pad: 0 },
    { grid: routeGrid, margin: 300, startDir: -1, endDir: -1, pad: 0 },
  ];
}

export {
  aStarOrthogonal,
  buildRouteAttempts,
  buildRouteWaypointCandidates,
  inflateObstacles,
};
