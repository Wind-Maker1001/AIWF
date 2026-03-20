import {
  directLanePoints,
  inflateObstacles,
  orthogonalLaneCandidatePoints,
  orthogonalLanePoints,
  simplifyPoints,
} from './routing_core_support.mjs';
import {
  aStarOrthogonal,
  buildRouteAttempts,
  buildRouteWaypointCandidates,
} from './routing_core_astar.mjs';

const ROUTE_GRID = 24;
const DIR_RIGHT = 0;
const DIR_LEFT = 1;

function routedPath(a, b, obstacles = [], options = {}) {
  const routeGrid = Number(options.routeGrid || ROUTE_GRID);
  const attempts = buildRouteAttempts(routeGrid, options);

  for (const opts of attempts) {
    const obs = inflateObstacles(obstacles, opts.pad);
    const pts = aStarOrthogonal(a, b, obs, opts);
    if (pts && pts.length >= 2) return { points: pts, mode: "astar" };
  }

  const obstacleCount = (obstacles || []).length;
  const candidates = buildRouteWaypointCandidates(a, b, obstacleCount);

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
    return { points: directLanePoints(a, b), mode: "segmented" };
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
  return { points: directLanePoints(a, b), mode: "segmented" };
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
