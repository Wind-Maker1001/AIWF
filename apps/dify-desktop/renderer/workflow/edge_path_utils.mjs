import { orthogonalLanePoints as sharedOrthogonalLanePoints } from './routing_core.mjs';

function toPath(points) {
  return points.map((p, i) => (i === 0 ? `M ${p[0]} ${p[1]}` : `L ${p[0]} ${p[1]}`)).join(' ');
}

function orthogonalLanePath(a, b, obstacles = []) {
  return toPath(sharedOrthogonalLanePoints(a, b, obstacles));
}

function isBetween(v, a, b) {
  return v >= Math.min(a, b) && v <= Math.max(a, b);
}

function segmentCrossCount(a1, a2, b1, b2) {
  const [ax1, ay1] = a1;
  const [ax2, ay2] = a2;
  const [bx1, by1] = b1;
  const [bx2, by2] = b2;
  const aVertical = ax1 === ax2;
  const bVertical = bx1 === bx2;
  if (aVertical === bVertical) return 0;
  if (aVertical) {
    if (isBetween(ax1, bx1, bx2) && isBetween(by1, ay1, ay2)) return 1;
    return 0;
  }
  if (isBetween(bx1, ax1, ax2) && isBetween(ay1, by1, by2)) return 1;
  return 0;
}

function countPolylineCrossings(points, existingPolylines = []) {
  if (!Array.isArray(points) || points.length < 2 || !Array.isArray(existingPolylines) || existingPolylines.length === 0) return 0;
  let count = 0;
  for (let i = 1; i < points.length; i += 1) {
    const a1 = points[i - 1];
    const a2 = points[i];
    for (const line of existingPolylines) {
      if (!Array.isArray(line) || line.length < 2) continue;
      for (let j = 1; j < line.length; j += 1) {
        count += segmentCrossCount(a1, a2, line[j - 1], line[j]);
      }
    }
  }
  return count;
}

function fallbackEdgePath(a, b) {
  const ax = Math.round(Number(a?.x || 0));
  const ay = Math.round(Number(a?.y || 0));
  const bx = Math.round(Number(b?.x || 0));
  const by = Math.round(Number(b?.y || 0));
  const midX = Math.round((ax + bx) / 2);
  return `M ${ax} ${ay} L ${midX} ${ay} L ${midX} ${by} L ${bx} ${by}`;
}

function ensureEdgePath(d, a, b) {
  const s = String(d || '').trim();
  if (!s || s.includes('NaN') || s.includes('undefined') || !/[ML]/.test(s)) {
    return fallbackEdgePath(a, b);
  }
  return s;
}

export {
  toPath,
  orthogonalLanePath,
  countPolylineCrossings,
  fallbackEdgePath,
  ensureEdgePath,
};
