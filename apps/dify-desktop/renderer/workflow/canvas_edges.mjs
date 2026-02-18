import {
  orthogonalLanePoints as sharedOrthogonalLanePoints,
  routedPath as sharedRoutedPath,
} from './routing_core.mjs';
import {
  toPath,
  orthogonalLanePath,
  countPolylineCrossings,
  ensureEdgePath,
} from './edge_path_utils.mjs';

function svgEl(tag) {
  return document.createElementNS('http://www.w3.org/2000/svg', tag);
}

function renderEdgesLayer(ctx) {
  ctx.edgesSvg.innerHTML = '';
  const dispW = ctx.surfaceWidth * ctx.zoom;
  const dispH = ctx.surfaceHeight * ctx.zoom;
  ctx.edgesSvg.setAttribute('width', String(dispW));
  ctx.edgesSvg.setAttribute('height', String(dispH));
  ctx.edgesSvg.setAttribute('viewBox', `0 0 ${dispW} ${dispH}`);

  const defs = svgEl('defs');
  const marker = svgEl('marker');
  marker.setAttribute('id', 'arrowHead');
  marker.setAttribute('viewBox', '0 0 10 10');
  marker.setAttribute('refX', '8');
  marker.setAttribute('refY', '5');
  marker.setAttribute('markerWidth', '6');
  marker.setAttribute('markerHeight', '6');
  marker.setAttribute('orient', 'auto-start-reverse');
  const tip = svgEl('path');
  tip.setAttribute('d', 'M 0 0 L 10 5 L 0 10 z');
  tip.setAttribute('fill', '#2c70b1');
  marker.append(tip);

  const glow = svgEl('filter');
  glow.setAttribute('id', 'edgeGlow');
  glow.setAttribute('x', '-20%');
  glow.setAttribute('y', '-20%');
  glow.setAttribute('width', '140%');
  glow.setAttribute('height', '140%');
  const blur = svgEl('feGaussianBlur');
  blur.setAttribute('stdDeviation', '0.8');
  blur.setAttribute('result', 'blur');
  glow.append(blur);

  defs.append(marker, glow);
  ctx.edgesSvg.append(defs);

  const graph = ctx.store.state.graph;
  ctx.routeMetrics = { edges: graph.edges.length, astar: 0, segmented: 0, fallback: 0, cacheHit: 0, cacheMiss: 0 };
  const useFastRouting = !!ctx.drag || !!ctx.linking;
  if (!useFastRouting) {
    const sig = ctx.buildGraphShapeSig(graph);
    if (sig !== ctx.routeSig) {
      ctx.routeSig = sig;
      ctx.invalidateAllRoutes();
    }
  }

  const aliveEdgeIds = new Set();
  const placedPolylines = [];
  for (const e of graph.edges) {
    const points = ctx.edgePoints(e.from, e.to);
    if (!points) continue;
    const edgeId = `${e.from}->${e.to}`;
    aliveEdgeIds.add(edgeId);
    let d = '';
    if (useFastRouting) {
      const pts = sharedOrthogonalLanePoints(points.a, points.b, []);
      d = toPath(pts);
      placedPolylines.push(pts);
      ctx.routeMetrics.fallback += 1;
    } else {
      const routeKey = `${Math.round(points.a.x)},${Math.round(points.a.y)}|${Math.round(points.b.x)},${Math.round(points.b.y)}`;
      const picked = ctx.edgeObstacleCandidates([e.from, e.to], points.a, points.b);
      const depsSig = Array.from(picked.deps).sort().join("|");
      const routeKeyWithDeps = `${routeKey}|${depsSig}`;
      const hit = ctx.routeCache.get(edgeId);
      if (hit && hit.routeKey === routeKeyWithDeps) {
        d = hit.d;
        if (Array.isArray(hit.points) && hit.points.length >= 2) placedPolylines.push(hit.points);
        ctx.routeMetrics.cacheHit += 1;
        if (hit.mode === 'astar') ctx.routeMetrics.astar += 1;
        else if (hit.mode === 'segmented') ctx.routeMetrics.segmented += 1;
        else ctx.routeMetrics.fallback += 1;
      } else {
        const routed = sharedRoutedPath(points.a, points.b, picked.obstacles);
        let chosenPoints = Array.isArray(routed.points) ? routed.points : null;
        let chosenMode = routed.mode;
        if (chosenPoints && chosenPoints.length >= 2) {
          const altPoints = sharedOrthogonalLanePoints(points.a, points.b, picked.obstacles);
          const crossCurrent = countPolylineCrossings(chosenPoints, placedPolylines);
          const crossAlt = countPolylineCrossings(altPoints, placedPolylines);
          if (altPoints.length >= 2 && crossAlt + 1 < crossCurrent) {
            chosenPoints = altPoints;
            if (chosenMode === 'fallback') chosenMode = 'segmented';
          }
        }
        d = chosenPoints && chosenPoints.length >= 2 ? toPath(chosenPoints) : routed.d;
        if (chosenPoints && chosenPoints.length >= 2) placedPolylines.push(chosenPoints);
        ctx.routeMetrics.cacheMiss += 1;
        if (chosenMode === 'astar') ctx.routeMetrics.astar += 1;
        else if (chosenMode === 'segmented') ctx.routeMetrics.segmented += 1;
        else ctx.routeMetrics.fallback += 1;
        ctx.routeCache.set(edgeId, {
          d,
          points: chosenPoints || [],
          mode: chosenMode,
          routeKey: routeKeyWithDeps,
          deps: Array.from(picked.deps),
          from: e.from,
          to: e.to,
        });
      }
    }
    d = ensureEdgePath(d, points.a, points.b);

    const stroke = svgEl('path');
    stroke.setAttribute('d', d);
    stroke.setAttribute('fill', 'none');
    stroke.setAttribute('stroke', '#2f7bc2');
    stroke.setAttribute('stroke-width', '2.4');
    stroke.setAttribute('marker-end', 'url(#arrowHead)');
    stroke.setAttribute('class', 'edge-line');
    stroke.setAttribute('filter', 'url(#edgeGlow)');

    const hit = svgEl('path');
    hit.setAttribute('d', d);
    hit.setAttribute('fill', 'none');
    hit.setAttribute('stroke', 'transparent');
    hit.setAttribute('stroke-width', '14');
    hit.setAttribute('pointer-events', 'stroke');
    hit.setAttribute('class', 'edge-hit');
    hit.dataset.from = e.from;
    hit.dataset.to = e.to;
    hit.addEventListener('mouseenter', () => stroke.classList.add('active'));
    hit.addEventListener('mouseleave', () => stroke.classList.remove('active'));
    hit.addEventListener('click', (evt) => {
      evt.stopPropagation();
      const from = String(hit.dataset.from);
      const to = String(hit.dataset.to);
      ctx.routeCache.delete(`${from}->${to}`);
      ctx.store.unlink(from, to);
      ctx.onChange();
    });

    ctx.edgesSvg.append(stroke, hit);
  }
  if (!useFastRouting && ctx.routeCache.size) {
    for (const edgeId of ctx.routeCache.keys()) {
      if (!aliveEdgeIds.has(edgeId)) ctx.routeCache.delete(edgeId);
    }
  }
  if (!useFastRouting) ctx.reportRouteMetrics();

  if (ctx.linking) {
    const fromPorts = ctx.portByNodeId.get(ctx.linking.from);
    const a = ctx.portCenter(fromPorts?.outPort);
    if (a) {
      const bWorld = ctx.clientToWorld(ctx.lastClient.x, ctx.lastClient.y);
      const b = ctx.worldToDisplay(bWorld.x, bWorld.y);
      const ghost = svgEl('path');
      ghost.setAttribute('d', ensureEdgePath(orthogonalLanePath(a, b, []), a, b));
      ghost.setAttribute('fill', 'none');
      ghost.setAttribute('stroke', '#0b7d67');
      ghost.setAttribute('stroke-width', '2.2');
      ghost.setAttribute('stroke-dasharray', '6 4');
      ghost.setAttribute('class', 'edge-ghost');
      ctx.edgesSvg.append(ghost);
    }
  }
}

export {
  renderEdgesLayer,
};
