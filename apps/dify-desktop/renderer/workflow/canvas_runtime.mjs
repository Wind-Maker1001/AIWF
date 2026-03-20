function getRouteMetrics(canvas) {
  const metrics = canvas.routeMetrics || { edges: 0, astar: 0, segmented: 0, fallback: 0, cacheHit: 0, cacheMiss: 0 };
  const solved = Number(metrics.astar || 0) + Number(metrics.segmented || 0) + Number(metrics.fallback || 0);
  const ratio = solved > 0 ? Number((Number(metrics.fallback || 0) / solved).toFixed(4)) : 0;
  return { ...metrics, solved, fallback_ratio: ratio };
}

function requestRender(canvas, withMinimap = false) {
  if (withMinimap) canvas._rafNeedMinimap = true;
  if (canvas._rafPending) return;
  canvas._rafPending = true;
  window.requestAnimationFrame(() => {
    canvas._rafPending = false;
    const doMinimap = canvas._rafNeedMinimap;
    canvas._rafNeedMinimap = false;
    canvas.renderCore(doMinimap);
  });
}

function requestMinimap(canvas) {
  if (canvas._minimapRafPending) return;
  canvas._minimapRafPending = true;
  window.requestAnimationFrame(() => {
    canvas._minimapRafPending = false;
    canvas.renderMinimap();
  });
}

function requestEdgeFrame(canvas) {
  if (canvas._edgeRafPending) return;
  canvas._edgeRafPending = true;
  window.requestAnimationFrame(() => {
    canvas._edgeRafPending = false;
    canvas.renderEdges();
    canvas.renderGuides();
    canvas.renderMarquee();
    canvas.renderRouteDebug();
  });
}

function renderRouteDebug(canvas, createElement) {
  if (!canvas.debugRouting) {
    if (canvas.routeDebugEl && canvas.routeDebugEl.parentNode) canvas.routeDebugEl.parentNode.removeChild(canvas.routeDebugEl);
    canvas.routeDebugEl = null;
    return;
  }
  if (!canvas.routeDebugEl) {
    canvas.routeDebugEl = createElement("div", "route-debug");
    canvas.canvasWrap.append(canvas.routeDebugEl);
  }
  const metrics = canvas.routeMetrics;
  canvas.routeDebugEl.textContent = `route: edges=${metrics.edges} astar=${metrics.astar} seg=${metrics.segmented} fallback=${metrics.fallback} cache(hit/miss)=${metrics.cacheHit}/${metrics.cacheMiss}`;
}

function reportRouteMetrics(canvas) {
  const workflowId = String(canvas.store?.state?.graph?.workflow_id || "custom");
  const metrics = canvas.routeMetrics || { edges: 0, astar: 0, segmented: 0, fallback: 0, cacheHit: 0, cacheMiss: 0 };
  if (!canvas.routeAgg[workflowId]) {
    canvas.routeAgg[workflowId] = { renders: 0, edges: 0, astar: 0, segmented: 0, fallback: 0, cacheHit: 0, cacheMiss: 0 };
  }
  const state = canvas.routeAgg[workflowId];
  state.renders += 1;
  state.edges += Number(metrics.edges || 0);
  state.astar += Number(metrics.astar || 0);
  state.segmented += Number(metrics.segmented || 0);
  state.fallback += Number(metrics.fallback || 0);
  state.cacheHit += Number(metrics.cacheHit || 0);
  state.cacheMiss += Number(metrics.cacheMiss || 0);
  const now = Date.now();
  if (now - canvas.lastRouteLogAt < 8000) return;
  canvas.lastRouteLogAt = now;
  const totalSolved = state.astar + state.segmented + state.fallback;
  const fallbackRatio = totalSolved > 0 ? (state.fallback / totalSolved) : 0;
  const payload = {
    workflow_id: workflowId,
    renders: state.renders,
    solved: totalSolved,
    astar: state.astar,
    segmented: state.segmented,
    fallback: state.fallback,
    fallback_ratio: Number(fallbackRatio.toFixed(4)),
    cache_hit: state.cacheHit,
    cache_miss: state.cacheMiss,
  };
  console.info(`[aiwf.route] wf=${workflowId} renders=${state.renders} solved=${totalSolved} astar=${state.astar} segmented=${state.segmented} fallback=${state.fallback} fallback_ratio=${fallbackRatio.toFixed(3)} cache=${state.cacheHit}/${state.cacheMiss}`);
  try {
    if (window?.aiwfDesktop?.logRouteMetrics) {
      window.aiwfDesktop.logRouteMetrics(payload).catch(() => {});
    }
  } catch {}
}

export {
  getRouteMetrics,
  renderRouteDebug,
  reportRouteMetrics,
  requestEdgeFrame,
  requestMinimap,
  requestRender,
};
