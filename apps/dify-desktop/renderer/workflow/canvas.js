import {
  clientToSurface as vpClientToSurface,
  clientToWorld as vpClientToWorld,
  worldToDisplay as vpWorldToDisplay,
  ensureViewport as vpEnsureViewport,
  ensureMinimapBitmap as vpEnsureMinimapBitmap,
  clampZoomValue as vpClampZoomValue,
  placeWorldPointAtClient as vpPlaceWorldPointAtClient,
  fitWorldRectToViewport as vpFitWorldRectToViewport,
} from './canvas_viewport.mjs';
import {
  applyAlignment as selApplyAlignment,
  alignSelected as selAlignSelected,
} from './canvas_selection.mjs';
import {
  NODE_W,
  NODE_H,
  MIN_ZOOM,
  MAX_ZOOM,
} from './canvas_consts.mjs';
import { renderEdgesLayer } from './canvas_edges.mjs';
import {
  renderNodesLayer,
  updateNodePositionsFastLayer,
} from './canvas_nodes.mjs';
import {
  bindCanvasEvents as bindCanvasEventsImpl,
  bindMinimapEvents as bindMinimapEventsImpl,
  renderGuides as renderGuidesImpl,
  renderMarquee as renderMarqueeImpl,
  onMarqueeMove as onMarqueeMoveImpl,
  onMarqueeEnd as onMarqueeEndImpl,
  renderMinimap as renderMinimapImpl,
  onDragStart as onDragStartImpl,
  onDragMove as onDragMoveImpl,
  onDragEnd as onDragEndImpl,
  onLinkStart as onLinkStartImpl,
  linkErrorMessage as linkErrorMessageImpl,
  finishLinkByEvent as finishLinkByEventImpl,
  markLinkTargets as markLinkTargetsImpl,
  clearLinkTargets as clearLinkTargetsImpl,
} from './canvas_interactions.mjs';

function el(tag, cls, text) {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text !== undefined) n.textContent = text;
  return n;
}

const EXPAND_PAD = 260;
const VIEW_PAD = 80;

function rectIntersects(a, b) {
  return !(
    a.x + a.w < b.x ||
    b.x + b.w < a.x ||
    a.y + a.h < b.y ||
    b.y + b.h < a.y
  );
}

export class WorkflowCanvas {
  constructor({
    store,
    nodeCatalog,
    canvasWrap,
    canvasSurface,
    nodesLayer,
    guideLayer,
    minimapCanvas,
    edgesSvg,
    onChange,
    onWarn,
    onSelectionChange,
    onEdgeSelect,
  }) {
    this.store = store;
    this.nodeCatalog = nodeCatalog;
    this.canvasWrap = canvasWrap;
    this.canvasSurface = canvasSurface || canvasWrap;
    this.nodesLayer = nodesLayer;
    this.guideLayer = guideLayer || null;
    this.minimapCanvas = minimapCanvas || null;
    this.edgesSvg = edgesSvg;
    this.onChange = onChange || (() => {});
    this.onWarn = onWarn || (() => {});
    this.onSelectionChange = onSelectionChange || (() => {});
    this.onEdgeSelect = onEdgeSelect || (() => {});

    this.drag = null;
    this.marquee = null;
    this.linking = null;
    this.touchPan = null;
    this.touchPinch = null;
    this.activePointers = new Map();
    this.lastClient = { x: 0, y: 0 };
    this.inputPortByNodeId = new Map();
    this.portByNodeId = new Map();
    this.nodeElById = new Map();

    this.snapEnabled = true;
    this.gridSize = 24;
    this.zoom = 1;

    this.worldMinX = -1200;
    this.worldMinY = -900;
    this.surfaceWidth = 3200;
    this.surfaceHeight = 2400;
    this.offsetX = -this.worldMinX;
    this.offsetY = -this.worldMinY;

    this.guides = { x: null, y: null };
    this.selectedIds = new Set();
    this.arrangePolicy = { preventOverlapOnAlign: false };
    this.routeCache = new Map();
    this.routeSig = "";
    this.routeMetrics = { edges: 0, astar: 0, segmented: 0, fallback: 0, cacheHit: 0, cacheMiss: 0 };
    this.routeAgg = {};
    this.lastRouteLogAt = 0;
    this.debugRouting = (window.location && window.location.search.includes("debug=1"))
      || window.localStorage.getItem("aiwf.workflow.debugRoutes") === "1";
    this.routeDebugEl = null;
    this.visibleNodeIds = new Set();
    this.cullMargin = 260;

    this._rafPending = false;
    this._rafNeedMinimap = false;
    this._minimapRafPending = false;
    this._edgeRafPending = false;

    this.bindCanvasEvents();
    this.bindMinimapEvents();
  }

  setSnap(enabled) {
    this.snapEnabled = !!enabled;
  }

  setZoom(zoom, focusClient = null) {
    const nextZoom = vpClampZoomValue(zoom, MIN_ZOOM, MAX_ZOOM);
    const rect = focusClient && this.canvasWrap ? this.canvasWrap.getBoundingClientRect() : null;
    const hasFocus = rect && Number.isFinite(focusClient?.clientX) && Number.isFinite(focusClient?.clientY);
    const worldFocus = hasFocus
      ? {
        x: Number.isFinite(focusClient?.anchorWorld?.x) ? Number(focusClient.anchorWorld.x) : this.clientToWorld(focusClient.clientX, focusClient.clientY).x,
        y: Number.isFinite(focusClient?.anchorWorld?.y) ? Number(focusClient.anchorWorld.y) : this.clientToWorld(focusClient.clientX, focusClient.clientY).y,
      }
      : null;
    if (Math.abs(nextZoom - this.zoom) < 0.0001) {
      if (worldFocus) {
        vpPlaceWorldPointAtClient(this, worldFocus.x, worldFocus.y, focusClient.clientX, focusClient.clientY);
        this.requestRender(true);
      }
      return;
    }
    this.zoom = nextZoom;
    this.invalidateAllRoutes();
    this.renderCore(false);
    if (worldFocus && rect) {
      vpPlaceWorldPointAtClient(this, worldFocus.x, worldFocus.y, focusClient.clientX, focusClient.clientY);
    }
    this.requestRender(true);
  }

  getZoom() {
    return this.zoom;
  }

  getRouteMetrics() {
    const m = this.routeMetrics || { edges: 0, astar: 0, segmented: 0, fallback: 0, cacheHit: 0, cacheMiss: 0 };
    const solved = Number(m.astar || 0) + Number(m.segmented || 0) + Number(m.fallback || 0);
    const ratio = solved > 0 ? Number((Number(m.fallback || 0) / solved).toFixed(4)) : 0;
    return { ...m, solved, fallback_ratio: ratio };
  }

  catalogName(type) {
    const it = this.nodeCatalog.find((x) => x.type === type);
    return it ? it.name : type;
  }

  clearSelection() {
    const changed = this.selectedIds.size > 0;
    this.selectedIds.clear();
    if (changed) this.onSelectionChange(this.getSelectedIds());
  }

  selectOne(id) {
    const prev = this.getSelectedIds().join("|");
    this.selectedIds.clear();
    if (id) this.selectedIds.add(id);
    if (prev !== this.getSelectedIds().join("|")) this.onSelectionChange(this.getSelectedIds());
  }

  toggleSelection(id) {
    if (!id) return;
    const prev = this.getSelectedIds().join("|");
    if (this.selectedIds.has(id)) this.selectedIds.delete(id);
    else this.selectedIds.add(id);
    if (prev !== this.getSelectedIds().join("|")) this.onSelectionChange(this.getSelectedIds());
  }

  isSelected(id) {
    return this.selectedIds.has(id);
  }

  getSelectedIds() {
    return Array.from(this.selectedIds);
  }

  setSelectedIds(ids = []) {
    const prev = this.getSelectedIds().join("|");
    this.selectedIds.clear();
    for (const id of ids || []) {
      const sid = String(id || "");
      if (sid) this.selectedIds.add(sid);
    }
    if (prev !== this.getSelectedIds().join("|")) this.onSelectionChange(this.getSelectedIds());
  }

  setArrangePolicy(policy = {}) {
    this.arrangePolicy = {
      ...this.arrangePolicy,
      ...(policy || {}),
    };
  }

  clientToSurface(clientX, clientY) {
    return vpClientToSurface(this, clientX, clientY);
  }

  clientToWorld(clientX, clientY) {
    return vpClientToWorld(this, clientX, clientY);
  }

  worldToDisplay(x, y) {
    return vpWorldToDisplay(this, x, y);
  }

  requestRender(withMinimap = false) {
    if (withMinimap) this._rafNeedMinimap = true;
    if (this._rafPending) return;
    this._rafPending = true;
    window.requestAnimationFrame(() => {
      this._rafPending = false;
      const doMinimap = this._rafNeedMinimap;
      this._rafNeedMinimap = false;
      this.renderCore(doMinimap);
    });
  }

  requestMinimap() {
    if (this._minimapRafPending) return;
    this._minimapRafPending = true;
    window.requestAnimationFrame(() => {
      this._minimapRafPending = false;
      this.renderMinimap();
    });
  }

  requestEdgeFrame() {
    if (this._edgeRafPending) return;
    this._edgeRafPending = true;
    window.requestAnimationFrame(() => {
      this._edgeRafPending = false;
      this.renderEdges();
      this.renderGuides();
      this.renderMarquee();
      this.renderRouteDebug();
    });
  }

  bindCanvasEvents() {
    bindCanvasEventsImpl(this);
  }

  bindMinimapEvents() {
    bindMinimapEventsImpl(this);
  }

  ensureViewport(extraPoint) {
    vpEnsureViewport(this, extraPoint, EXPAND_PAD, VIEW_PAD, NODE_W, NODE_H);
  }

  ensureMinimapBitmap() {
    return vpEnsureMinimapBitmap(this);
  }

  render() {
    this.renderCore(true);
  }

  renderCore(withMinimap) {
    this.ensureViewport();
    this.renderNodes();
    this.renderEdges();
    this.renderRouteDebug();
    this.renderGuides();
    this.renderMarquee();
    if (withMinimap) this.renderMinimap();
  }

  renderRouteDebug() {
    if (!this.debugRouting) {
      if (this.routeDebugEl && this.routeDebugEl.parentNode) this.routeDebugEl.parentNode.removeChild(this.routeDebugEl);
      this.routeDebugEl = null;
      return;
    }
    if (!this.routeDebugEl) {
      this.routeDebugEl = el("div", "route-debug");
      this.canvasWrap.append(this.routeDebugEl);
    }
    const m = this.routeMetrics;
    this.routeDebugEl.textContent = `route: edges=${m.edges} astar=${m.astar} seg=${m.segmented} fallback=${m.fallback} cache(hit/miss)=${m.cacheHit}/${m.cacheMiss}`;
  }

  reportRouteMetrics() {
    const wf = String(this.store?.state?.graph?.workflow_id || "custom");
    const m = this.routeMetrics || { edges: 0, astar: 0, segmented: 0, fallback: 0, cacheHit: 0, cacheMiss: 0 };
    if (!this.routeAgg[wf]) {
      this.routeAgg[wf] = { renders: 0, edges: 0, astar: 0, segmented: 0, fallback: 0, cacheHit: 0, cacheMiss: 0 };
    }
    const s = this.routeAgg[wf];
    s.renders += 1;
    s.edges += Number(m.edges || 0);
    s.astar += Number(m.astar || 0);
    s.segmented += Number(m.segmented || 0);
    s.fallback += Number(m.fallback || 0);
    s.cacheHit += Number(m.cacheHit || 0);
    s.cacheMiss += Number(m.cacheMiss || 0);
    const now = Date.now();
    if (now - this.lastRouteLogAt >= 8000) {
      this.lastRouteLogAt = now;
      const totalSolved = s.astar + s.segmented + s.fallback;
      const fbRatio = totalSolved > 0 ? (s.fallback / totalSolved) : 0;
      const payload = {
        workflow_id: wf,
        renders: s.renders,
        solved: totalSolved,
        astar: s.astar,
        segmented: s.segmented,
        fallback: s.fallback,
        fallback_ratio: Number(fbRatio.toFixed(4)),
        cache_hit: s.cacheHit,
        cache_miss: s.cacheMiss,
      };
      console.info(`[aiwf.route] wf=${wf} renders=${s.renders} solved=${totalSolved} astar=${s.astar} segmented=${s.segmented} fallback=${s.fallback} fallback_ratio=${fbRatio.toFixed(3)} cache=${s.cacheHit}/${s.cacheMiss}`);
      try {
        if (window?.aiwfDesktop?.logRouteMetrics) {
          window.aiwfDesktop.logRouteMetrics(payload).catch(() => {});
        }
      } catch {}
    }
  }

  renderNodes() {
    renderNodesLayer(this, NODE_W, NODE_H, el);
  }

  getVisibleDisplayRect(extra = 0) {
    const m = Math.max(0, Number(extra || 0));
    return {
      x: this.canvasWrap.scrollLeft - m,
      y: this.canvasWrap.scrollTop - m,
      w: this.canvasWrap.clientWidth + m * 2,
      h: this.canvasWrap.clientHeight + m * 2,
    };
  }

  getGraphBounds() {
    const nodes = Array.isArray(this.store?.state?.graph?.nodes) ? this.store.state.graph.nodes : [];
    if (!nodes.length) return null;
    let minLeft = Number.POSITIVE_INFINITY;
    let minTop = Number.POSITIVE_INFINITY;
    let maxRight = Number.NEGATIVE_INFINITY;
    let maxBottom = Number.NEGATIVE_INFINITY;
    nodes.forEach((node) => {
      const x = Number(node?.x || 0);
      const y = Number(node?.y || 0);
      minLeft = Math.min(minLeft, x);
      minTop = Math.min(minTop, y);
      maxRight = Math.max(maxRight, x + NODE_W);
      maxBottom = Math.max(maxBottom, y + NODE_H);
    });
    if (!Number.isFinite(minLeft) || !Number.isFinite(minTop) || !Number.isFinite(maxRight) || !Number.isFinite(maxBottom)) return null;
    return { minLeft, minTop, maxRight, maxBottom };
  }

  fitToView(padding = 96) {
    const bounds = this.getGraphBounds();
    if (!bounds || this.canvasWrap.clientWidth <= 0 || this.canvasWrap.clientHeight <= 0) return false;
    const fit = vpFitWorldRectToViewport(bounds, this.canvasWrap.clientWidth, this.canvasWrap.clientHeight, padding, MIN_ZOOM, MAX_ZOOM);
    const rect = this.canvasWrap.getBoundingClientRect();
    this.setZoom(fit.zoom, {
      clientX: rect.left + this.canvasWrap.clientWidth / 2,
      clientY: rect.top + this.canvasWrap.clientHeight / 2,
      anchorWorld: { x: fit.centerX, y: fit.centerY },
    });
    return true;
  }

  updateNodePositionsFast(ids) {
    updateNodePositionsFastLayer(this, ids);
  }

  portCenter(portEl) {
    if (!portEl) return null;
    const wrapRect = this.canvasWrap.getBoundingClientRect();
    const rect = portEl.getBoundingClientRect();
    return {
      x: rect.left - wrapRect.left + this.canvasWrap.scrollLeft + rect.width / 2,
      y: rect.top - wrapRect.top + this.canvasWrap.scrollTop + rect.height / 2,
    };
  }

  edgePoints(fromId, toId) {
    const fromPorts = this.portByNodeId.get(fromId);
    const toPorts = this.portByNodeId.get(toId);
    const a = this.portCenter(fromPorts?.outPort);
    const b = this.portCenter(toPorts?.inPort);
    if (!a || !b) return null;
    return { a, b };
  }

  edgeObstacles(ignoreIds = []) {
    const ignore = new Set(ignoreIds);
    const out = [];
    for (const n of this.store.state.graph.nodes) {
      if (ignore.has(n.id)) continue;
      const p = this.worldToDisplay(Number(n.x || 0), Number(n.y || 0));
      out.push({ x: p.x, y: p.y, w: NODE_W * this.zoom, h: NODE_H * this.zoom });
    }
    return out;
  }

  edgeObstacleCandidates(ignoreIds = [], a, b) {
    const ignore = new Set(ignoreIds);
    const out = [];
    const deps = new Set();
    const margin = 220 * this.zoom;
    const x1 = Math.min(a.x, b.x) - margin;
    const y1 = Math.min(a.y, b.y) - margin;
    const x2 = Math.max(a.x, b.x) + margin;
    const y2 = Math.max(a.y, b.y) + margin;
    const corridor = { x: x1, y: y1, w: x2 - x1, h: y2 - y1 };
    for (const n of this.store.state.graph.nodes) {
      if (ignore.has(n.id)) continue;
      const p = this.worldToDisplay(Number(n.x || 0), Number(n.y || 0));
      const box = { x: p.x, y: p.y, w: NODE_W * this.zoom, h: NODE_H * this.zoom };
      if (!rectIntersects(corridor, box)) continue;
      out.push(box);
      deps.add(n.id);
    }
    return { obstacles: out, deps };
  }

  buildGraphShapeSig(graph) {
    const nodes = graph.nodes
      .map((n) => String(n.id))
      .sort()
      .join("|");
    const edges = graph.edges
      .map((e) => `${e.from}->${e.to}`)
      .sort()
      .join("|");
    return `${this.zoom.toFixed(3)}|${nodes}|${edges}`;
  }

  invalidateAllRoutes() {
    this.routeCache.clear();
  }

  invalidateEdgesForNodes(nodeIds = []) {
    const dirty = new Set((nodeIds || []).map((x) => String(x)));
    if (!dirty.size || !this.routeCache.size) return;
    for (const [edgeId, entry] of this.routeCache.entries()) {
      const deps = Array.isArray(entry?.deps) ? entry.deps : [];
      if (
        dirty.has(String(entry?.from || "")) ||
        dirty.has(String(entry?.to || "")) ||
        deps.some((id) => dirty.has(String(id)))
      ) {
        this.routeCache.delete(edgeId);
      }
    }
  }

  renderEdges() {
    renderEdgesLayer(this);
  }

  applyAlignment(x, y, nodeId) {
    return selApplyAlignment(this, x, y, nodeId, NODE_W, NODE_H);
  }

  renderGuides() {
    renderGuidesImpl(this, el);
  }

  renderMarquee() {
    renderMarqueeImpl(this, el);
  }

  onMarqueeMove(evt) {
    onMarqueeMoveImpl(this, evt);
  }

  onMarqueeEnd(evt) {
    onMarqueeEndImpl(this, NODE_W, NODE_H, evt);
  }

  renderMinimap() {
    renderMinimapImpl(this, NODE_W, NODE_H);
  }

  alignSelected(mode) {
    return selAlignSelected(this, mode);
  }

  onDragStart(evt, nodeId) {
    onDragStartImpl(this, evt, nodeId);
  }

  onDragMove(evt) {
    onDragMoveImpl(this, evt);
  }

  onDragEnd() {
    onDragEndImpl(this);
  }

  onLinkStart(evt, fromId) {
    onLinkStartImpl(this, evt, fromId);
  }

  linkErrorMessage(reason) {
    return linkErrorMessageImpl(reason);
  }

  finishLinkByEvent(evt) {
    finishLinkByEventImpl(this, evt);
  }

  markLinkTargets() {
    markLinkTargetsImpl(this);
  }

  clearLinkTargets() {
    clearLinkTargetsImpl(this);
  }
}




