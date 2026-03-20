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
  renderGuides as renderGuidesImpl,
  renderMarquee as renderMarqueeImpl,
  onMarqueeMove as onMarqueeMoveImpl,
  onMarqueeEnd as onMarqueeEndImpl,
  renderMinimap as renderMinimapImpl,
} from './canvas_interactions.mjs';
import {
  el,
  rectIntersects,
} from './canvas_utils.mjs';
import {
  buildGraphShapeSig as geoBuildGraphShapeSig,
  edgeObstacleCandidates as geoEdgeObstacleCandidates,
  edgeObstacles as geoEdgeObstacles,
  edgePoints as geoEdgePoints,
  getGraphBounds as geoGetGraphBounds,
  getVisibleDisplayRect as geoGetVisibleDisplayRect,
  invalidateEdgesForNodes as geoInvalidateEdgesForNodes,
  portCenter as geoPortCenter,
} from './canvas_geometry.mjs';
import {
  alignSelected as wrapAlignSelected,
  applyAlignment as wrapApplyAlignment,
  fitToView as wrapFitToView,
  invalidateAllRoutes as wrapInvalidateAllRoutes,
  onMarqueeEnd as wrapOnMarqueeEnd,
  onMarqueeMove as wrapOnMarqueeMove,
  renderEdges as wrapRenderEdges,
  renderGuides as wrapRenderGuides,
  renderMarquee as wrapRenderMarquee,
  renderMinimap as wrapRenderMinimap,
  renderNodes as wrapRenderNodes,
  updateNodePositionsFast as wrapUpdateNodePositionsFast,
} from './canvas_wrappers.mjs';
import { fitWorldRectToViewport as vpFitWorldRectToViewport } from './canvas_viewport.mjs';
import { defineMethod } from './canvas_class_methods_support.mjs';

function installWorkflowCanvasRenderGeometryMethods(WorkflowCanvas) {
  const prototype = WorkflowCanvas.prototype;

  defineMethod(prototype, 'renderNodes', function renderNodes() {
    wrapRenderNodes(this, renderNodesLayer, NODE_W, NODE_H, el);
  });

  defineMethod(prototype, 'getVisibleDisplayRect', function getVisibleDisplayRect(extra = 0) {
    return geoGetVisibleDisplayRect(this, extra);
  });

  defineMethod(prototype, 'getGraphBounds', function getGraphBounds() {
    return geoGetGraphBounds(this, NODE_W, NODE_H);
  });

  defineMethod(prototype, 'fitToView', function fitToView(padding = 96) {
    return wrapFitToView(this, padding, () => this.getGraphBounds(), vpFitWorldRectToViewport, MIN_ZOOM, MAX_ZOOM);
  });

  defineMethod(prototype, 'updateNodePositionsFast', function updateNodePositionsFast(ids) {
    wrapUpdateNodePositionsFast(this, ids, updateNodePositionsFastLayer);
  });

  defineMethod(prototype, 'portCenter', function portCenter(portEl) {
    return geoPortCenter(this, portEl);
  });

  defineMethod(prototype, 'edgePoints', function edgePoints(fromId, toId) {
    return geoEdgePoints(this, fromId, toId);
  });

  defineMethod(prototype, 'edgeObstacles', function edgeObstacles(ignoreIds = []) {
    return geoEdgeObstacles(this, ignoreIds, NODE_W, NODE_H);
  });

  defineMethod(prototype, 'edgeObstacleCandidates', function edgeObstacleCandidates(ignoreIds = [], a, b) {
    return geoEdgeObstacleCandidates(this, ignoreIds, a, b, NODE_W, NODE_H, rectIntersects);
  });

  defineMethod(prototype, 'buildGraphShapeSig', function buildGraphShapeSig(graph) {
    return geoBuildGraphShapeSig(this, graph);
  });

  defineMethod(prototype, 'invalidateAllRoutes', function invalidateAllRoutes() {
    wrapInvalidateAllRoutes(this);
  });

  defineMethod(prototype, 'invalidateEdgesForNodes', function invalidateEdgesForNodes(nodeIds = []) {
    geoInvalidateEdgesForNodes(this, nodeIds);
  });

  defineMethod(prototype, 'renderEdges', function renderEdges() {
    wrapRenderEdges(this, renderEdgesLayer);
  });

  defineMethod(prototype, 'applyAlignment', function applyAlignment(x, y, nodeId) {
    return wrapApplyAlignment(this, x, y, nodeId, selApplyAlignment, NODE_W, NODE_H);
  });

  defineMethod(prototype, 'renderGuides', function renderGuides() {
    wrapRenderGuides(this, renderGuidesImpl, el);
  });

  defineMethod(prototype, 'renderMarquee', function renderMarquee() {
    wrapRenderMarquee(this, renderMarqueeImpl, el);
  });

  defineMethod(prototype, 'onMarqueeMove', function onMarqueeMove(evt) {
    wrapOnMarqueeMove(this, evt, onMarqueeMoveImpl);
  });

  defineMethod(prototype, 'onMarqueeEnd', function onMarqueeEnd(evt) {
    wrapOnMarqueeEnd(this, evt, onMarqueeEndImpl, NODE_W, NODE_H);
  });

  defineMethod(prototype, 'renderMinimap', function renderMinimap() {
    wrapRenderMinimap(this, renderMinimapImpl, NODE_W, NODE_H);
  });

  defineMethod(prototype, 'alignSelected', function alignSelected(mode) {
    return wrapAlignSelected(this, mode, selAlignSelected);
  });
}

export { installWorkflowCanvasRenderGeometryMethods };
