import {
  clientToSurface as vpClientToSurface,
  clientToWorld as vpClientToWorld,
  worldToDisplay as vpWorldToDisplay,
  ensureViewport as vpEnsureViewport,
  ensureMinimapBitmap as vpEnsureMinimapBitmap,
  clampZoomValue as vpClampZoomValue,
  placeWorldPointAtClient as vpPlaceWorldPointAtClient,
} from './canvas_viewport.mjs';
import {
  NODE_W,
  NODE_H,
  MIN_ZOOM,
  MAX_ZOOM,
} from './canvas_consts.mjs';
import {
  bindCanvasEvents as bindCanvasEventsImpl,
  bindMinimapEvents as bindMinimapEventsImpl,
} from './canvas_interactions.mjs';
import {
  el,
  EXPAND_PAD,
  VIEW_PAD,
} from './canvas_utils.mjs';
import {
  getRouteMetrics as runtimeGetRouteMetrics,
  renderRouteDebug as runtimeRenderRouteDebug,
  reportRouteMetrics as runtimeReportRouteMetrics,
  requestEdgeFrame as runtimeRequestEdgeFrame,
  requestMinimap as runtimeRequestMinimap,
  requestRender as runtimeRequestRender,
} from './canvas_runtime.mjs';
import {
  bindCanvasEvents as wrapBindCanvasEvents,
  bindMinimapEvents as wrapBindMinimapEvents,
  clientToSurface as wrapClientToSurface,
  clientToWorld as wrapClientToWorld,
  ensureMinimapBitmap as wrapEnsureMinimapBitmap,
  ensureViewport as wrapEnsureViewport,
  worldToDisplay as wrapWorldToDisplay,
} from './canvas_wrappers.mjs';
import {
  getZoom as viewGetZoom,
  render as viewRender,
  renderCore as viewRenderCore,
  setSnap as viewSetSnap,
  setZoom as viewSetZoom,
} from './canvas_view.mjs';
import { defineMethod } from './canvas_class_methods_support.mjs';

function installWorkflowCanvasViewRuntimeMethods(WorkflowCanvas) {
  const prototype = WorkflowCanvas.prototype;

  defineMethod(prototype, 'setSnap', function setSnap(enabled) {
    viewSetSnap(this, enabled);
  });

  defineMethod(prototype, 'setZoom', function setZoom(zoom, focusClient = null) {
    viewSetZoom(this, zoom, focusClient, vpClampZoomValue, MIN_ZOOM, MAX_ZOOM, vpPlaceWorldPointAtClient);
  });

  defineMethod(prototype, 'getZoom', function getZoom() {
    return viewGetZoom(this);
  });

  defineMethod(prototype, 'getRouteMetrics', function getRouteMetrics() {
    return runtimeGetRouteMetrics(this);
  });

  defineMethod(prototype, 'clientToSurface', function clientToSurface(clientX, clientY) {
    return wrapClientToSurface(this, clientX, clientY, vpClientToSurface);
  });

  defineMethod(prototype, 'clientToWorld', function clientToWorld(clientX, clientY) {
    return wrapClientToWorld(this, clientX, clientY, vpClientToWorld);
  });

  defineMethod(prototype, 'worldToDisplay', function worldToDisplay(x, y) {
    return wrapWorldToDisplay(this, x, y, vpWorldToDisplay);
  });

  defineMethod(prototype, 'requestRender', function requestRender(withMinimap = false) {
    runtimeRequestRender(this, withMinimap);
  });

  defineMethod(prototype, 'requestMinimap', function requestMinimap() {
    runtimeRequestMinimap(this);
  });

  defineMethod(prototype, 'requestEdgeFrame', function requestEdgeFrame() {
    runtimeRequestEdgeFrame(this);
  });

  defineMethod(prototype, 'bindCanvasEvents', function bindCanvasEvents() {
    wrapBindCanvasEvents(this, bindCanvasEventsImpl);
  });

  defineMethod(prototype, 'bindMinimapEvents', function bindMinimapEvents() {
    wrapBindMinimapEvents(this, bindMinimapEventsImpl);
  });

  defineMethod(prototype, 'ensureViewport', function ensureViewport(extraPoint) {
    wrapEnsureViewport(this, extraPoint, vpEnsureViewport, EXPAND_PAD, VIEW_PAD, NODE_W, NODE_H);
  });

  defineMethod(prototype, 'ensureMinimapBitmap', function ensureMinimapBitmap() {
    return wrapEnsureMinimapBitmap(this, vpEnsureMinimapBitmap);
  });

  defineMethod(prototype, 'render', function render() {
    viewRender(this);
  });

  defineMethod(prototype, 'renderCore', function renderCore(withMinimap) {
    viewRenderCore(this, withMinimap);
  });

  defineMethod(prototype, 'renderRouteDebug', function renderRouteDebug() {
    runtimeRenderRouteDebug(this, el);
  });

  defineMethod(prototype, 'reportRouteMetrics', function reportRouteMetrics() {
    runtimeReportRouteMetrics(this);
  });
}

export { installWorkflowCanvasViewRuntimeMethods };
