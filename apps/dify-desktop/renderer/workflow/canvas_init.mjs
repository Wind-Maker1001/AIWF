function initializeCanvasInstance(canvas, params = {}) {
  const {
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
  } = params;

  canvas.store = store;
  canvas.nodeCatalog = nodeCatalog;
  canvas.canvasWrap = canvasWrap;
  canvas.canvasSurface = canvasSurface || canvasWrap;
  canvas.nodesLayer = nodesLayer;
  canvas.guideLayer = guideLayer || null;
  canvas.minimapCanvas = minimapCanvas || null;
  canvas.edgesSvg = edgesSvg;
  canvas.onChange = onChange || (() => {});
  canvas.onWarn = onWarn || (() => {});
  canvas.onSelectionChange = onSelectionChange || (() => {});
  canvas.onEdgeSelect = onEdgeSelect || (() => {});

  canvas.drag = null;
  canvas.marquee = null;
  canvas.linking = null;
  canvas.touchPan = null;
  canvas.touchPinch = null;
  canvas.activePointers = new Map();
  canvas.lastClient = { x: 0, y: 0 };
  canvas.inputPortByNodeId = new Map();
  canvas.portByNodeId = new Map();
  canvas.nodeElById = new Map();

  canvas.snapEnabled = true;
  canvas.gridSize = 24;
  canvas.zoom = 1;

  canvas.worldMinX = -1200;
  canvas.worldMinY = -900;
  canvas.surfaceWidth = 3200;
  canvas.surfaceHeight = 2400;
  canvas.offsetX = -canvas.worldMinX;
  canvas.offsetY = -canvas.worldMinY;

  canvas.guides = { x: null, y: null };
  canvas.selectedIds = new Set();
  canvas.arrangePolicy = { preventOverlapOnAlign: false };
  canvas.routeCache = new Map();
  canvas.routeSig = "";
  canvas.routeMetrics = { edges: 0, astar: 0, segmented: 0, fallback: 0, cacheHit: 0, cacheMiss: 0 };
  canvas.routeAgg = {};
  canvas.lastRouteLogAt = 0;
  canvas.debugRouting = (window.location && window.location.search.includes("debug=1"))
    || window.localStorage.getItem("aiwf.workflow.debugRoutes") === "1";
  canvas.routeDebugEl = null;
  canvas.visibleNodeIds = new Set();
  canvas.cullMargin = 260;

  canvas._rafPending = false;
  canvas._rafNeedMinimap = false;
  canvas._minimapRafPending = false;
  canvas._edgeRafPending = false;
}

export { initializeCanvasInstance };
