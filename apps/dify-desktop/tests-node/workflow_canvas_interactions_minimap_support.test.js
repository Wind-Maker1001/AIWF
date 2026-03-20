const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadCanvasMinimapModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/canvas_interactions_minimap.mjs")).href;
  return import(file);
}

function createEventTarget() {
  const handlers = {};
  return {
    handlers,
    addEventListener(eventName, handler) {
      handlers[eventName] = handler;
    },
  };
}

test("workflow canvas minimap helpers bind pointer events and render viewport box", async () => {
  const { bindMinimapEvents, renderMinimap } = await loadCanvasMinimapModule();
  const calls = [];
  const minimapCanvas = createEventTarget();
  minimapCanvas.getContext = () => ({
    setTransform: () => {},
    clearRect: () => {},
    fillStyle: "",
    fillRect: (...args) => calls.push({ fillRect: args }),
    strokeStyle: "",
    lineWidth: 0,
    beginPath: () => {},
    moveTo: () => {},
    lineTo: () => {},
    stroke: () => {},
    strokeRect: (...args) => calls.push({ strokeRect: args }),
  });
  const ctx = {
    minimapCanvas,
    minimapPointerId: null,
    ensureMinimapBitmap: () => ({ cssW: 100, cssH: 50, dpr: 1 }),
    surfaceWidth: 1000,
    surfaceHeight: 500,
    store: { state: { graph: { nodes: [{ id: "n1", x: 10, y: 10 }], edges: [] } } },
    offsetX: 0,
    offsetY: 0,
    canvasWrap: { scrollLeft: 0, scrollTop: 0, clientWidth: 200, clientHeight: 100 },
    zoom: 2,
    render: () => { calls.push("render"); },
  };
  minimapCanvas.getBoundingClientRect = () => ({ left: 0, top: 0, width: 100, height: 50 });

  bindMinimapEvents(ctx);
  minimapCanvas.handlers.pointerdown({ button: 0, pointerId: 5, clientX: 10, clientY: 10 });
  minimapCanvas.handlers.pointermove({ pointerId: 5, clientX: 20, clientY: 20 });
  minimapCanvas.handlers.pointerup();
  assert.equal(ctx.minimapPointerId, null);

  renderMinimap(ctx, 100, 60);
  assert.ok(calls.some((item) => typeof item === "string" ? item === "render" : item.strokeRect));
});
