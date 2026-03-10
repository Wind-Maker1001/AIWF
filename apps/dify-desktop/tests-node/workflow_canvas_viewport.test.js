const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadViewport() {
  const modPath = path.resolve(__dirname, "..", "renderer", "workflow", "canvas_viewport.mjs");
  return import(pathToFileURL(modPath).href);
}

test("fitWorldRectToViewport clamps zoom and returns center", async () => {
  const viewport = await loadViewport();
  const out = viewport.fitWorldRectToViewport(
    { minLeft: 0, minTop: 0, maxRight: 1600, maxBottom: 1200 },
    800,
    600,
    80,
    0.45,
    3.25
  );
  assert.equal(Number(out.centerX.toFixed(2)), 800);
  assert.equal(Number(out.centerY.toFixed(2)), 600);
  assert.ok(out.zoom >= 0.45 && out.zoom <= 3.25);
});

test("placeWorldPointAtClient aligns scroll offsets to anchor world point", async () => {
  const viewport = await loadViewport();
  const ctx = {
    offsetX: 100,
    offsetY: 80,
    zoom: 2,
    canvasSurface: { scrollWidth: 4000, scrollHeight: 3000, clientWidth: 4000, clientHeight: 3000 },
    canvasWrap: {
      clientWidth: 800,
      clientHeight: 600,
      scrollLeft: 0,
      scrollTop: 0,
      getBoundingClientRect() {
        return { left: 10, top: 20 };
      },
    },
  };
  viewport.placeWorldPointAtClient(ctx, 300, 240, 210, 220);
  assert.equal(ctx.canvasWrap.scrollLeft, 600);
  assert.equal(ctx.canvasWrap.scrollTop, 440);
});
