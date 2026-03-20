const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadCanvasTouchModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/canvas_interactions_touch.mjs")).href;
  return import(file);
}

async function loadCanvasLinkModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/canvas_interactions_link.mjs")).href;
  return import(file);
}

test("workflow canvas touch helpers handle snapping pointer storage pan and minimap jump", async () => {
  const {
    snap,
    isTouchLike,
    isPrimaryPointer,
    storePointer,
    dropPointer,
    updateTouchPan,
    jumpMinimap,
  } = await loadCanvasTouchModule();

  assert.equal(snap(13, 10, false), 13);
  assert.equal(snap(13, 10, true), 10);
  assert.equal(isTouchLike({ pointerType: "touch" }), true);
  assert.equal(isPrimaryPointer({ pointerType: "mouse", button: 0 }), true);

  const ctx = {
    activePointers: new Map(),
    touchPan: {
      pointerId: 1,
      startClientX: 100,
      startClientY: 100,
      startScrollLeft: 50,
      startScrollTop: 40,
      moved: false,
    },
    canvasWrap: { scrollLeft: 0, scrollTop: 0, clientWidth: 200, clientHeight: 100 },
    requestMinimap: () => { ctx.minimapRequested = true; },
    requestEdgeFrame: () => { ctx.edgeRequested = true; },
    minimapCanvas: {
      getBoundingClientRect: () => ({ left: 0, top: 0, width: 100, height: 50 }),
    },
    surfaceWidth: 1000,
    surfaceHeight: 500,
    offsetX: 0,
    offsetY: 0,
    zoom: 2,
    render: () => { ctx.rendered = true; },
  };

  storePointer(ctx, { pointerId: 1, clientX: 120, clientY: 130, pointerType: "touch" });
  assert.equal(ctx.activePointers.size, 1);
  assert.equal(updateTouchPan(ctx, { pointerId: 1, clientX: 110, clientY: 120 }), true);
  assert.equal(ctx.canvasWrap.scrollLeft, 40);
  assert.equal(ctx.canvasWrap.scrollTop, 20);
  jumpMinimap(ctx, 50, 25);
  assert.equal(ctx.rendered, true);
  dropPointer(ctx, { pointerId: 1 });
  assert.equal(ctx.activePointers.size, 0);
});

test("workflow canvas link helpers resolve targets and toggle link state", async () => {
  const {
    linkErrorMessage,
    resolveLinkTarget,
    markLinkTargets,
    clearLinkTargets,
  } = await loadCanvasLinkModule();

  assert.equal(linkErrorMessage("cycle"), "不允许形成环路（仅支持 DAG）");

  const directPort = {
    dataset: { nodeId: "n2" },
    closest: () => directPort,
  };
  assert.equal(resolveLinkTarget({ target: directPort, clientX: 0, clientY: 0 }, { elementFromPoint: () => null }), "n2");

  const calls = [];
  const ctx = {
    linking: { from: "n1" },
    inputPortByNodeId: new Map([
      ["n1", { classList: { add: (name) => calls.push(`skip-${name}`), remove: (name) => calls.push(`rm1-${name}`) } }],
      ["n2", { classList: { add: (name) => calls.push(`add-${name}`), remove: (name) => calls.push(`rm2-${name}`) } }],
    ]),
  };
  markLinkTargets(ctx);
  clearLinkTargets(ctx);

  assert.ok(calls.includes("add-target"));
  assert.ok(calls.includes("rm1-target"));
  assert.ok(calls.includes("rm2-target"));
});
