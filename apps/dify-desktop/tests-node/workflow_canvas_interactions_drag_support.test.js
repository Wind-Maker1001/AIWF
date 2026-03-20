const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadCanvasDragModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/canvas_interactions_drag.mjs")).href;
  return import(file);
}

test("workflow canvas drag helpers render marquee and select intersecting nodes", async () => {
  const { renderMarquee, onMarqueeEnd } = await loadCanvasDragModule();
  const guideLayer = {
    children: [],
    append(node) { this.children.push(node); },
  };
  const ctx = {
    guideLayer,
    marquee: { x0: 10, y0: 20, x1: 30, y1: 50 },
    store: {
      state: {
        graph: {
          nodes: [
            { id: "n1", x: 10, y: 10 },
            { id: "n2", x: 200, y: 200 },
          ],
        },
      },
    },
    worldToDisplay: (x, y) => ({ x, y }),
    zoom: 1,
    setSelectedIds: (ids) => { ctx.selected = ids; },
    render: () => { ctx.rendered = true; },
  };
  const createEl = (tag, className) => ({ tag, className, style: {} });

  renderMarquee(ctx, createEl);
  assert.equal(guideLayer.children.length, 1);
  assert.equal(guideLayer.children[0].style.width, "20px");

  onMarqueeEnd(ctx, 100, 60, null);
  assert.deepEqual(ctx.selected, ["n1"]);
  assert.equal(ctx.rendered, true);
});

test("workflow canvas drag helpers move selected nodes and finish drag", async () => {
  const { onDragMove, onDragEnd } = await loadCanvasDragModule();
  const moves = [];
  const ctx = {
    drag: {
      id: "n1",
      offsetX: 0,
      offsetY: 0,
      selected: ["n1", "n2"],
      startMap: new Map([
        ["n1", { x: 10, y: 10 }],
        ["n2", { x: 20, y: 20 }],
      ]),
    },
    clientToWorld: (x, y) => ({ x, y }),
    applyAlignment: (x, y) => ({ x, y }),
    gridSize: 10,
    snapEnabled: true,
    store: { moveNode: (id, x, y) => moves.push({ id, x, y }) },
    offsetX: 0,
    offsetY: 0,
    ensureViewport: () => {},
    requestRender: () => { ctx.renderRequested = true; },
    updateNodePositionsFast: (ids) => { ctx.fastIds = ids; },
    requestEdgeFrame: () => { ctx.edgeRequested = true; },
    guides: { x: 1, y: 2 },
    invalidateEdgesForNodes: (ids) => { ctx.invalidated = ids; },
    render: () => { ctx.rendered = true; },
  };

  onDragMove(ctx, { clientX: 35, clientY: 45 });
  assert.deepEqual(moves, [
    { id: "n1", x: 40, y: 50 },
    { id: "n2", x: 50, y: 60 },
  ]);
  assert.deepEqual(ctx.fastIds, ["n1", "n2"]);
  assert.equal(ctx.edgeRequested, true);

  onDragEnd(ctx);
  assert.deepEqual(ctx.invalidated, ["n1", "n2"]);
  assert.equal(ctx.drag, null);
  assert.equal(ctx.guides.x, null);
  assert.equal(ctx.guides.y, null);
  assert.equal(ctx.rendered, true);
});
