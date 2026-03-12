const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadCanvasViewUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/canvas-view-ui.js")).href;
  return import(file);
}

function createCanvasStub() {
  return {
    zoom: 1,
    snap: null,
    policy: null,
    fitOk: true,
    alignResult: { ok: true, moved: 2, total: 3 },
    setSnap(v) { this.snap = v; },
    setArrangePolicy(v) { this.policy = v; },
    setZoom(z) { this.zoom = z; },
    getZoom() { return this.zoom; },
    fitToView() { return this.fitOk; },
    alignSelected() { return this.alignResult; },
  };
}

test("workflow canvas view ui syncs zoom text and updates zoom", async () => {
  const { createWorkflowCanvasViewUi } = await loadCanvasViewUiModule();
  const calls = [];
  const canvas = createCanvasStub();
  const ui = createWorkflowCanvasViewUi({
    snapGrid: { checked: true },
    zoomText: { textContent: "" },
    canvasWrap: {
      clientWidth: 800,
      clientHeight: 600,
      getBoundingClientRect() {
        return { left: 10, top: 20 };
      },
    },
  }, {
    canvas,
    renderNodeConfigEditor: () => calls.push("node"),
    renderEdgeConfigEditor: () => calls.push("edge"),
    refreshOfflineBoundaryHint: () => calls.push("hint"),
  });

  ui.setZoom(1.35);

  assert.equal(canvas.snap, true);
  assert.deepEqual(canvas.policy, { preventOverlapOnAlign: false });
  assert.equal(canvas.zoom, 1.35);
  assert.deepEqual(calls, ["node", "edge", "hint"]);
});

test("workflow canvas view ui fits view and reports status", async () => {
  const { createWorkflowCanvasViewUi } = await loadCanvasViewUiModule();
  const statuses = [];
  const canvas = createCanvasStub();
  const ui = createWorkflowCanvasViewUi({
    snapGrid: { checked: false },
    zoomText: { textContent: "" },
    canvasWrap: {
      clientWidth: 800,
      clientHeight: 600,
      getBoundingClientRect() {
        return { left: 0, top: 0 };
      },
    },
  }, {
    canvas,
    setStatus: (text, ok) => statuses.push({ text, ok }),
  });

  ui.fitCanvasToView();
  canvas.fitOk = false;
  ui.fitCanvasToView();

  assert.deepEqual(statuses, [
    { text: "已适配当前流程视图", ok: true },
    { text: "当前没有可适配的节点", ok: false },
  ]);
});

test("workflow canvas view ui applies arrange feedback", async () => {
  const { createWorkflowCanvasViewUi } = await loadCanvasViewUiModule();
  const statuses = [];
  const canvas = createCanvasStub();
  const ui = createWorkflowCanvasViewUi({
    snapGrid: { checked: false },
    zoomText: { textContent: "" },
    canvasWrap: {
      clientWidth: 800,
      clientHeight: 600,
      getBoundingClientRect() {
        return { left: 0, top: 0 };
      },
    },
  }, {
    canvas,
    setStatus: (text, ok) => statuses.push({ text, ok }),
  });

  ui.applyArrange("left", "左对齐");
  canvas.alignResult = { ok: true, moved: 0, total: 3 };
  ui.applyArrange("top", "上对齐");

  assert.deepEqual(statuses, [
    { text: "左对齐: 已调整 2/3 个节点", ok: true },
    { text: "上对齐: 节点已处于目标布局", ok: true },
  ]);
});

test("workflow canvas view ui focuses node and scrolls it into view", async () => {
  const { createWorkflowCanvasViewUi } = await loadCanvasViewUiModule();
  const canvas = createCanvasStub();
  const selected = [];
  const scrollCalls = [];
  let renderCount = 0;
  const ui = createWorkflowCanvasViewUi({
    snapGrid: { checked: false },
    zoomText: { textContent: "" },
    canvasWrap: {
      clientWidth: 800,
      clientHeight: 600,
      getBoundingClientRect() {
        return { left: 0, top: 0 };
      },
      scrollTo(payload) {
        scrollCalls.push(payload);
      },
    },
  }, {
    canvas,
    getNode: (id) => (id === "n1" ? { id: "n1", x: 400, y: 300 } : null),
    selectNodeIds: (ids) => selected.push(ids),
    renderAll: () => { renderCount += 1; },
  });

  ui.focusNodeInCanvas("n1");
  ui.focusNodeInCanvas("missing");

  assert.deepEqual(selected, [["n1"]]);
  assert.equal(renderCount, 1);
  assert.deepEqual(scrollCalls, [{ left: 120, top: 90, behavior: "smooth" }]);
});
