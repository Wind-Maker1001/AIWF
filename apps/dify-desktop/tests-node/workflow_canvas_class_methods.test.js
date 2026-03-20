const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadCanvasClassMethodsModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/canvas_class_methods.mjs")).href;
  return import(file);
}

test("workflow canvas class methods installer adds expected prototype methods", async () => {
  const { installWorkflowCanvasMethods } = await loadCanvasClassMethodsModule();

  class DummyCanvas {}
  installWorkflowCanvasMethods(DummyCanvas);

  assert.equal(typeof DummyCanvas.prototype.setZoom, "function");
  assert.equal(typeof DummyCanvas.prototype.getSelectedIds, "function");
  assert.equal(typeof DummyCanvas.prototype.renderEdges, "function");
  assert.equal(typeof DummyCanvas.prototype.finishLinkByEvent, "function");
});
