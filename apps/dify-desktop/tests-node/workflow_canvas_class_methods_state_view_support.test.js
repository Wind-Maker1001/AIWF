const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadCanvasStateMethodsModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/canvas_class_methods_state.mjs")).href;
  return import(file);
}

async function loadCanvasViewRuntimeModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/canvas_class_methods_view_runtime.mjs")).href;
  return import(file);
}

test("workflow canvas method support installers attach state and view/runtime methods", async () => {
  const { installWorkflowCanvasStateMethods } = await loadCanvasStateMethodsModule();
  const { installWorkflowCanvasViewRuntimeMethods } = await loadCanvasViewRuntimeModule();

  class DummyCanvas {}
  installWorkflowCanvasStateMethods(DummyCanvas);
  installWorkflowCanvasViewRuntimeMethods(DummyCanvas);

  assert.equal(typeof DummyCanvas.prototype.catalogName, "function");
  assert.equal(typeof DummyCanvas.prototype.getSelectedIds, "function");
  assert.equal(typeof DummyCanvas.prototype.setZoom, "function");
  assert.equal(typeof DummyCanvas.prototype.renderCore, "function");
  assert.equal(typeof DummyCanvas.prototype.requestRender, "function");
});
