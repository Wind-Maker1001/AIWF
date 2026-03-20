const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAppBootSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-boot-support.js")).href;
  return import(file);
}

test("workflow app boot support builds scoped binding deps", async () => {
  const {
    buildToolbarBindingDeps,
    buildCanvasBindingDeps,
    buildEditorBindingDeps,
    buildStartupDeps,
  } = await loadAppBootSupportModule();

  const noop = () => {};
  const ctx = {
    els: { ok: true },
    setStatus: noop,
    handleAddNode: noop,
    canvas: { x: 1 },
    store: { y: 2 },
    setZoom: noop,
    renderAll: noop,
    renderPalette: noop,
    renderTemplateSelect: noop,
    selectedEdgeRef: { current: null },
    setCfgMode: noop,
    refreshDiagnostics: noop,
    extraIgnored: "ignored",
  };

  const toolbar = buildToolbarBindingDeps(ctx);
  const canvas = buildCanvasBindingDeps(ctx);
  const editor = buildEditorBindingDeps(ctx);
  const startup = buildStartupDeps(ctx);

  assert.equal(toolbar.handleAddNode, noop);
  assert.equal(toolbar.extraIgnored, undefined);

  assert.equal(canvas.canvas, ctx.canvas);
  assert.equal(canvas.selectedEdgeRef, ctx.selectedEdgeRef);
  assert.equal(canvas.handleAddNode, undefined);

  assert.equal(editor.setCfgMode, noop);
  assert.equal(editor.selectedEdgeRef, ctx.selectedEdgeRef);
  assert.equal(editor.renderPalette, undefined);

  assert.equal(startup.renderPalette, noop);
  assert.equal(startup.renderTemplateSelect, noop);
  assert.equal(startup.refreshDiagnostics, noop);
  assert.equal(startup.setZoom, undefined);
});
