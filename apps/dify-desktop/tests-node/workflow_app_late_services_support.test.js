const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

test("workflow late services support builds scoped dependency bags", async () => {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-late-services.js")).href;
  const {
    buildFlowIoDeps,
    buildPaletteUiDeps,
    buildCanvasViewUiDeps,
    buildRunControllerUiDeps,
  } = await import(file);

  const noop = () => {};
  const ctx = {
    setStatus: noop,
    graphPayload: noop,
    refreshVersions: noop,
    migrateLoadedWorkflowGraph: noop,
    applyLoadedWorkflowGraph: noop,
    getLoadedWorkflowName: noop,
    renderMigrationReport: noop,
    nodeCatalog: { a: 1 },
    defaultNodeConfigFn: noop,
    store: { addNode: noop },
    selectNodeIds: noop,
    renderAll: noop,
    computeDropPosition: noop,
    canvas: { ok: true },
    renderNodeConfigEditor: noop,
    renderEdgeConfigEditor: noop,
    refreshOfflineBoundaryHint: noop,
    getNode: noop,
    runWorkflowPreflight: noop,
    runPayload: noop,
    renderNodeRuns: noop,
    refreshDiagnostics: noop,
    refreshRunHistory: noop,
    refreshReviewQueue: noop,
    refreshQueue: noop,
    extra: 1,
  };

  const flowIo = buildFlowIoDeps(ctx);
  const palette = buildPaletteUiDeps(ctx);
  const canvasView = buildCanvasViewUiDeps(ctx);
  const runController = buildRunControllerUiDeps(ctx);

  assert.equal(typeof flowIo.refreshVersions, "function");
  assert.equal(flowIo.extra, undefined);
  assert.equal(typeof palette.createNode, "function");
  assert.equal(palette.nodeCatalog.a, 1);
  assert.equal(canvasView.canvas.ok, true);
  assert.equal(canvasView.extra, undefined);
  assert.equal(runController.runPayload, noop);
  assert.equal(runController.refreshQueue, noop);
});
