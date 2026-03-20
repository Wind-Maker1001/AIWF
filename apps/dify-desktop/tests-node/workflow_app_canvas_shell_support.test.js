const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadCanvasShellSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-canvas-shell-support.js")).href;
  return import(file);
}

test("workflow canvas shell support builds canvas callbacks and graph shell deps", async () => {
  const {
    buildWorkflowCanvasDeps,
    buildGraphShellDeps,
  } = await loadCanvasShellSupportModule();

  const statuses = [];
  const selectedEdges = [];
  let nodeEditorCalls = 0;
  let edgeEditorCalls = 0;
  const ctx = {
    store: { ok: true },
    nodeCatalog: { a: 1 },
    els: {
      canvasWrap: { id: "wrap" },
      canvasSurface: { id: "surface" },
      nodesLayer: { id: "nodes" },
      guideLayer: { id: "guides" },
      minimap: { id: "minimap" },
      edges: { id: "edges" },
    },
    setStatus: (text, ok) => statuses.push({ text, ok }),
    renderAll: () => { nodeEditorCalls += 10; },
    selectedEdgeRef: { set: (edge) => selectedEdges.push(edge) },
    getRenderNodeConfigEditor: () => () => { nodeEditorCalls += 1; },
    getRenderEdgeConfigEditor: () => () => { edgeEditorCalls += 1; },
  };

  const canvasDeps = buildWorkflowCanvasDeps(ctx);
  assert.equal(canvasDeps.canvasWrap.id, "wrap");
  canvasDeps.onWarn("warn");
  canvasDeps.onSelectionChange();
  canvasDeps.onEdgeSelect({ from: "a", to: "b", when: true });
  canvasDeps.onEdgeSelect({ from: "a" });

  assert.deepEqual(statuses, [{ text: "warn", ok: false }]);
  assert.equal(nodeEditorCalls, 1);
  assert.equal(edgeEditorCalls, 2);
  assert.deepEqual(selectedEdges, [{ from: "a", to: "b", when: true }, null]);

  const graphEdges = [];
  const graphDeps = buildGraphShellDeps({
    ...ctx,
    getResetWorkflowName: () => "reset-name",
    renderMigrationReport: () => {},
    selectedEdgeRef: { set: (edge) => graphEdges.push(edge) },
  });

  assert.equal(graphDeps.getResetWorkflowName(), "reset-name");
  graphDeps.setSelectedEdge({ from: "x", to: "y" });
  assert.deepEqual(graphEdges, [{ from: "x", to: "y" }]);
});
