const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadGraphShellUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/graph-shell-ui.js")).href;
  return import(file);
}

test("workflow graph shell ui applies restored graph", async () => {
  const { createWorkflowGraphShellUi } = await loadGraphShellUiModule();
  const imported = [];
  const selectedEdges = [];
  let renderCount = 0;
  const ui = createWorkflowGraphShellUi({
    workflowName: { value: "" },
  }, {
    store: {
      importGraph: (graph) => imported.push(graph),
      reset() {},
      clear() {},
    },
    setSelectedEdge: (edge) => selectedEdges.push(edge),
    renderAll: () => { renderCount += 1; },
  });

  ui.applyRestoredWorkflowGraph({ nodes: [{ id: "n1" }], edges: [] });

  assert.deepEqual(imported, [{ nodes: [{ id: "n1" }], edges: [] }]);
  assert.deepEqual(selectedEdges, [null]);
  assert.equal(renderCount, 1);
});

test("workflow graph shell ui resets and clears graph shell state", async () => {
  const { createWorkflowGraphShellUi } = await loadGraphShellUiModule();
  const calls = [];
  const statuses = [];
  const workflowName = { value: "" };
  const ui = createWorkflowGraphShellUi({ workflowName }, {
    store: {
      importGraph() {},
      reset: () => calls.push("reset"),
      clear: () => calls.push("clear"),
    },
    setSelectedEdge: (edge) => calls.push({ selectedEdge: edge }),
    renderAll: () => calls.push("render"),
    renderMigrationReport: (report) => calls.push({ migration: report }),
    setStatus: (text, ok) => statuses.push({ text, ok }),
    getResetWorkflowName: () => "自由编排流程",
  });

  ui.resetWorkflow();
  ui.clearWorkflow();

  assert.equal(workflowName.value, "自由编排流程");
  assert.deepEqual(calls, [
    "reset",
    { selectedEdge: null },
    "render",
    { migration: { migrated: false } },
    "clear",
    { selectedEdge: null },
    "render",
    { migration: { migrated: false } },
  ]);
  assert.deepEqual(statuses, [
    { text: "已重置默认流程", ok: true },
    { text: "画布已清空", ok: true },
  ]);
});
