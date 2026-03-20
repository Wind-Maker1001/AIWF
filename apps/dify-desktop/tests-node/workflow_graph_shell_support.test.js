const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadGraphShellSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/graph-shell-support.js")).href;
  return import(file);
}

test("workflow graph shell support applies reset and clear side effects", async () => {
  const {
    applyGraphShellResetState,
    applyGraphShellClearState,
  } = await loadGraphShellSupportModule();

  const calls = [];
  const statuses = [];
  const workflowName = { value: "" };
  const store = {
    reset: () => calls.push("reset"),
    clear: () => calls.push("clear"),
  };

  applyGraphShellResetState({
    store,
    setSelectedEdge: (edge) => calls.push({ edge }),
    renderAll: () => calls.push("render"),
    renderMigrationReport: (report) => calls.push({ migration: report }),
    setStatus: (text, ok) => statuses.push({ text, ok }),
    workflowName,
    getResetWorkflowName: () => "自由编排流程",
  });
  applyGraphShellClearState({
    store,
    setSelectedEdge: (edge) => calls.push({ edge }),
    renderAll: () => calls.push("render"),
    renderMigrationReport: (report) => calls.push({ migration: report }),
    setStatus: (text, ok) => statuses.push({ text, ok }),
  });

  assert.equal(workflowName.value, "自由编排流程");
  assert.deepEqual(calls, [
    "reset",
    { edge: null },
    "render",
    { migration: { migrated: false } },
    "clear",
    { edge: null },
    "render",
    { migration: { migrated: false } },
  ]);
  assert.deepEqual(statuses, [
    { text: "已重置默认流程", ok: true },
    { text: "画布已清空", ok: true },
  ]);
});
