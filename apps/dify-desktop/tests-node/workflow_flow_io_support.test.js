const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadFlowIoSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/flow-io-support.js")).href;
  return import(file);
}

test("workflow flow io support normalizes graph json name and load status", async () => {
  const {
    stringifyWorkflowGraph,
    saveWorkflowName,
    loadWorkflowStatusMessage,
  } = await loadFlowIoSupportModule();

  assert.match(stringifyWorkflowGraph({ id: "wf_1" }), /"id": "wf_1"/);
  assert.equal(saveWorkflowName("  Alpha  "), "Alpha");
  assert.equal(saveWorkflowName(""), "workflow");
  assert.equal(loadWorkflowStatusMessage("D:/a.json", { migrated: false }), "流程已加载: D:/a.json");
  assert.equal(
    loadWorkflowStatusMessage("D:/a.json", { migrated: true, notes: ["v2", "sanitized"] }),
    "流程已加载并迁移: D:/a.json (v2, sanitized)"
  );
});
