const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadElementsModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/elements.js")).href;
  return import(file);
}

test("workflow elements compose selectors from grouped helpers", async () => {
  const { getWorkflowElements } = await loadElementsModule();
  const calls = [];
  const getById = (id) => {
    calls.push(id);
    return { id };
  };

  const els = getWorkflowElements(getById);
  assert.equal(els.templateSelect.id, "templateSelect");
  assert.equal(els.canvasWrap.id, "canvasWrap");
  assert.equal(els.nodeTypePolicyHint.id, "nodeTypePolicyHint");
  assert.equal(els.edgeWhenText.id, "edgeWhenText");
  assert.equal(els.selectedNodePolicyInfo.id, "selectedNodePolicyInfo");
  assert.equal(els.compareRunA.id, "compareRunA");
  assert.ok(calls.includes("qualityGateRunIdFilter"));
  assert.ok(calls.includes("btnAdd"));
});
