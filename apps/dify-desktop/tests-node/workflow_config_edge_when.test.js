const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadConfigEdgeWhenModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/config-ui-edge-when.js")).href;
  return import(file);
}

function createWrap() {
  return { style: { display: "" } };
}

test("workflow config edge when parses and builds rule conditions", async () => {
  const { createWorkflowConfigEdgeWhenSupport } = await loadConfigEdgeWhenModule();
  const els = {
    edgeWhenText: { value: '{"field":"status","op":"eq","value":"ok"}' },
    edgeWhenKind: { value: "rule" },
    edgeWhenBoolWrap: createWrap(),
    edgeWhenPathWrap: createWrap(),
    edgeWhenRuleWrap: createWrap(),
    edgeWhenBool: { value: "true" },
    edgeWhenPath: { value: "" },
    edgeWhenField: { value: "score" },
    edgeWhenOp: { value: "gte" },
    edgeWhenValue: { value: "3" },
  };

  const support = createWorkflowConfigEdgeWhenSupport(els);

  assert.deepEqual(support.parseEdgeWhenText(), { field: "status", op: "eq", value: "ok" });
  assert.deepEqual(support.edgeWhenFromBuilder(), { field: "score", op: "gte", value: 3 });

  support.setEdgeWhenBuilderVisibility("rule");
  support.syncEdgeTextFromBuilder();
  assert.match(els.edgeWhenText.value, /"field": "score"/);
  assert.equal(els.edgeWhenRuleWrap.style.display, "block");
  assert.equal(els.edgeWhenBoolWrap.style.display, "none");
});

test("workflow config edge when applies bool path and none states", async () => {
  const { createWorkflowConfigEdgeWhenSupport } = await loadConfigEdgeWhenModule();
  const els = {
    edgeWhenText: { value: "" },
    edgeWhenKind: { value: "none" },
    edgeWhenBoolWrap: createWrap(),
    edgeWhenPathWrap: createWrap(),
    edgeWhenRuleWrap: createWrap(),
    edgeWhenBool: { value: "true" },
    edgeWhenPath: { value: "" },
    edgeWhenField: { value: "" },
    edgeWhenOp: { value: "eq" },
    edgeWhenValue: { value: "" },
  };

  const support = createWorkflowConfigEdgeWhenSupport(els);

  support.applyEdgeWhenToBuilder(false);
  assert.equal(els.edgeWhenKind.value, "bool");
  assert.equal(els.edgeWhenBool.value, "false");
  assert.equal(els.edgeWhenBoolWrap.style.display, "block");

  support.applyEdgeWhenToBuilder("output.ok");
  assert.equal(els.edgeWhenKind.value, "path");
  assert.equal(els.edgeWhenPath.value, "output.ok");
  assert.equal(els.edgeWhenPathWrap.style.display, "block");

  support.applyEdgeWhenToBuilder(null);
  assert.equal(els.edgeWhenKind.value, "none");
  assert.equal(els.edgeWhenField.value, "");
  assert.equal(els.edgeWhenValue.value, "");
  assert.equal(els.edgeWhenRuleWrap.style.display, "none");
});
