const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadStatusSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/status-ui-support.js")).href;
  return import(file);
}

test("workflow status support computes class names and applies status", async () => {
  const { workflowStatusClassName, applyWorkflowStatus } = await loadStatusSupportModule();
  const els = { status: { className: "", textContent: "" } };

  assert.equal(workflowStatusClassName(true), "status ok");
  assert.equal(workflowStatusClassName(false), "status bad");

  applyWorkflowStatus(els, "ready", true);
  assert.equal(els.status.className, "status ok");
  assert.equal(els.status.textContent, "ready");
});
