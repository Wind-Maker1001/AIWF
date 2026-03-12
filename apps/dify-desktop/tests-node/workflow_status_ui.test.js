const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadStatusUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/status-ui.js")).href;
  return import(file);
}

test("workflow status ui updates status element text and class", async () => {
  const { createWorkflowStatusUi } = await loadStatusUiModule();
  const statusEl = { className: "", textContent: "" };
  const { setStatus } = createWorkflowStatusUi({ status: statusEl });

  setStatus("ready");
  assert.equal(statusEl.className, "status ok");
  assert.equal(statusEl.textContent, "ready");

  setStatus("failed", false);
  assert.equal(statusEl.className, "status bad");
  assert.equal(statusEl.textContent, "failed");
});
