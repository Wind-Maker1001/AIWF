const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPanelsUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/panels-ui.js")).href;
  return import(file);
}

test("workflow panels ui renders queue control summary", async () => {
  const { createWorkflowPanelsUi } = await loadPanelsUiModule();
  const queueControlText = { textContent: "" };
  const ui = createWorkflowPanelsUi({ queueControlText });

  ui.renderQueueControl({ paused: true, quotas: { local: 2, rust: 1 } });
  assert.equal(queueControlText.textContent, "队列状态: 暂停 | 并发配额: local:2, rust:1");

  ui.renderQueueControl({});
  assert.equal(queueControlText.textContent, "队列状态: 运行 | 并发配额: 默认");
});
