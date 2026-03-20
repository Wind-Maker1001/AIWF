const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadRunQueueSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/run-queue-support.js")).href;
  return import(file);
}

test("workflow run queue support builds request payloads and queue status text", async () => {
  const {
    runHistoryRequestPayload,
    queueRequestPayload,
    queueControlStatusText,
    normalizeQueueItems,
    normalizeQueueControl,
  } = await loadRunQueueSupportModule();

  assert.deepEqual(runHistoryRequestPayload(), { limit: 80 });
  assert.deepEqual(queueRequestPayload(), { limit: 120 });
  assert.equal(queueControlStatusText(true), "队列已暂停");
  assert.equal(queueControlStatusText(false, "busy"), "恢复失败: busy");
  assert.deepEqual(normalizeQueueItems({ items: [{ id: 1 }] }), [{ id: 1 }]);
  assert.deepEqual(normalizeQueueControl({ control: { paused: true } }), { paused: true });
});
