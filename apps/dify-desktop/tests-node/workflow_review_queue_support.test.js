const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadReviewQueueSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/review-queue-support.js")).href;
  return import(file);
}

test("workflow review queue support normalizes review items list", async () => {
  const { normalizeReviewQueueItems } = await loadReviewQueueSupportModule();
  assert.deepEqual(normalizeReviewQueueItems({ items: [{ id: 1 }] }), [{ id: 1 }]);
  assert.deepEqual(normalizeReviewQueueItems({ items: null }), []);
  assert.deepEqual(normalizeReviewQueueItems(null), []);
});
