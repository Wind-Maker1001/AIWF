const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPanelsUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/panels-ui.js")).href;
  return import(file);
}

test("workflow panels ui renders cache stats summary", async () => {
  const { createWorkflowPanelsUi } = await loadPanelsUiModule();
  const cacheStatsText = { textContent: "" };
  const ui = createWorkflowPanelsUi({ cacheStatsText });

  ui.renderCacheStats({ entries: 5, hits: 9, misses: 3, hit_rate: 0.75 });
  assert.equal(cacheStatsText.textContent, "缓存项:5 命中:9 未命中:3 命中率:0.75");

  ui.renderCacheStats(null);
  assert.equal(cacheStatsText.textContent, "缓存状态: -");
});
