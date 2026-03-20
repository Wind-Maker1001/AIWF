const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadVersionCacheSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/version-cache-support.js")).href;
  return import(file);
}

test("workflow version cache support builds compare payloads and cache status text", async () => {
  const {
    versionListRequestPayload,
    versionComparePayload,
    cacheStatsStatusText,
  } = await loadVersionCacheSupportModule();

  assert.deepEqual(versionListRequestPayload(), { limit: 120 });
  assert.deepEqual(versionComparePayload(" ver_a ", "ver_b"), { version_a: "ver_a", version_b: "ver_b" });
  assert.equal(cacheStatsStatusText(true), "缓存已清空");
  assert.equal(cacheStatsStatusText(false, "locked"), "清空缓存失败: locked");
});
