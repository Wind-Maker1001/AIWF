const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadConnectivitySupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/connectivity-support.js")).href;
  return import(file);
}

test("workflow connectivity support classifies nodes and builds boundary hints", async () => {
  const {
    classifyConnectivityNodes,
    buildOfflineBoundaryHint,
    deepSeekDefaults,
  } = await loadConnectivitySupportModule();

  const classified = classifyConnectivityNodes({
    nodes: [{ type: "ai_refine" }, { type: "clean_md" }, { type: "custom_x" }],
  });
  assert.equal(classified.onlineNodes.length, 1);
  assert.equal(classified.unknownNodes.length, 1);
  assert.match(buildOfflineBoundaryHint({ nodes: [{ type: "clean_md" }] }, ""), /离线能力边界/);
  assert.deepEqual(deepSeekDefaults("", ""), {
    endpoint: "https://api.deepseek.com/v1/chat/completions",
    model: "deepseek-chat",
  });
});
