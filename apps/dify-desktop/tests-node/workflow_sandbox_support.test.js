const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadSandboxSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/sandbox-support.js")).href;
  return import(file);
}

test("workflow sandbox support builds request payload helpers", async () => {
  const {
    sandboxExportFormat,
    sandboxAlertsPayload,
    sandboxRulesPayload,
    sandboxMutePayload,
    sandboxPresetExportPayload,
    sandboxListPayload,
  } = await loadSandboxSupportModule();

  assert.equal(sandboxExportFormat("json"), "json");
  assert.equal(sandboxExportFormat(""), "md");
  assert.deepEqual(sandboxAlertsPayload({ yellow: 1 }, 600), {
    limit: 500,
    thresholds: { yellow: 1 },
    dedup_window_sec: 600,
  });
  assert.deepEqual(sandboxRulesPayload({ a: 1 }), { rules: { a: 1 } });
  assert.deepEqual(sandboxMutePayload({
    sandboxMuteNodeType: { value: " node " },
    sandboxMuteNodeId: { value: "" },
    sandboxMuteCode: { value: "" },
    sandboxMuteMinutes: { value: "30" },
  }), {
    node_type: "node",
    node_id: "*",
    code: "*",
    minutes: 30,
  });
  assert.deepEqual(sandboxPresetExportPayload({ x: 1 }), { preset: { x: 1 } });
  assert.deepEqual(sandboxListPayload(80), { limit: 80 });
});
