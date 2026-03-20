const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAuditSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/audit-ui-support.js")).href;
  return import(file);
}

test("workflow audit support normalizes run ids and payloads", async () => {
  const {
    normalizeTimelineRunId,
    timelineStatusMessage,
    failureSummaryRequestPayload,
    auditLogRequestPayload,
  } = await loadAuditSupportModule();

  assert.equal(normalizeTimelineRunId(" run_123 "), "run_123");
  assert.equal(normalizeTimelineRunId(""), "");
  assert.equal(timelineStatusMessage({ ok: true }), "时间线刷新完成");
  assert.equal(timelineStatusMessage({ ok: false, error: "denied" }), "时间线刷新失败: denied");
  assert.deepEqual(failureSummaryRequestPayload(), { limit: 500 });
  assert.deepEqual(auditLogRequestPayload(), { limit: 120 });
});
