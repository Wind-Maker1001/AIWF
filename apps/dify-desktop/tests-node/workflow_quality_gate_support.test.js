const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadQualityGateSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/quality-gate-support.js")).href;
  return import(file);
}

test("workflow quality gate support normalizes format status and prefs", async () => {
  const {
    normalizeQualityGateExportFormat,
    normalizeQualityGateStatus,
    parseQualityGatePrefs,
  } = await loadQualityGateSupportModule();

  assert.equal(normalizeQualityGateExportFormat("JSON"), "json");
  assert.equal(normalizeQualityGateExportFormat("csv"), "md");
  assert.equal(normalizeQualityGateStatus("BLOCKED"), "blocked");
  assert.equal(normalizeQualityGateStatus("weird"), "all");

  assert.deepEqual(parseQualityGatePrefs('{"filter":{"run_id":"run_1","status":"pass"},"format":"json"}'), {
    filter: { run_id: "run_1", status: "pass" },
    format: "json",
  });
  assert.equal(parseQualityGatePrefs(""), null);
});
