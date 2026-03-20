const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPanelsUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/panels-ui.js")).href;
  return import(file);
}

test("workflow panels ui builds quality gate prefs payload", async () => {
  const { createWorkflowPanelsUi } = await loadPanelsUiModule();
  const ui = createWorkflowPanelsUi({
    qualityGateRunIdFilter: { value: " run_1 " },
    qualityGateStatusFilter: { value: "BLOCKED" },
    qualityGateExportFormat: { value: "json" },
  });

  assert.deepEqual(ui.qualityGateFilterPayload(), { run_id: "run_1", status: "blocked" });
  assert.deepEqual(ui.qualityGatePrefsPayload(), {
    filter: { run_id: "run_1", status: "blocked" },
    format: "json",
  });
});
