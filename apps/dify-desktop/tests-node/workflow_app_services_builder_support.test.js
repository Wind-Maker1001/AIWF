const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPanelsSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-services.js")).href;
  return import(file);
}

test("workflow app services panel support builds refresh and jump helpers", async () => {
  const { buildPanelsUiDeps } = await loadPanelsSupportModule();
  let reviewRefresh = 0;
  let qualityRefresh = 0;
  const els = {
    btnReviewsRefresh: { scrollIntoView: () => { reviewRefresh += 10; } },
    qualityGateRunIdFilter: { value: "" },
    qualityGateStatusFilter: { value: "" },
    btnQualityGateRefresh: { scrollIntoView: () => { qualityRefresh += 10; } },
  };
  const panelServices = {
    refreshReviewQueue: async () => { reviewRefresh += 1; },
    refreshQualityGateReports: async () => { qualityRefresh += 1; },
  };
  const deps = buildPanelsUiDeps({
    els,
    getPanelServices: () => panelServices,
    coreServices: {},
    graphShellApi: { applyRestoredWorkflowGraph: () => {} },
  });

  await deps.showReviewQueue();
  await deps.showQualityGate("run_1");

  assert.equal(reviewRefresh, 11);
  assert.equal(qualityRefresh, 11);
  assert.equal(els.qualityGateRunIdFilter.value, "run_1");
  assert.equal(els.qualityGateStatusFilter.value, "blocked");
});
