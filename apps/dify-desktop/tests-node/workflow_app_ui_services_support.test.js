const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAppUiServicesSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-ui-services-support.js")).href;
  return import(file);
}

test("workflow app ui services support builds child dependency bags", async () => {
  const {
    buildDiagnosticsPanelDeps,
    buildDebugApiDeps,
    buildConnectivityUiDeps,
  } = await loadAppUiServicesSupportModule();

  const noop = () => {};
  const store = { exportGraph: () => ({ workflow_id: "wf_1" }) };

  const diag = buildDiagnosticsPanelDeps({ renderDiagRuns: noop, fetchRustRuntimeStats: noop });
  const debug = buildDebugApiDeps({ store, canvas: { ok: true }, renderAll: noop });
  const connectivity = buildConnectivityUiDeps({ setStatus: noop, store });

  assert.equal(diag.renderDiagRuns, noop);
  assert.equal(debug.store, store);
  assert.equal(debug.canvas.ok, true);
  assert.deepEqual(connectivity.exportGraph(), { workflow_id: "wf_1" });
});
