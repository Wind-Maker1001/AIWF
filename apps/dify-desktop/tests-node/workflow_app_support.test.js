const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAppSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-support.js")).href;
  return import(file);
}

const noop = () => {};

test("workflow app support adopts parent desktop bridge when available", async () => {
  const { ensureWorkflowDesktopBridge } = await loadAppSupportModule();
  const bridge = { ping: true };
  const fakeWindow = {
    aiwfDesktop: null,
    parent: { aiwfDesktop: bridge },
  };

  ensureWorkflowDesktopBridge(fakeWindow);
  assert.equal(fakeWindow.aiwfDesktop, bridge);
});

test("workflow app support builds static and boot contexts", async () => {
  const {
    buildWorkflowStaticConfig,
    buildBootWorkflowContext,
    buildWorkflowBootServices,
  } = await loadAppSupportModule();

  const staticConfig = buildWorkflowStaticConfig({ node: 1 }, "prefs");
  assert.equal(staticConfig.qualityGatePrefsKey, "prefs");
  assert.equal(staticConfig.nodeCatalog.node, 1);
  assert.equal(typeof staticConfig.templateStorageKey, "string");

  const selected = [];
  const boot = buildBootWorkflowContext({
    els: { ok: true },
    setStatus: () => {},
    compareState: { get: () => ({ ok: true }) },
    cfgViewModeRef: { get: () => "form" },
    autoFixSummaryRef: { set: (value) => selected.push({ auto: value }) },
    selectedEdgeRef: { get: () => ({ from: "a", to: "b" }), set: (value) => selected.push({ edge: value }) },
    getRenderMigrationReport: () => () => ({ migrated: false }),
    refreshOfflineBoundaryHint: () => {},
    renderAll: () => {},
    store: { ok: true },
    canvas: { ok: true },
    defaultNodeConfig: () => ({}),
    syncCanvasPanels: () => {},
    bootServices: { parseNodeConfigText: () => ({}) },
  });

  assert.equal(boot.getCfgViewMode(), "form");
  assert.deepEqual(boot.getLastCompareResult(), { ok: true });
  assert.equal(typeof boot.selectedEdgeRef.get, "function");
  boot.selectedEdgeRef.set({ from: "x", to: "y" });
  boot.setLastAutoFixSummary({ changed: true });
  assert.deepEqual(selected, [{ edge: { from: "x", to: "y" } }, { auto: { changed: true } }]);

  const bootServices = buildWorkflowBootServices({
    uiServices: { applyDeepSeekDefaults: noop, renderNodeRuns: noop, renderDiagRuns: noop },
    appServices: { handleAddNode: noop, renderPreflightReport: noop, renderQueueRows: noop },
    resetWorkflow: noop,
    clearWorkflow: noop,
  });
  assert.equal(bootServices.handleAddNode, noop);
  assert.equal(bootServices.resetWorkflow, noop);
  assert.equal(bootServices.applyDeepSeekDefaults, noop);
  assert.equal(bootServices.renderQueueRows, noop);
});
