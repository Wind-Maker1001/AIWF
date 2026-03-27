const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAppServicesAssemblySupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-services.js")).href;
  return import(file);
}

test("workflow app services assembly support builds core context and combines services", async () => {
  const {
    buildCoreServicesContext,
    combineWorkflowAppServices,
  } = await loadAppServicesAssemblySupportModule();

  const coreServicesRef = { current: { syncRunParamsFormFromJson: () => "ok", sandboxDedupWindowSec: () => 33 } };
  const ctx = buildCoreServicesContext({
    els: { ok: true },
    store: { ok: true },
    setStatus: () => {},
    state: {
      getLastCompareResult: () => ({ ok: true }),
      setLastCompareResult: () => {},
      getSelectedEdge: () => ({ from: "a", to: "b" }),
      setSelectedEdge: () => {},
      getCfgViewMode: () => "json",
      setCfgViewMode: () => {},
    },
    renderMigrationReport: () => {},
    staticConfig: { templateStorageKey: "t", builtinTemplates: [], nodeFormSchemas: {}, edgeHintsByNodeType: {} },
    renderAll: () => {},
    refreshOfflineBoundaryHint: () => {},
    coreServicesRef,
    canvas: { ok: true },
  });

  assert.equal(ctx.templateStorageKey, "t");
  assert.deepEqual(ctx.getLastCompareResult(), { ok: true });
  assert.equal(ctx.getCfgViewMode(), "json");
  assert.equal(ctx.sandboxDedupWindowSec(), 33);
  assert.equal(ctx.syncRunParamsFormFromJson(), "ok");

  const combined = combineWorkflowAppServices(
    { a: 1 },
    { renderMigrationReport: () => "migration", b: 2 },
    { c: 3 },
    { d: 4 }
  );
  assert.equal(combined.a, 1);
  assert.equal(combined.b, 2);
  assert.equal(combined.c, 3);
  assert.equal(combined.d, 4);
  assert.equal(typeof combined.renderMigrationReportImpl, "function");
});
