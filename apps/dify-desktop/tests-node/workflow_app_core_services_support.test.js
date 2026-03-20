const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAppCoreServicesSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-core-services-support.js")).href;
  return import(file);
}

test("workflow app core services support builds child dependency bags", async () => {
  const {
    buildSupportUiDeps,
    buildRunPayloadUiDeps,
    buildAppFormUiDeps,
    buildConfigUiDeps,
    buildTemplateUiDeps,
  } = await loadAppCoreServicesSupportModule();

  const noop = () => {};
  const support = buildSupportUiDeps({
    setStatus: noop,
    getLastCompareResult: noop,
    setLastCompareResult: noop,
  });
  assert.equal(support.setStatus, noop);

  const runPayload = buildRunPayloadUiDeps({
    store: { ok: true },
    supportUi: { sandboxDedupWindowSec: noop },
  });
  assert.equal(runPayload.store.ok, true);
  assert.equal(runPayload.sandboxDedupWindowSec, noop);

  const appForm = buildAppFormUiDeps({ setStatus: noop });
  assert.equal(appForm.setStatus, noop);

  const config = buildConfigUiDeps({
    store: { ok: true },
    canvas: { ok: true },
    nodeFormSchemas: { x: [] },
    edgeHintsByNodeType: { y: [] },
    setStatus: noop,
  });
  assert.equal(config.store.ok, true);
  assert.equal(config.canvas.ok, true);

  const template = buildTemplateUiDeps({
    templateStorageKey: "k",
    builtinTemplates: [{ id: "t" }],
    store: { ok: true },
    runPayloadUi: { graphPayload: noop },
    syncRunParamsFormFromJson: noop,
  });
  assert.equal(template.templateStorageKey, "k");
  assert.equal(template.graphPayload, noop);
});
