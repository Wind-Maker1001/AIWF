const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadStartupSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-startup-support.js")).href;
  return import(file);
}

test("workflow app startup support parses schema text safely", async () => {
  const { parseStartupAppSchemaJson } = await loadStartupSupportModule();
  assert.deepEqual(parseStartupAppSchemaJson('{"title":{"type":"string"}}'), { title: { type: "string" } });
  assert.deepEqual(parseStartupAppSchemaJson("{bad"), {});
});

test("workflow app startup support renders initial state and swallows refresh rejections", async () => {
  const {
    renderInitialWorkflowAppState,
    runWorkflowStartupRefreshes,
  } = await loadStartupSupportModule();

  const calls = [];
  const els = {
    edgeWhenKind: { value: "rule" },
    appSchemaJson: { value: "" },
  };

  renderInitialWorkflowAppState({
    els,
    renderPalette: () => calls.push("palette"),
    renderTemplateSelect: () => calls.push("template"),
    setCfgMode: (mode) => calls.push(`cfg:${mode}`),
    setEdgeWhenBuilderVisibility: (kind) => calls.push(`edge:${kind}`),
    rebuildEdgeHints: (edge) => calls.push(edge === null ? "hints:null" : "hints"),
    renderAll: () => calls.push("all"),
    renderRunHistoryRows: (items) => calls.push(`history:${items.length}`),
    renderQueueControl: (value) => calls.push(`queueControl:${Object.keys(value).length}`),
    renderVersionCompare: (value) => calls.push(`versionCompare:${value.error}`),
    renderAppSchemaForm: (rows) => calls.push(`schema:${rows.length}`),
    syncAppSchemaJsonFromForm: () => { els.appSchemaJson.value = '{"title":{"type":"string"}}'; },
    renderRunParamsFormBySchema: (schema) => calls.push(`params:${Object.keys(schema).length}`),
    renderSandboxHealth: (health) => calls.push(`sandbox:${health.level}`),
    renderMigrationReport: (value) => calls.push(`migration:${value.migrated}`),
    renderCompareResult: (value) => calls.push(`compare:${value.error}`),
    renderPreflightReport: (value) => calls.push(`preflight:${value.ok}`),
  });

  assert.ok(calls.includes("palette"));
  assert.ok(calls.includes("cfg:form"));
  assert.ok(calls.includes("edge:rule"));
  assert.ok(calls.includes("history:0"));
  assert.ok(calls.includes("versionCompare:暂无"));
  assert.ok(calls.includes("schema:1"));
  assert.ok(calls.includes("params:1"));
  assert.ok(calls.includes("sandbox:green"));
  assert.ok(calls.includes("migration:false"));
  assert.ok(calls.includes("compare:暂无"));
  assert.ok(calls.includes("preflight:true"));

  const refreshCalls = [];
  runWorkflowStartupRefreshes({
    refreshTemplateMarketplace: async () => { refreshCalls.push("market"); throw new Error("offline"); },
    renderTemplateSelect: () => refreshCalls.push("template"),
    refreshQualityRuleSets: async () => { refreshCalls.push("ruleSets"); throw new Error("nope"); },
    refreshDiagnostics: async () => { refreshCalls.push("diag"); },
    refreshRunHistory: async () => { refreshCalls.push("history"); throw new Error("x"); },
  });

  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.ok(refreshCalls.includes("market"));
  assert.ok(refreshCalls.includes("ruleSets"));
  assert.ok(refreshCalls.includes("diag"));
  assert.ok(refreshCalls.includes("history"));
  assert.equal(refreshCalls.includes("template"), false);
});
