const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");
const fs = require("node:fs");

async function loadStaticConfigModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/static-config.js")).href;
  return import(file);
}

function normalizeBuiltinTemplate(item = {}) {
  const workflowDefinition = JSON.parse(JSON.stringify(item.workflow_definition || item.graph || {}));
  if (workflowDefinition && typeof workflowDefinition === "object") {
    delete workflowDefinition.version;
  }
  return {
    id: String(item.id || ""),
    name: String(item.name || ""),
    params_schema: item.params_schema || {},
    workflow_definition: workflowDefinition,
  };
}

test("workflow builtin template snapshot stays aligned with static-config builtins", async () => {
  const { BUILTIN_TEMPLATES } = await loadStaticConfigModule();
  const snapshotPath = path.resolve(__dirname, "../../../contracts/desktop/workflow_builtin_templates.v1.json");
  const snapshot = JSON.parse(fs.readFileSync(snapshotPath, "utf8"));

  assert.equal(snapshot.schema_version, "workflow_builtin_templates.v1");
  assert.equal(snapshot.items.length, BUILTIN_TEMPLATES.length);
  assert.deepEqual(
    snapshot.items.map(normalizeBuiltinTemplate),
    BUILTIN_TEMPLATES.map(normalizeBuiltinTemplate),
  );
});
