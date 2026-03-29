const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadTemplateStorageContractModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/template-storage-contract.js")).href;
  return import(file);
}

function templateGraph() {
  return {
    workflow_id: "wf_local_template",
    version: "1.0.0",
    name: "Local Template",
    nodes: [{ id: "n1", type: "ingest_files", config: {} }],
    edges: [],
  };
}

test("workflow template storage contract migrates legacy array storage", async () => {
  const {
    LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
    LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
    normalizeLocalTemplateStorage,
  } = await loadTemplateStorageContractModule();

  const out = normalizeLocalTemplateStorage([{
    id: "custom_1",
    name: "Local Template 1",
    graph: templateGraph(),
  }], {
    allowStorageSchemaMigration: true,
    allowEntrySchemaMigration: true,
  });

  assert.equal(out.schema_version, LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION);
  assert.equal(out.migrated, true);
  assert.equal(out.items.length, 1);
  assert.equal(out.items[0].schema_version, LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION);
  assert.deepEqual(out.items[0].workflow_definition, templateGraph());
  assert.equal(Object.prototype.hasOwnProperty.call(out.items[0], "graph"), false);
  assert.match(out.notes.join(" | "), /local template storage schema_version migrated/i);
});

test("workflow template storage contract rejects invalid schema or missing workflow definition", async () => {
  const {
    LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
    normalizeLocalTemplateStorage,
  } = await loadTemplateStorageContractModule();

  assert.throws(
    () => normalizeLocalTemplateStorage({
      schema_version: "local_template_storage.v999",
      items: [],
    }),
    /unsupported local template storage schema_version/i
  );

  assert.throws(
    () => normalizeLocalTemplateStorage({
      schema_version: LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
      items: [{
        schema_version: "local_template_entry.v1",
        id: "bad",
        name: "Bad",
      }],
    }),
    /workflow_definition is required/i
  );
});

test("workflow template storage contract stringifies versioned envelope", async () => {
  const {
    LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
    stringifyLocalTemplateStorage,
  } = await loadTemplateStorageContractModule();

  const text = stringifyLocalTemplateStorage([{
    id: "custom_1",
    name: "Local Template 1",
    graph: templateGraph(),
  }], {
    allowStorageSchemaMigration: true,
    allowEntrySchemaMigration: true,
  });

  const payload = JSON.parse(text);
  assert.equal(payload.schema_version, LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION);
  assert.equal(payload.items.length, 1);
  assert.equal(payload.items[0].id, "custom_1");
  assert.deepEqual(payload.items[0].workflow_definition, templateGraph());
  assert.equal(Object.prototype.hasOwnProperty.call(payload.items[0], "graph"), false);
});
