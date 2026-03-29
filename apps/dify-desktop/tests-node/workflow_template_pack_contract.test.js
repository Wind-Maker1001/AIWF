const test = require("node:test");
const assert = require("node:assert/strict");

const {
  TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION,
  exportTemplatePackArtifact,
  normalizeTemplatePackArtifact,
} = require("../workflow_template_pack_contract");

function templateGraph() {
  return {
    workflow_id: "wf_template_pack",
    version: "1.0.0",
    name: "Template Graph",
    nodes: [{ id: "n1", type: "ingest_files", config: {} }],
    edges: [],
  };
}

test("workflow template pack contract migrates legacy artifact and normalizes template workflow definitions", () => {
  const out = normalizeTemplatePackArtifact({
    id: "pack_1",
    name: "Finance Pack",
    templates: [{
      id: "tpl_1",
      name: "Finance Template",
      graph: templateGraph(),
    }],
  }, {
    allowVersionMigration: true,
    source: "legacy_file",
  });

  assert.equal(out.schema_version, TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION);
  assert.equal(out.id, "pack_1");
  assert.equal(out.name, "Finance Pack");
  assert.equal(out.source, "legacy_file");
  assert.equal(out.migrated, true);
  assert.match(out.notes.join(" | "), /schema_version migrated/i);
  assert.equal(out.templates.length, 1);
  assert.equal(out.templates[0].id, "tpl_1");
  assert.equal(out.templates[0].template_spec_version, 1);
  assert.deepEqual(out.templates[0].workflow_definition, templateGraph());
  assert.equal(Object.prototype.hasOwnProperty.call(out.templates[0], "graph"), false);
});

test("workflow template pack contract rejects unsupported schema version and missing template workflow definitions", () => {
  assert.throws(
    () => normalizeTemplatePackArtifact({
      schema_version: "template_pack_artifact.v999",
      id: "pack_bad",
      name: "Bad",
      templates: [],
    }),
    /unsupported template pack schema_version/i
  );

  assert.throws(
    () => normalizeTemplatePackArtifact({
      schema_version: TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION,
      id: "pack_bad",
      name: "Bad",
      templates: [{ id: "tpl_bad", name: "Bad Template" }],
    }),
    /workflow_definition is required/i
  );
});

test("workflow template pack contract exports artifact schema instead of marketplace entry schema", () => {
  const artifact = exportTemplatePackArtifact({
    schema_version: "template_pack_entry.v1",
    id: "pack_1",
    name: "Finance Pack",
    version: "v2",
    source: "inline",
    templates: [{
      id: "tpl_1",
      name: "Finance Template",
      graph: templateGraph(),
      template_spec_version: 1,
    }],
    created_at: "2026-03-24T00:00:00Z",
  }, {
    source: "marketplace_export",
  });

  assert.equal(artifact.schema_version, TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION);
  assert.equal(artifact.source, "marketplace_export");
  assert.equal(artifact.templates.length, 1);
  assert.equal(artifact.templates[0].id, "tpl_1");
  assert.deepEqual(artifact.templates[0].workflow_definition, templateGraph());
  assert.equal(Object.prototype.hasOwnProperty.call(artifact.templates[0], "graph"), false);
});
