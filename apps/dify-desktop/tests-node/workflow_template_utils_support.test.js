const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadTemplateUtilsRulesModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/template-utils-rules.js")).href;
  return import(file);
}

async function loadTemplateUtilsMigrationModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/template-utils-migration.js")).href;
  return import(file);
}

test("workflow template utils rules resolve conditions and validate params", async () => {
  const {
    defaultTemplateParamValue,
    resolveTemplateRule,
    validateTemplateParams,
    applyTemplateVars,
  } = await loadTemplateUtilsRulesModule();

  assert.equal(defaultTemplateParamValue({ type: "number" }), 0);
  assert.deepEqual(defaultTemplateParamValue({ type: "array" }), []);

  const rule = resolveTemplateRule({
    type: "string",
    depends_on: { field: "mode", equals: "strict" },
    conditional: [{ when: { field: "mode", equals: "strict" }, min_length: 3 }],
  }, { mode: "strict" });
  assert.equal(rule.__active, true);
  assert.equal(rule.min_length, 3);

  validateTemplateParams({
    title: { type: "string", required: true, min_length: 3 },
    level: { type: "number", min: 1, max: 5 },
  }, { title: "demo", level: 3 });
  assert.throws(
    () => validateTemplateParams({ title: { type: "string", required: true } }, {}),
    /模板参数校验失败/
  );

  assert.deepEqual(applyTemplateVars({ title: "{{name}}", count: "{{num}}" }, { name: "A", num: 3 }), {
    title: "A",
    count: 3,
  });
});

test("workflow template utils migration upgrades legacy workflow graphs", async () => {
  const {
    cloneJson,
    previewValue,
    migrateLoadedWorkflowGraph,
  } = await loadTemplateUtilsMigrationModule();

  assert.deepEqual(cloneJson({ a: 1 }), { a: 1 });
  assert.equal(previewValue(undefined), "(undefined)");
  assert.equal(previewValue({ a: 1 }), '{"a":1}');

  const migrated = migrateLoadedWorkflowGraph({
    workflow_id: "finance_report_v1",
    nodes: [{ id: "n1", type: "anomaly_explain_v1", config: {} }],
  });
  assert.equal(migrated.migrated, true);
  assert.equal(migrated.graph.workflow_id, "finance_report_v2");
  assert.equal(migrated.graph.template_key, "finance_report");
  assert.equal(migrated.graph.template_version, 2);
  assert.equal(migrated.graph.nodes[0].config.score_field, "anomaly_score");
  assert.equal(migrated.graph.nodes[0].config.threshold, 0.8);
});
