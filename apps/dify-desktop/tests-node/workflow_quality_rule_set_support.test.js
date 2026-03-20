const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadQualityRuleSetSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/quality-rule-set-support.js")).href;
  return import(file);
}

test("workflow quality rule set support normalizes ids options and graph rules", async () => {
  const {
    currentQualityRuleSetId,
    normalizeQualityRuleSets,
    buildQualityRuleSetOptions,
    collectQualityRulesFromGraph,
  } = await loadQualityRuleSetSupportModule();

  assert.equal(currentQualityRuleSetId({
    qualityRuleSetId: { value: "" },
    qualityRuleSetSelect: { value: "set_b" },
  }), "set_b");

  const sets = normalizeQualityRuleSets({
    sets: [{ id: "set_a", name: "Set A", version: "v2" }, { id: "" }],
  });
  assert.equal(sets.length, 2);
  assert.deepEqual(buildQualityRuleSetOptions(sets), [
    { value: "set_a", textContent: "Set A (v2)" },
  ]);

  assert.deepEqual(collectQualityRulesFromGraph({
    nodes: [
      { id: "n1", type: "quality_check_v3", config: { rules: { required_columns: ["amount"] } } },
    ],
  }), { required_columns: ["amount"] });
});
