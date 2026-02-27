const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { registerOutputDomainChiplets } = require("../workflow_chiplets/domains/output_domain");

test("output domain registers expected chiplets", () => {
  const registry = new WorkflowChipletRegistry();
  registerOutputDomainChiplets(registry, {
    fs,
    path,
    summarizeCorpus: () => ({ sections: 0, bullets: 0, chars: 0, cjk: 0, latin: 0, sha256: "x" }),
    writeWorkflowSummary: () => {},
    sha256Text: () => "sha",
    nodeOutputByType: () => null,
  });
  assert.equal(registry.has("sql_chart_v1"), true);
  assert.equal(registry.has("office_slot_fill_v1"), true);
  assert.equal(registry.has("md_output"), true);
});

test("output sql_chart_v1 groups rows into categories and series", async () => {
  const registry = new WorkflowChipletRegistry();
  registerOutputDomainChiplets(registry, {
    fs,
    path,
    summarizeCorpus: () => ({ sections: 0, bullets: 0, chars: 0, cjk: 0, latin: 0, sha256: "x" }),
    writeWorkflowSummary: () => {},
    sha256Text: () => "sha",
    nodeOutputByType: () => null,
  });
  const out = await registry.resolve("sql_chart_v1").run(
    {},
    {
      config: {
        rows: [
          { category: "A", series: "s1", value: 3 },
          { category: "A", series: "s1", value: 2 },
          { category: "B", series: "s1", value: 4 },
        ],
      },
    },
  );
  assert.equal(out.ok, true);
  assert.deepEqual(out.categories, ["A", "B"]);
  assert.equal(Array.isArray(out.series), true);
  assert.equal(out.rows_in, 3);
});
