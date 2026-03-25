const test = require("node:test");
const assert = require("node:assert/strict");

const { createWorkflowReportSupport } = require("../workflow_ipc_reports");

test("workflow report support injects backend-owned quality rule sets into payload", async () => {
  const support = createWorkflowReportSupport({
    deepClone: (value) => JSON.parse(JSON.stringify(value)),
    findRunById: () => null,
    listRunBaselines: () => [],
    qualityRuleSetSupport: {
      getQualityRuleSet: async () => ({
        id: "finance_default",
        name: "Finance Default",
        version: "v3",
        provider: "glue_http",
        owner: "glue-python",
        rules: { required_columns: ["amount"] },
      }),
    },
  });

  const payload = {
    quality_rule_set_id: "finance_default",
    workflow: {
      nodes: [
        { id: "n1", type: "quality_check_v3", config: { existing: true } },
        { id: "n2", type: "ingest_files", config: {} },
      ],
    },
  };

  const out = await support.applyQualityRuleSetToPayload(payload, { mode: "base_api" });
  assert.deepEqual(out.workflow.nodes[0].config.rules, { required_columns: ["amount"] });
  assert.deepEqual(out.workflow.nodes[0].config.rule_set_meta, {
    id: "finance_default",
    name: "Finance Default",
    version: "v3",
    provider: "glue_http",
    owner: "glue-python",
  });
  assert.equal(out.workflow.nodes[1].config.rules, undefined);
});

test("workflow report support rejects missing quality rule sets instead of silently skipping", async () => {
  const support = createWorkflowReportSupport({
    deepClone: (value) => JSON.parse(JSON.stringify(value)),
    listRunBaselines: () => [],
    qualityRuleSetSupport: {
      getQualityRuleSet: async () => null,
    },
  });

  await assert.rejects(
    () => support.applyQualityRuleSetToPayload({
      quality_rule_set_id: "missing_set",
      workflow: { nodes: [{ id: "n1", type: "quality_check_v3", config: {} }] },
    }, { mode: "base_api" }),
    /quality rule set not found: missing_set/
  );
});

test("workflow report support compares runs via async run provider", async () => {
  const runs = new Map([
    ["run_a", {
      run_id: "run_a",
      result: {
        ok: true,
        status: "done",
        node_runs: [{ id: "n1", type: "ingest_files", status: "done", seconds: 1.0 }],
      },
    }],
    ["run_b", {
      run_id: "run_b",
      result: {
        ok: false,
        status: "failed",
        node_runs: [
          { id: "n1", type: "ingest_files", status: "failed", seconds: 2.5 },
          { id: "n2", type: "quality_check_v3", status: "failed", seconds: 1.0 },
        ],
      },
    }],
  ]);
  const support = createWorkflowReportSupport({
    deepClone: (value) => JSON.parse(JSON.stringify(value)),
    getRun: async (runId) => runs.get(runId) || null,
    listRunBaselines: async () => ({ items: [{ baseline_id: "base_1", name: "Base", run_id: "run_a" }] }),
  });

  const compare = await support.buildRunCompare("run_a", "run_b");
  assert.equal(compare.ok, true);
  assert.equal(compare.summary.changed_nodes, 1);
  assert.equal(compare.node_diff.length, 1);
  assert.equal(compare.node_diff[0].status_changed, true);

  const baseline = await support.buildRunRegressionAgainstBaseline("run_b", "base_1");
  assert.equal(baseline.ok, true);
  assert.equal(baseline.regression.changed_nodes, 1);
  assert.equal(baseline.regression.status_flip_nodes, 1);
});
