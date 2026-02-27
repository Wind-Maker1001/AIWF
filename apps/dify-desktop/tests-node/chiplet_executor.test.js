const test = require("node:test");
const assert = require("node:assert/strict");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { executeWorkflowDag } = require("../workflow_chiplets/executor");
const { buildEnvelope } = require("../workflow_chiplets/contract");

test("executor schedules ready nodes by chiplet priority", async () => {
  const prev = process.env.AIWF_CHIPLET_MAX_PARALLEL;
  process.env.AIWF_CHIPLET_MAX_PARALLEL = "1";
  const registry = new WorkflowChipletRegistry();
  const order = [];
  registry.register("type_low", {
    id: "chiplet.low.v1",
    priority: 10,
    async run() {
      order.push("low");
      return { ok: true };
    },
  });
  registry.register("type_high", {
    id: "chiplet.high.v1",
    priority: 200,
    async run() {
      order.push("high");
      return { ok: true };
    },
  });

  const graph = {
    workflow_id: "w1",
    nodes: [
      { id: "n1", type: "type_low" },
      { id: "n2", type: "type_high" },
    ],
    edges: [],
  };
  const ctx = { runId: "r1", workflowId: "w1", payload: {} };
  try {
    await executeWorkflowDag({
      graph,
      registry,
      ctx,
      buildEnvelope,
    });
  } finally {
    if (typeof prev === "undefined") delete process.env.AIWF_CHIPLET_MAX_PARALLEL;
    else process.env.AIWF_CHIPLET_MAX_PARALLEL = prev;
  }
  assert.deepEqual(order, ["high", "low"]);
});

test("executor surfaces structured failure from runner", async () => {
  const registry = new WorkflowChipletRegistry();
  registry.register("type_fail", {
    id: "chiplet.fail.v1",
    timeout_ms: 1000,
    async run() {
      throw new Error("boom");
    },
  });
  const graph = {
    workflow_id: "w_fail",
    nodes: [{ id: "n1", type: "type_fail" }],
    edges: [],
  };
  const failed = [];
  await assert.rejects(
    executeWorkflowDag({
      graph,
      registry,
      ctx: { runId: "r1", workflowId: "w_fail", payload: {} },
      buildEnvelope,
      onNodeFailure(node, err) {
        failed.push({ id: node.id, code: err.code, kind: err.kind });
      },
    }),
    /boom/
  );
  assert.equal(failed.length, 1);
  assert.equal(failed[0].code, "node_execution_failed");
  assert.equal(failed[0].kind, "runtime");
});
