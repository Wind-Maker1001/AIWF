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
