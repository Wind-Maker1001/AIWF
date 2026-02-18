const test = require("node:test");
const assert = require("node:assert/strict");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { runChipletNode, __resetRunnerCircuitForTests } = require("../workflow_chiplets/runner");
const { buildEnvelope } = require("../workflow_chiplets/contract");

function makeCtx() {
  return {
    payload: {},
    runId: "r1",
    workflowId: "w1",
  };
}

test("runner retries and eventually succeeds", async () => {
  __resetRunnerCircuitForTests();
  const registry = new WorkflowChipletRegistry();
  let attempts = 0;
  registry.register("ingest_files", {
    id: "chiplet.test.retry.v1",
    retries: 1,
    timeout_ms: 3000,
    async run() {
      attempts += 1;
      if (attempts < 2) throw new Error("first fail");
      return { input_files: [], count: 0 };
    },
  });
  const out = await runChipletNode({
    registry,
    node: { id: "n1", type: "ingest_files" },
    ctx: makeCtx(),
    envelope: buildEnvelope({ run_id: "r1", workflow_id: "w1", node_id: "n1", node_type: "ingest_files" }),
  });
  assert.equal(out.count, 0);
  assert.equal(attempts, 2);
});

test("runner enforces timeout", async () => {
  __resetRunnerCircuitForTests();
  const registry = new WorkflowChipletRegistry();
  registry.register("ingest_files", {
    id: "chiplet.test.timeout.v1",
    retries: 0,
    timeout_ms: 1000,
    async run() {
      await new Promise((r) => setTimeout(r, 1300));
      return { input_files: [], count: 0 };
    },
  });
  await assert.rejects(
    runChipletNode({
      registry,
      node: { id: "n1", type: "ingest_files" },
      ctx: makeCtx(),
      envelope: buildEnvelope({ run_id: "r1", workflow_id: "w1", node_id: "n1", node_type: "ingest_files" }),
    }),
    /chiplet timeout/i
  );
});

test("runner opens circuit after threshold", async () => {
  __resetRunnerCircuitForTests();
  const registry = new WorkflowChipletRegistry();
  registry.register("ingest_files", {
    id: "chiplet.test.circuit.v1",
    retries: 0,
    timeout_ms: 3000,
    circuit: { enabled: true, failure_threshold: 1, cooldown_ms: 2000 },
    async run() {
      throw new Error("always fail");
    },
  });
  await assert.rejects(
    runChipletNode({
      registry,
      node: { id: "n1", type: "ingest_files" },
      ctx: makeCtx(),
      envelope: buildEnvelope({ run_id: "r1", workflow_id: "w1", node_id: "n1", node_type: "ingest_files" }),
    })
  );
  await assert.rejects(
    runChipletNode({
      registry,
      node: { id: "n1", type: "ingest_files" },
      ctx: makeCtx(),
      envelope: buildEnvelope({ run_id: "r1", workflow_id: "w1", node_id: "n1", node_type: "ingest_files" }),
    }),
    /circuit open/i
  );
});
