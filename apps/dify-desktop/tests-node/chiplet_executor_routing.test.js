const test = require("node:test");
const assert = require("node:assert/strict");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { executeWorkflowDag } = require("../workflow_chiplets/executor");
const { buildEnvelope } = require("../workflow_chiplets/contract");

test("executor routes by edge.when and skips unreachable branch", async () => {
  const registry = new WorkflowChipletRegistry();
  const ran = [];
  registry.register("gate", {
    id: "chiplet.gate.v1",
    priority: 100,
    async run() {
      ran.push("gate");
      return { ok: true, route: "yes" };
    },
  });
  registry.register("yes", {
    id: "chiplet.yes.v1",
    async run() {
      ran.push("yes");
      return { ok: true };
    },
  });
  registry.register("no", {
    id: "chiplet.no.v1",
    async run() {
      ran.push("no");
      return { ok: true };
    },
  });

  const statuses = {};
  await executeWorkflowDag({
    graph: {
      workflow_id: "w_route",
      nodes: [
        { id: "n1", type: "gate" },
        { id: "n2", type: "yes" },
        { id: "n3", type: "no" },
      ],
      edges: [
        { from: "n1", to: "n2", when: { field: "route", op: "eq", value: "yes" } },
        { from: "n1", to: "n3", when: { field: "route", op: "eq", value: "no" } },
      ],
    },
    registry,
    ctx: { runId: "r1", workflowId: "w_route", payload: {} },
    buildEnvelope,
    onNodeSuccess(node, out) {
      statuses[node.id] = String(out?.status || "done");
    },
  });

  assert.deepEqual(ran, ["gate", "yes"]);
  assert.equal(statuses.n3, "skipped");
});

test("manual_review chiplet can reject then downstream gets skipped by condition", async () => {
  const registry = new WorkflowChipletRegistry();
  registry.register("manual_review", {
    id: "chiplet.manual_review.v1",
    async run() {
      return { ok: false, approved: false, status: "rejected", review_key: "r1" };
    },
  });
  let ranDownstream = false;
  registry.register("finalize", {
    id: "chiplet.finalize.v1",
    async run() {
      ranDownstream = true;
      return { ok: true };
    },
  });
  const statuses = {};
  await executeWorkflowDag({
    graph: {
      workflow_id: "w_review",
      nodes: [
        { id: "n1", type: "manual_review" },
        { id: "n2", type: "finalize" },
      ],
      edges: [{ from: "n1", to: "n2", when: { field: "approved", op: "eq", value: true } }],
    },
    registry,
    ctx: { runId: "r2", workflowId: "w_review", payload: {} },
    buildEnvelope,
    onNodeSuccess(node, out) {
      statuses[node.id] = String(out?.status || "done");
    },
  });
  assert.equal(ranDownstream, false);
  assert.equal(statuses.n2, "skipped");
});
