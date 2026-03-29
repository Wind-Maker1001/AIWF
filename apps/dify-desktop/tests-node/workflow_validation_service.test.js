const test = require("node:test");
const assert = require("node:assert/strict");

const { createWorkflowValidationSupport } = require("../workflow_validation_service");

function okResponse(payload) {
  return {
    ok: true,
    status: 200,
    async text() {
      return JSON.stringify(payload);
    },
  };
}

test("workflow validation service returns normalized workflow on Rust success", async () => {
  const support = createWorkflowValidationSupport({
    loadConfig: () => ({ accelUrl: "http://127.0.0.1:18082" }),
    fetchImpl: async (url, init) => {
      assert.equal(url, "http://127.0.0.1:18082/operators/workflow_contract_v1/validate");
      const body = JSON.parse(String(init.body || "{}"));
      assert.equal(body.validation_scope, "run");
      return okResponse({
        ok: true,
        status: "done",
        valid: true,
        normalized_workflow_definition: {
          workflow_id: "wf_ok",
          version: "1.0.0",
          nodes: [{ id: "n1", type: "ingest_files" }],
          edges: [],
        },
        notes: [],
      });
    },
  });

  const out = await support.validateWorkflowDefinitionAuthoritatively({
    workflowDefinition: { workflow_id: "wf_ok", nodes: [], edges: [] },
    requireNonEmptyNodes: true,
    validationScope: "run",
  });

  assert.equal(out.ok, true);
  assert.equal(out.normalized_workflow_definition.workflow_id, "wf_ok");
  assert.equal(out.normalized_workflow_definition.version, "1.0.0");
});

test("workflow validation service surfaces Rust-originated validation items", async () => {
  const support = createWorkflowValidationSupport({
    fetchImpl: async () => okResponse({
      ok: true,
      status: "invalid",
      valid: false,
      error_items: [{
        path: "workflow.nodes",
        code: "unknown_node_type",
        message: "workflow contains unregistered node types: unknown_future_node",
      }],
    }),
  });

  await assert.rejects(
    () => support.validateWorkflowDefinitionAuthoritatively({
      workflowDefinition: {
        workflow_id: "wf_bad",
        version: "1.0.0",
        nodes: [{ id: "n1", type: "unknown_future_node" }],
        edges: [],
      },
      validationScope: "publish",
    }),
    /unknown_future_node/i,
  );
});

test("workflow validation service fails closed on Rust unavailability", async () => {
  const support = createWorkflowValidationSupport({
    fetchImpl: async () => {
      throw new Error("connect ECONNREFUSED");
    },
  });

  await assert.rejects(
    () => support.validateWorkflowDefinitionAuthoritatively({
      workflowDefinition: {
        workflow_id: "wf_unavailable",
        version: "1.0.0",
        nodes: [{ id: "n1", type: "ingest_files" }],
        edges: [],
      },
      validationScope: "authoring",
    }),
    /workflow validation unavailable/i,
  );
});
