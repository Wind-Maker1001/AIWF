const test = require("node:test");
const assert = require("node:assert/strict");

const { createWorkflowExecutionSupport } = require("../workflow_execution_service");

function okResponse(payload) {
  return {
    ok: true,
    status: 200,
    async text() {
      return JSON.stringify(payload);
    },
  };
}

test("workflow execution service preserves pending_review as a structured run result", async () => {
  const support = createWorkflowExecutionSupport({
    loadConfig: () => ({ accelUrl: "http://127.0.0.1:18082" }),
    fetchImpl: async (url, init) => {
      assert.equal(url, "http://127.0.0.1:18082/operators/workflow_draft_run_v1");
      const body = JSON.parse(String(init.body || "{}"));
      assert.equal(body.workflow_definition.workflow_id, "wf_review");
      return okResponse({
        ok: false,
        operator: "workflow_draft_run_v1",
        status: "pending_review",
        workflow_id: "wf_review",
        run_id: "run_review_1",
        execution: {
          status: "pending_review",
          run_id: "run_review_1",
          context: {},
          steps: [],
        },
        final_output: {
          status: "pending_review",
          pending_reviews: [{
            run_id: "run_review_1",
            workflow_id: "wf_review",
            node_id: "review_gate",
            review_key: "review_gate",
            status: "pending",
          }],
        },
      });
    },
  });

  const out = await support.executeDraftWorkflowAuthoritatively({
    payload: {
      workflow_definition: {
        workflow_id: "wf_review",
        version: "1.0.0",
        nodes: [{ id: "n1", type: "manual_review" }],
        edges: [],
      },
    },
  });

  assert.equal(out.ok, false);
  assert.equal(out.status, "pending_review");
  assert.equal(out.run_id, "run_review_1");
  assert.equal(out.pending_reviews.length, 1);
  assert.equal(out.compatibility_fallback, false);
});

test("workflow execution service still fails closed on non-structured invalid responses", async () => {
  const support = createWorkflowExecutionSupport({
    fetchImpl: async () => okResponse({
      ok: false,
      error: "workflow reference execution invalid",
      error_code: "workflow_reference_execution_invalid",
    }),
  });

  await assert.rejects(
    () => support.executeReferenceWorkflowAuthoritatively({
      payload: {
        workflow_definition: {
          workflow_id: "wf_ref",
          version: "1.0.0",
          nodes: [{ id: "n1", type: "ingest_files" }],
          edges: [],
        },
        version_id: "ver_1",
        published_version_id: "ver_1",
      },
    }),
    /workflow reference execution invalid/i,
  );
});
