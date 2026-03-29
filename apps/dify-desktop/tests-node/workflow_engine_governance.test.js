const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { runMinimalWorkflow } = require("../workflow_engine");

function makeTmpOutDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-governance-"));
}

function configWithAuthoritativeValidation() {
  return {
    workflowValidationSupport: {
      validateWorkflowDefinitionAuthoritatively: async ({ workflowDefinition }) => ({
        ok: true,
        normalized_workflow_definition: {
          ...workflowDefinition,
          workflow_id: String(workflowDefinition?.workflow_id || "custom_v1"),
          version: String(workflowDefinition?.version || "1.0.0"),
          nodes: Array.isArray(workflowDefinition?.nodes) ? workflowDefinition.nodes : [],
          edges: Array.isArray(workflowDefinition?.edges) ? workflowDefinition.edges : [],
        },
        notes: [],
      }),
    },
  };
}

test("runMinimalWorkflow blocks forbidden nodes by role policy", async () => {
  const out = await runMinimalWorkflow({
    payload: {
      actor_role: "reviewer",
      chiplet_isolation_enabled: false,
      workflow: {
        workflow_id: "w_forbidden",
        nodes: [{ id: "n1", type: "ai_refine", config: { reuse_existing: false } }],
        edges: [],
      },
    },
    config: configWithAuthoritativeValidation(),
    outputRoot: makeTmpOutDir(),
    nodeCache: null,
  });
  assert.equal(out.ok, false);
  assert.equal(out.status, "forbidden_graph");
  assert.match(String(out.error || ""), /governance_forbidden_nodes/i);
});

test("runMinimalWorkflow emits governance input classification", async () => {
  const out = await runMinimalWorkflow({
    payload: {
      actor_role: "owner",
      chiplet_isolation_enabled: false,
      params: { input_files: ["D:/data/finance.xlsx", "D:/paper/report.pdf"] },
      workflow: {
        workflow_id: "w_classify",
        nodes: [{ id: "n1", type: "ingest_files", config: {} }],
        edges: [],
      },
    },
    config: configWithAuthoritativeValidation(),
    outputRoot: makeTmpOutDir(),
    nodeCache: null,
  });
  assert.equal(out.ok, true);
  assert.equal(Number(out?.governance?.input_classes?.data_files || 0), 1);
  assert.equal(Number(out?.governance?.input_classes?.doc_files || 0), 1);
});

test("runMinimalWorkflow enforces ai budget max calls per run", async () => {
  const out = await runMinimalWorkflow({
    payload: {
      actor_role: "owner",
      chiplet_isolation_enabled: false,
      ai: {
        allow_on_data: true,
      },
      governance: {
        ai_budget: {
          enabled: true,
          max_calls_per_run: 1,
          max_estimated_tokens_per_run: 999999,
          max_estimated_cost_usd_per_run: 99,
        },
      },
      workflow: {
        workflow_id: "w_ai_budget",
        nodes: [
          { id: "n1", type: "ai_refine", config: { reuse_existing: false } },
          { id: "n2", type: "ai_refine", config: { reuse_existing: false } },
        ],
        edges: [{ from: "n1", to: "n2" }],
      },
    },
    config: configWithAuthoritativeValidation(),
    outputRoot: makeTmpOutDir(),
    nodeCache: null,
  });
  assert.equal(out.ok, false);
  assert.equal(out.status, "failed");
  assert.match(String(out.error || ""), /ai_budget_exceeded:calls/i);
});

test("runMinimalWorkflow fails closed when Rust-authoritative validation is unavailable", async () => {
  const out = await runMinimalWorkflow({
    payload: {
      actor_role: "owner",
      chiplet_isolation_enabled: false,
      workflow: {
        workflow_id: "wf_unavailable",
        nodes: [{ id: "n1", type: "ingest_files", config: {} }],
        edges: [],
      },
    },
    config: {
      workflowValidationSupport: {
        validateWorkflowDefinitionAuthoritatively: async () => {
          const error = new Error("workflow validation unavailable: connection refused");
          error.remote_payload = {
            ok: false,
            error: "workflow validation unavailable: connection refused",
            error_code: "workflow_validation_unavailable",
            validation_scope: "run",
          };
          throw error;
        },
      },
    },
    outputRoot: makeTmpOutDir(),
    nodeCache: null,
  });

  assert.equal(out.ok, false);
  assert.equal(out.status, "workflow_validation_unavailable");
  assert.equal(out.error_code, "workflow_validation_unavailable");
  assert.match(String(out.error || ""), /workflow validation unavailable/i);
});
