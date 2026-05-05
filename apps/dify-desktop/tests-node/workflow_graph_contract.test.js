const test = require("node:test");
const assert = require("node:assert/strict");
const workflowGraph = require("../workflow_graph");

test("main workflow graph normalization carries version and reports migration", () => {
  const normalized = workflowGraph.normalizeWorkflow({
    workflow: {
      workflow_id: "wf_main_contract",
      nodes: [{ id: "n1", type: "ingest_files" }],
      edges: [],
    },
  }, {
    allowLegacyWorkflowAlias: true,
  });

  assert.equal(normalized.graph.version, "1.0.0");
  assert.equal(normalized.contract.migrated, true);
  const ordered = workflowGraph.topoSort(
    [
      { id: "n1", type: "ingest_files" },
      { id: "n2", type: "md_output" },
      { id: "n3", type: "ai_audit" },
    ],
    [
      { from: "n1", to: "n2" },
      { from: "n2", to: "n3" },
    ],
  );
  assert.deepEqual(ordered.map((node) => node.id), ["n1", "n2", "n3"]);
});

test("workflow graph helper only accepts legacy workflow alias when explicitly allowed", () => {
  const payload = {
    workflow: {
      workflow_id: "wf_legacy_alias",
      version: "1.0.0",
      nodes: [{ id: "n1", type: "ingest_files" }],
      edges: [],
    },
  };

  assert.equal(workflowGraph.resolveWorkflowDefinitionPayload(payload), null);
  assert.equal(
    workflowGraph.resolveWorkflowDefinitionPayload(payload, { allowLegacyWorkflowAlias: true }).workflow_id,
    "wf_legacy_alias",
  );

  assert.deepEqual(workflowGraph.normalizeWorkflowPayloadShape(payload), payload);
  const normalized = workflowGraph.normalizeWorkflowPayloadShape(payload, null, { allowLegacyWorkflowAlias: true });
  assert.equal(normalized.workflow_definition.workflow_id, "wf_legacy_alias");
  assert.equal(Object.prototype.hasOwnProperty.call(normalized, "workflow"), false);
});
