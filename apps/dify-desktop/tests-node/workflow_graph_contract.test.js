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
