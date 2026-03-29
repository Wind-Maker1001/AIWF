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

  const invalid = workflowGraph.validateGraph({
    workflow_id: "wf_invalid_contract",
    version: "1.0.0",
    nodes: [{ id: "n1", type: "ingest_files" }],
    edges: [{ from: "n1", to: "missing" }],
  });
  assert.equal(invalid.ok, false);
  assert.match(invalid.errors.join(" | "), /edge to does not exist/i);
  assert.ok(Array.isArray(invalid.error_items));
  assert.ok(invalid.error_items.some((item) => item.path === "workflow.edges[0].to" && item.code === "edge_missing_to_node"));

  const unknownType = workflowGraph.validateGraph({
    workflow_id: "wf_unknown_type_contract",
    version: "1.0.0",
    nodes: [{ id: "n1", type: "unknown_future_node" }],
    edges: [],
  });
  assert.equal(unknownType.ok, true);
  assert.deepEqual(unknownType.errors, []);
});
