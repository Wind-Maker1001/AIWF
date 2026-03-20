const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadStoreSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/store-support.js")).href;
  return import(file);
}

test("workflow store support derives next ids and normalizes imported graphs", async () => {
  const {
    nextNodeIdFromNodes,
    normalizeImportedGraph,
    wouldGraphCreateCycle,
  } = await loadStoreSupportModule();

  assert.equal(nextNodeIdFromNodes([{ id: "n1" }, { id: "n3" }]), "n4");

  const graph = normalizeImportedGraph({
    workflow_id: "w1",
    name: "demo",
    nodes: [{ id: "n1", type: "load_rows_v2" }, { type: "aggregate_rows_v2" }],
    edges: [{ from: "n1", to: "n2" }, { from: "n2", to: "n2" }, { from: "x", to: "n1" }],
  });
  assert.equal(graph.workflow_id, "w1");
  assert.equal(graph.nodes.length, 2);
  assert.equal(graph.edges.length, 1);
  assert.equal(graph.edges[0].from, "n1");
  assert.equal(graph.edges[0].to, "n2");

  assert.equal(
    wouldGraphCreateCycle(
      [{ id: "n1" }, { id: "n2" }],
      [{ from: "n1", to: "n2" }],
      "n2",
      "n1"
    ),
    true
  );
});
