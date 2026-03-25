const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadStoreModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/store.js")).href;
  return import(file);
}

test("workflow store assigns default config for new rust nodes", async () => {
  const { createWorkflowStore } = await loadStoreModule();
  const store = createWorkflowStore();
  const id = store.addNode("join_rows_v2", 10, 20);
  const node = store.getNode(id);
  assert.ok(node);
  assert.equal(node.type, "join_rows_v2");
  assert.deepEqual(node.config.left_on, ["id"]);
  assert.deepEqual(node.config.right_on, ["id"]);
  assert.equal(node.config.join_type, "inner");

  const dsId = store.addNode("ds_refine", 30, 40);
  const dsNode = store.getNode(dsId);
  assert.ok(dsNode);
  assert.equal(dsNode.type, "ds_refine");
  assert.equal(dsNode.config.provider_name, "DeepSeek");
  assert.equal(dsNode.config.ai_model, "deepseek-chat");
});

test("workflow store import/export preserves node config", async () => {
  const { createWorkflowStore } = await loadStoreModule();
  const store = createWorkflowStore();
  store.importGraph({
    workflow_id: "w1",
    name: "x",
    nodes: [
      { id: "n1", type: "load_rows_v2", x: 1, y: 2, config: { source_type: "csv", source: "D:/a.csv", limit: 100 } },
      { id: "n2", type: "aggregate_rows_v2", x: 3, y: 4 },
    ],
    edges: [{ from: "n1", to: "n2" }],
  });

  const out = store.exportGraph();
  assert.equal(out.version, "1.0.0");
  assert.equal(out.nodes.length, 2);
  assert.equal(out.nodes[0].config.source, "D:/a.csv");
  assert.equal(out.nodes[1].type, "aggregate_rows_v2");
  assert.ok(Array.isArray(out.nodes[1].config.aggregates));

  const ok = store.updateNodeConfig("n2", { group_by: ["dept"], aggregates: [{ op: "count", as: "c" }] });
  assert.equal(ok, true);
  const n2 = store.getNode("n2");
  assert.deepEqual(n2.config.group_by, ["dept"]);
});

test("workflow store preserves edge when condition", async () => {
  const { createWorkflowStore } = await loadStoreModule();
  const store = createWorkflowStore();
  store.importGraph({
    workflow_id: "w_when",
    name: "edge-when",
    nodes: [
      { id: "n1", type: "manual_review", x: 1, y: 2 },
      { id: "n2", type: "md_output", x: 3, y: 4 },
    ],
    edges: [{ from: "n1", to: "n2", when: { field: "approved", op: "eq", value: true } }],
  });
  const e = store.getEdge("n1", "n2");
  assert.ok(e);
  assert.equal(e.when.field, "approved");
  assert.equal(store.updateEdgeWhen("n1", "n2", "output.ok"), true);
  const out = store.exportGraph();
  assert.equal(out.version, "1.0.0");
  assert.equal(out.edges[0].when, "output.ok");
});

test("workflow store rejects unregistered node types during authoring add", async () => {
  const { createWorkflowStore } = await loadStoreModule();
  const store = createWorkflowStore();

  assert.throws(
    () => store.addNode("unknown_future_node", 10, 20),
    /unregistered node types in add_node/i,
  );
});
