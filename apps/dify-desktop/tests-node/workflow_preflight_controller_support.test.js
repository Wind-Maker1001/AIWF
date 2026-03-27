const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPreflightSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/preflight-controller-support.js")).href;
  return import(file);
}

test("workflow preflight support normalizes graph validation and rust context", async () => {
  const {
    buildGraphValidationIssues,
    collectRustPreflightContext,
  } = await loadPreflightSupportModule();

  assert.deepEqual(buildGraphValidationIssues({
    errors: ["bad graph"],
    warnings: ["warn graph"],
  }), [
    { level: "error", kind: "graph", message: "bad graph", error_code: "", error_path: "", error_contract: "", resolution_hint: "" },
    { level: "warning", kind: "graph", message: "warn graph", error_code: "", error_path: "", error_contract: "", resolution_hint: "" },
  ]);

  assert.deepEqual(buildGraphValidationIssues({
    errors: ["workflow contains unregistered node types: unknown_future_node"],
    error_items: [{ code: "unknown_node_type", path: "workflow.nodes", message: "workflow contains unregistered node types: unknown_future_node" }],
  }), [
    {
      level: "error",
      kind: "unknown_node_type",
      message: "workflow contains unregistered node types: unknown_future_node",
      error_code: "unknown_node_type",
      error_path: "workflow.nodes",
      contract_boundary: "node_catalog_truth",
      resolution_hint: "replace node type or sync Rust manifest / local node policy",
      action_text: "定位节点",
    },
  ]);

  assert.deepEqual(buildGraphValidationIssues({
    errors: ["workflow.version is required"],
    error_items: [{
      code: "required",
      path: "workflow.version",
      message: "workflow.version is required",
      error_contract: "contracts/desktop/node_config_validation_errors.v1.json",
    }],
  }), [
    {
      level: "error",
      kind: "graph_contract",
      message: "workflow.version is required",
      error_code: "required",
      error_path: "workflow.version",
      error_contract: "contracts/desktop/node_config_validation_errors.v1.json",
      resolution_hint: "请先把流程迁移到带顶层 version 的格式后再保存、运行或发布。",
    },
  ]);

  const rustCtx = collectRustPreflightContext({
    nodes: [
      { id: "n1", type: "transform_rows_v3" },
      { id: "n2", type: "clean_md" },
    ],
  }, {
    rustEndpoint: { value: "http://localhost:9000/" },
    rustRequired: { checked: true },
    inputFiles: { value: "a.csv\r\nb.csv" },
  });

  assert.equal(rustCtx.endpoint, "http://localhost:9000");
  assert.equal(rustCtx.rustRequired, true);
  assert.equal(rustCtx.rustNodes.length, 1);
  assert.equal(rustCtx.firstInputFile, "a.csv");
});

test("workflow preflight support auto-fixes duplicated broken self-loop and isolated graph items", async () => {
  const { autoFixWorkflowGraph } = await loadPreflightSupportModule();
  const out = autoFixWorkflowGraph({
    nodes: [
      { id: "a", type: "t" },
      { id: "b", type: "t" },
      { id: "c", type: "t" },
    ],
    edges: [
      { from: "a", to: "b" },
      { from: "a", to: "b" },
      { from: "b", to: "b" },
      { from: "x", to: "c" },
    ],
  });

  assert.equal(out.summary.changed, true);
  assert.equal(out.summary.removed_dup_edges, 1);
  assert.equal(out.summary.removed_self_loops, 1);
  assert.equal(out.summary.removed_broken_edges, 1);
  assert.equal(out.summary.removed_isolated_nodes, 1);
  assert.deepEqual(out.graph.nodes, [
    { id: "a", type: "t" },
    { id: "b", type: "t" },
  ]);
  assert.deepEqual(out.graph.edges, [{ from: "a", to: "b" }]);
});
