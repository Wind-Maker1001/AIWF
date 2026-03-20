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
    { level: "error", kind: "graph", message: "bad graph" },
    { level: "warning", kind: "graph", message: "warn graph" },
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
