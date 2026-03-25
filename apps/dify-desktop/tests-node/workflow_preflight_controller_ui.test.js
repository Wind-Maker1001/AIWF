const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPreflightControllerUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/preflight-controller-ui.js")).href;
  return import(file);
}

test("workflow preflight controller auto-fixes duplicate, broken, self-loop and isolated graph items", async () => {
  const { createWorkflowPreflightControllerUi } = await loadPreflightControllerUiModule();
  const applied = [];
  const controller = createWorkflowPreflightControllerUi({}, {
    exportGraph: () => ({
      nodes: [
        { id: "n1", type: "ingest_files" },
        { id: "n2", type: "clean_md" },
        { id: "n3", type: "md_output" },
        { id: "n4", type: "md_output" },
      ],
      edges: [
        { from: "n1", to: "n2" },
        { from: "n1", to: "n2" },
        { from: "n2", to: "n2" },
        { from: "n2", to: "missing" },
        { from: "n2", to: "n3" },
      ],
    }),
    applyGraph: (graph) => applied.push(graph),
  });

  const out = controller.autoFixGraphStructure();

  assert.equal(out.changed, true);
  assert.equal(out.removed_dup_edges, 1);
  assert.equal(out.removed_self_loops, 1);
  assert.equal(out.removed_broken_edges, 1);
  assert.equal(out.removed_isolated_nodes, 1);
  assert.deepEqual(applied, [{
    nodes: [
      { id: "n1", type: "ingest_files" },
      { id: "n2", type: "clean_md" },
      { id: "n3", type: "md_output" },
    ],
    edges: [
      { from: "n1", to: "n2" },
      { from: "n2", to: "n3" },
    ],
  }]);
});

test("workflow preflight controller reports missing rust endpoint for compatible nodes", async () => {
  const { createWorkflowPreflightControllerUi } = await loadPreflightControllerUiModule();
  const rendered = [];
  let lastReport = null;
  const controller = createWorkflowPreflightControllerUi({
    rustEndpoint: { value: "" },
    rustRequired: { checked: true },
    inputFiles: { value: "D:/input.csv" },
  }, {
    graphPayload: () => ({
      workflow_id: "wf_preflight",
      version: "1.0.0",
      nodes: [
        { id: "n1", type: "transform_rows_v3", config: {} },
        { id: "n2", type: "md_output", config: {} },
      ],
      edges: [{ from: "n1", to: "n2" }],
    }),
    computePreflightRisk: (issues) => ({ score: issues.length, level: "high", label: "高风险" }),
    renderPreflightReport: (report) => rendered.push(report),
    setLastPreflightReport: (report) => { lastReport = report; },
  });

  const report = await controller.runWorkflowPreflight();

  assert.equal(report.ok, false);
  assert.match(report.issues[0].message, /Rust Endpoint/);
  assert.equal(lastReport, report);
  assert.equal(rendered.length, 1);
});

test("workflow preflight controller reports unregistered node types before runtime", async () => {
  const { createWorkflowPreflightControllerUi } = await loadPreflightControllerUiModule();
  let lastReport = null;
  const rendered = [];
  const controller = createWorkflowPreflightControllerUi({
    rustEndpoint: { value: "" },
    rustRequired: { checked: false },
    inputFiles: { value: "" },
  }, {
    graphPayload: () => ({
      workflow_id: "wf_unknown_preflight",
      version: "1.0.0",
      nodes: [
        { id: "n1", type: "unknown_future_node", config: {} },
      ],
      edges: [],
    }),
    computePreflightRisk: (issues) => ({ score: issues.length, level: "high", label: "高风险" }),
    renderPreflightReport: (report) => rendered.push(report),
    setLastPreflightReport: (report) => { lastReport = report; },
  });

  const report = await controller.runWorkflowPreflight();

  assert.equal(report.ok, false);
  assert.match(report.issues.map((item) => item.message).join(" | "), /unregistered node types/i);
  assert.equal(lastReport, report);
  assert.equal(rendered.length, 1);
});
