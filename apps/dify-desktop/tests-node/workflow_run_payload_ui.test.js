const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadRunPayloadUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/run-payload-ui.js")).href;
  return import(file);
}

test("workflow run payload ui syncs workflow name into exported graph", async () => {
  const { createWorkflowRunPayloadUi } = await loadRunPayloadUiModule();
  const names = [];
  const ui = createWorkflowRunPayloadUi({
    workflowName: { value: "Flow Alpha" },
  }, {
    store: {
      setWorkflowName: (name) => names.push(name),
      exportGraph: () => ({ workflow_id: "wf_1", version: "1.0.0", nodes: [], edges: [] }),
    },
  });

  const graph = ui.graphPayload();

  assert.deepEqual(names, ["Flow Alpha"]);
  assert.deepEqual(graph, { workflow_id: "wf_1", version: "1.0.0", nodes: [], edges: [] });
});

test("workflow run payload ui builds run payload and merges extra params", async () => {
  const { createWorkflowRunPayloadUi } = await loadRunPayloadUiModule();
  const ui = createWorkflowRunPayloadUi({
    workflowName: { value: "Flow Beta" },
    qualityRuleSetId: { value: "rule_set_1" },
    qualityRuleSetSelect: { value: "" },
    reportTitle: { value: "Quarterly Report" },
    inputFiles: { value: "D:/input/a.pdf" },
    exportCanonicalBundle: { checked: true },
    canonicalTitle: { value: "" },
    breakpointNodeId: { value: "n2" },
    aiEndpoint: { value: "https://api.example.com" },
    aiKey: { value: "sk-demo" },
    aiModel: { value: "model-x" },
    rustEndpoint: { value: "http://localhost:8000" },
    rustRequired: { checked: true },
    chipletIsolationEnabled: { checked: false },
    chipletIsolationMode: { value: "all" },
    chipletIsolatedTypes: { value: "a;b, c " },
    sandboxAutoFixEnabled: { checked: true },
    sandboxAutoFixPauseQueue: { checked: false },
    sandboxAutoFixRequireReview: { checked: true },
    sandboxAutoFixForceIsolation: { checked: true },
    sandboxAutoFixRedThreshold: { value: "5" },
    sandboxAutoFixWindowSec: { value: "120" },
    sandboxAutoFixForceMinutes: { value: "30" },
    sandboxAutoFixForceMode: { value: "process" },
    sandboxMaxDurationMs: { value: "2000" },
    sandboxMaxCpuMs: { value: "3000" },
    sandboxMaxRssMb: { value: "256" },
    sandboxMaxOutputBytes: { value: "4096" },
  }, {
    store: {
      setWorkflowName: () => {},
      exportGraph: () => ({
        workflow_id: "wf_2",
        version: "1.0.0",
        nodes: [{ id: "n1", type: "ingest_files" }],
        edges: [],
      }),
    },
    sandboxDedupWindowSec: () => 42,
  });

  const payload = ui.runPayload({
    params: { strict_output_gate: true },
    extra_flag: 1,
  });

  assert.equal(payload.workflow_id, "wf_2");
  assert.equal(payload.workflow_version, "1.0.0");
  assert.equal(payload.run_request_kind, "draft");
  assert.equal(payload.workflow_definition_source, "draft_inline");
  assert.deepEqual(payload.workflow_definition, {
    workflow_id: "wf_2",
    version: "1.0.0",
    nodes: [{ id: "n1", type: "ingest_files" }],
    edges: [],
  });
  assert.equal(payload.quality_rule_set_id, "rule_set_1");
  assert.equal(payload.params.report_title, "Quarterly Report");
  assert.equal(payload.params.input_files, "D:/input/a.pdf");
  assert.equal(payload.params.md_only, true);
  assert.equal(payload.params.paper_markdown_enabled, true);
  assert.equal(payload.params.export_canonical_bundle, true);
  assert.equal(typeof payload.params.canonical_title, "string");
  assert.equal(payload.params.ocr_lang, "chi_sim+eng");
  assert.equal(payload.params.strict_output_gate, true);
  assert.deepEqual(payload.chiplet_isolated_types, ["a", "b", "c"]);
  assert.equal(payload.sandbox_alert_dedup_window_sec, 42);
  assert.deepEqual(payload.sandbox_limits, {
    max_duration_ms: 2000,
    max_cpu_ms: 3000,
    max_rss_mb: 256,
    max_output_bytes: 4096,
  });
  assert.equal(payload.extra_flag, 1);
});
