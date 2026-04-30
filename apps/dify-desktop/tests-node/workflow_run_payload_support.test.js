const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadRunPayloadSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/run-payload-support.js")).href;
  return import(file);
}

test("workflow run payload support parses isolated types and positive limits", async () => {
  const {
    parseChipletIsolatedTypes,
    positiveIntegerOrDefault,
  } = await loadRunPayloadSupportModule();

  assert.deepEqual(parseChipletIsolatedTypes("a;b, c "), ["a", "b", "c"]);
  assert.equal(positiveIntegerOrDefault("42", 1), 42);
  assert.equal(positiveIntegerOrDefault("0", 9), 9);
  assert.equal(positiveIntegerOrDefault("bad", 7), 7);
});

test("workflow run payload support builds base payload and merges params", async () => {
  const {
    buildDraftRunPayload,
    buildBaseRunPayload,
    buildReferenceRunRequest,
    mergeRunPayload,
  } = await loadRunPayloadSupportModule();

  const base = buildDraftRunPayload({
    qualityRuleSetId: { value: "set_1" },
    reportTitle: { value: "Report" },
    inputFiles: { value: "D:/a.pdf" },
    exportCanonicalBundle: { checked: true },
    canonicalTitle: { value: "" },
    breakpointNodeId: { value: "n1" },
    aiEndpoint: { value: "https://api.example.com" },
    aiKey: { value: "sk" },
    aiModel: { value: "m" },
    rustEndpoint: { value: "http://localhost:8000" },
    rustRequired: { checked: true },
    chipletIsolationEnabled: { checked: false },
    chipletIsolationMode: { value: "all" },
    chipletIsolatedTypes: { value: "x,y" },
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
  }, { workflow_id: "wf_1", version: "1.0.0" }, 42);

  const merged = mergeRunPayload(base, { params: { strict_output_gate: true }, extra_flag: 1 });
  const legacyAlias = buildBaseRunPayload({}, { workflow_id: "wf_legacy", version: "1.0.0" }, 600);
  const reference = buildReferenceRunRequest({ published_version_id: "ver_pub_1" }, { params: { region: "cn" } });
  assert.equal(merged.run_request_kind, "draft");
  assert.equal(merged.workflow_definition_source, "draft_inline");
  assert.equal(merged.workflow_id, "wf_1");
  assert.equal(merged.workflow_version, "1.0.0");
  assert.equal(merged.workflow_definition.version, "1.0.0");
  assert.equal(merged.quality_rule_set_id, "set_1");
  assert.deepEqual(merged.chiplet_isolated_types, ["x", "y"]);
  assert.equal(merged.sandbox_alert_dedup_window_sec, 42);
  assert.equal(merged.params.strict_output_gate, true);
  assert.equal(merged.extra_flag, 1);
  assert.equal(legacyAlias.run_request_kind, "draft");
  assert.equal(reference.run_request_kind, "reference");
  assert.equal(reference.workflow_definition_source, "version_reference");
  assert.equal(reference.version_id, "ver_pub_1");
  assert.equal(reference.published_version_id, "ver_pub_1");
  assert.deepEqual(reference.params, { region: "cn" });
});

test("workflow run payload support keeps unknown node types in payload and defers authority to runtime validation", async () => {
  const {
    buildBaseRunPayload,
  } = await loadRunPayloadSupportModule();

  const out = buildBaseRunPayload({}, {
    workflow_id: "wf_bad_type",
    version: "1.0.0",
    nodes: [{ id: "n1", type: "unknown_future_node" }],
    edges: [],
  });

  assert.equal(out.workflow_definition.workflow_id, "wf_bad_type");
  assert.equal(out.workflow_definition.nodes[0].type, "unknown_future_node");
});
