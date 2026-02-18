const test = require("node:test");
const assert = require("node:assert/strict");
const {
  validateChipletOutput,
  buildEnvelope,
  validateEnvelope,
} = require("../workflow_chiplets/contract");

test("validateEnvelope accepts v1 envelope", () => {
  const env = buildEnvelope({
    run_id: "r1",
    workflow_id: "w1",
    node_id: "n1",
    node_type: "clean_md",
  });
  validateEnvelope(env, { id: "n1", type: "clean_md" });
});

test("validateChipletOutput enforces clean_md fields", () => {
  assert.throws(
    () => validateChipletOutput({ job_id: "a", warnings: [] }, "clean_md"),
    /ai_corpus_path/
  );
  validateChipletOutput(
    { job_id: "a", ai_corpus_path: "x.md", warnings: [] },
    "clean_md"
  );
});

test("validateChipletOutput enforces ai_audit fields", () => {
  assert.throws(
    () => validateChipletOutput({ passed: true, reasons: [] }, "ai_audit"),
    /metrics_hash/
  );
  validateChipletOutput(
    { passed: true, reasons: [], metrics_hash: "m", ai_hash: "a" },
    "ai_audit"
  );
});

test("validateChipletOutput enforces compute_rust metric fields", () => {
  assert.throws(
    () => validateChipletOutput({ engine: "x", metrics: {}, rust_started: false }, "compute_rust"),
    /metrics\.sections/
  );
  validateChipletOutput({
    engine: "x",
    metrics: { sections: 1, bullets: 2, chars: 3, cjk: 0, latin: 3, sha256: "abc" },
    rust_started: false,
  }, "compute_rust");
});
