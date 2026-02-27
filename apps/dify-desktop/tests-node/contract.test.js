const test = require("node:test");
const assert = require("node:assert/strict");
const {
  validateChipletOutput,
  buildEnvelope,
  validateEnvelope,
  createChipletError,
  normalizeChipletError,
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

test("normalizeChipletError keeps structured fields", () => {
  const err = createChipletError("runner_timeout", "timeout on node");
  const n = normalizeChipletError(err);
  assert.equal(n.code, "runner_timeout");
  assert.equal(n.kind, "timeout");
  assert.equal(n.retryable, true);
});

test("normalizeChipletError infers timeout from plain error", () => {
  const n = normalizeChipletError(new Error("chiplet timeout: ingest_files"));
  assert.equal(n.code, "runner_timeout");
  assert.equal(n.kind, "timeout");
});
