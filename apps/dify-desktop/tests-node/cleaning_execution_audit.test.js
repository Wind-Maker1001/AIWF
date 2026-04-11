const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const {
  executionAuditFields,
  appendRunModeAuditEntry,
  buildCleaningShadowRolloutEvidence,
} = require("../cleaning_execution_audit");

test("executionAuditFields extracts stable cleaning rollout fields", () => {
  const fields = executionAuditFields({
    quality: { rust_v2_used: false },
    execution: {
      execution_mode: "python_legacy",
      eligibility_reason: "eligible",
      requested_rust_v2_mode: "shadow",
      effective_rust_v2_mode: "shadow",
      verify_on_default: false,
      shadow_compare: { status: "matched", mismatch_count: 0 },
    },
  });
  assert.deepEqual(fields, {
    execution_mode: "python_legacy",
    execution_eligibility_reason: "eligible",
    requested_rust_v2_mode: "shadow",
    effective_rust_v2_mode: "shadow",
    verify_on_default: false,
    shadow_compare_status: "matched",
    shadow_compare_mismatch_count: 0,
    rust_v2_used: false,
    template: "",
    template_expected_profile: "",
    requested_profile: "",
    recommended_profile: "",
    profile_mismatch: false,
    blocking_reason_codes: [],
    blank_output_expected: undefined,
    zero_output_unexpected: undefined,
  });
});

test("appendRunModeAuditEntry writes one normalized jsonl line", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-run-mode-audit-"));
  const filePath = path.join(root, "logs", "run_mode_audit.jsonl");
  const entry = appendRunModeAuditEntry({
    fs,
    path,
    filePath,
    mode: "offline_local",
    startedAt: Date.now() - 25,
    result: {
      ok: true,
      job_id: "job-1",
      quality: { rust_v2_used: true },
      execution: {
        execution_mode: "rust_v2",
        eligibility_reason: "eligible",
        requested_rust_v2_mode: "shadow",
        effective_rust_v2_mode: "shadow",
        verify_on_default: false,
        shadow_compare: { status: "matched", mismatch_count: 0 },
      },
    },
  });
  const lines = fs.readFileSync(filePath, "utf8").trim().split(/\r?\n/);
  assert.equal(lines.length, 1);
  const parsed = JSON.parse(lines[0]);
  assert.equal(parsed.mode, "offline_local");
  assert.equal(parsed.execution_mode, "rust_v2");
  assert.equal(parsed.requested_rust_v2_mode, "shadow");
  assert.equal(parsed.shadow_compare_status, "matched");
  assert.equal(parsed.requested_profile, "");
  assert.deepEqual(parsed.blocking_reason_codes, []);
  assert.equal(entry.execution_mode, "rust_v2");
});

test("buildCleaningShadowRolloutEvidence produces stable acceptance evidence", () => {
  const evidence = buildCleaningShadowRolloutEvidence({
    acceptance: "desktop_real_sample",
    runModeAuditPath: "D:/tmp/run_mode_audit.jsonl",
    sampleResultPath: "D:/tmp/cleaning_result.json",
    reportPath: "D:/tmp/acceptance_report.md",
    result: {
      ok: true,
      job_id: "job-2",
      quality: {
        rust_v2_used: false,
        input_rows: 10,
        output_rows: 8,
        invalid_rows: 1,
        filtered_rows: 1,
        duplicate_rows_removed: 0,
      },
      execution: {
        execution_mode: "python_legacy",
        eligibility_reason: "eligible",
        requested_rust_v2_mode: "shadow",
        effective_rust_v2_mode: "shadow",
        verify_on_default: false,
        execution_audit: { schema: "python_cleaning.audit.v1" },
        shadow_compare: {
          status: "matched",
          matched: true,
          mismatch_count: 0,
          mismatches: [],
          skipped_reason: "",
          compare_fields: ["rows", "quality", "reason_counts"],
        },
      },
    },
  });
  assert.equal(evidence.schema, "cleaning_shadow_rollout.acceptance.v1");
  assert.equal(evidence.acceptance, "desktop_real_sample");
  assert.equal(evidence.requested_rust_v2_mode, "shadow");
  assert.equal(evidence.shadow_compare.status, "matched");
  assert.equal(evidence.execution.execution_audit_schema, "python_cleaning.audit.v1");
  assert.equal(evidence.quality_summary.requested_profile, "");
  assert.deepEqual(evidence.quality_summary.blocking_reason_codes, []);
});
