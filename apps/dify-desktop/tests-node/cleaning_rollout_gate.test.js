const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function writeJsonl(filePath, entries) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${entries.map((entry) => JSON.stringify(entry)).join("\n")}\n`, "utf8");
}

function makeFixture(overrides = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-cleaning-rollout-"));
  const outDir = path.join(root, "out");
  const sampleResultPath = path.join(root, "sample_result.json");
  const runModeAuditPath = path.join(root, "run_mode_audit.jsonl");
  const consistencyReportPath = path.join(root, "sidecar_python_rust_consistency_report.json");
  const reportPath = path.join(root, "acceptance_report.md");
  const evidencePath = path.join(root, "cleaning_shadow_rollout.json");
  fs.writeFileSync(reportPath, "# acceptance\n", "utf8");

  const sampleResult = overrides.sampleResult || {
    ok: true,
    job_id: "job-shadow-1",
    quality: { rust_v2_used: true, input_rows: 1, output_rows: 1 },
    execution: {
      execution_mode: "rust_v2",
      eligibility_reason: "eligible",
      requested_rust_v2_mode: "default",
      effective_rust_v2_mode: "default",
      verify_on_default: true,
      shadow_compare: {
        status: "matched",
        matched: true,
        mismatch_count: 0,
        mismatches: [],
        skipped_reason: "",
        compare_fields: ["rows", "quality", "reason_counts"],
      },
    },
  };
  const runModeAuditEntries = overrides.runModeAuditEntries || [{
    mode: "offline_local",
    execution_mode: "rust_v2",
    execution_eligibility_reason: "eligible",
    requested_rust_v2_mode: "default",
    effective_rust_v2_mode: "default",
    verify_on_default: true,
    shadow_compare_status: "matched",
    shadow_compare_mismatch_count: 0,
    rust_v2_used: true,
  }];
  const consistencyReport = overrides.consistencyReport || {
    ok: true,
    skipped: [],
    items: [{ id: "case_ok", mismatches: [] }],
  };
  const evidence = overrides.evidence || {
    schema: "cleaning_shadow_rollout.acceptance.v1",
    acceptance: "desktop_real_sample",
    ok: true,
    requested_rust_v2_mode: "default",
    effective_rust_v2_mode: "default",
    verify_on_default: true,
    run_mode_audit_path: runModeAuditPath,
    sample_result_path: sampleResultPath,
    acceptance_report_path: reportPath,
    execution: { execution_mode: "rust_v2" },
    shadow_compare: {
      status: "matched",
      matched: true,
      mismatch_count: 0,
      mismatches: [],
      skipped_reason: "",
      compare_fields: ["rows", "quality", "reason_counts"],
    },
  };

  writeJson(sampleResultPath, sampleResult);
  writeJsonl(runModeAuditPath, runModeAuditEntries);
  writeJson(consistencyReportPath, consistencyReport);
  if (!overrides.skipEvidenceWrite) {
    writeJson(evidencePath, evidence);
  }
  return { root, outDir, sampleResultPath, runModeAuditPath, consistencyReportPath, evidencePath };
}

function runGate(fixture, extraArgs = []) {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.join(repoRoot, "ops", "scripts", "check_cleaning_rust_v2_rollout.ps1");
  return spawnSync(
    "powershell",
    [
      "-ExecutionPolicy",
      "Bypass",
      "-File",
      script,
      "-RepoRoot",
      repoRoot,
      "-OutDir",
      fixture.outDir,
      "-ConsistencyReportPath",
      fixture.consistencyReportPath,
      "-EvidencePath",
      fixture.evidencePath,
      "-RequestedMode",
      "default",
      "-VerifyOnDefault",
      "-RequireRealEvidence",
      "-RequireNoSkipped",
      ...extraArgs,
    ],
    {
      cwd: repoRoot,
      encoding: "utf8",
    },
  );
}

test("cleaning rollout gate passes with valid default verify acceptance evidence", () => {
  const fixture = makeFixture();
  const result = runGate(fixture);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const summaryPath = path.join(fixture.outDir, "cleaning_rust_v2_rollout_gate_latest.json");
  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  assert.equal(summary.status, "passed");
  assert.equal(summary.mode, "default");
  assert.equal(summary.synthetic_sample_used, false);
  assert.equal(summary.evidence_acceptance, "desktop_real_sample");
});

test("cleaning rollout gate fails when real evidence is missing", () => {
  const fixture = makeFixture({ skipEvidenceWrite: true });
  const result = runGate(fixture);
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}\n${result.stderr}`, /evidence/i);
});

test("cleaning rollout gate fails when evidence mode is not default", () => {
  const fixture = makeFixture({
    evidence: {
      schema: "cleaning_shadow_rollout.acceptance.v1",
      acceptance: "desktop_real_sample",
      ok: true,
      requested_rust_v2_mode: "shadow",
      effective_rust_v2_mode: "shadow",
      verify_on_default: false,
      run_mode_audit_path: path.join(os.tmpdir(), "ignore"),
      sample_result_path: path.join(os.tmpdir(), "ignore"),
      acceptance_report_path: path.join(os.tmpdir(), "ignore.md"),
      execution: { execution_mode: "python_legacy" },
      shadow_compare: { status: "matched" },
    },
  });
  const result = runGate(fixture);
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}\n${result.stderr}`, /requested_rust_v2_mode|effective_rust_v2_mode/i);
});

test("cleaning rollout gate fails when shadow_compare status is empty", () => {
  const reportRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-rollout-report-"));
  const reportPath = path.join(reportRoot, "acceptance_report.md");
  fs.writeFileSync(reportPath, "# acceptance\n", "utf8");
  const fixture = makeFixture();
  writeJson(fixture.evidencePath, {
    schema: "cleaning_shadow_rollout.acceptance.v1",
    acceptance: "desktop_real_sample",
    ok: true,
    requested_rust_v2_mode: "default",
    effective_rust_v2_mode: "default",
    verify_on_default: true,
    run_mode_audit_path: fixture.runModeAuditPath,
    sample_result_path: fixture.sampleResultPath,
    acceptance_report_path: reportPath,
    execution: { execution_mode: "rust_v2" },
    shadow_compare: { status: "" },
  });
  const result = runGate(fixture);
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}\n${result.stderr}`, /shadow_compare/i);
});

test("cleaning rollout gate fails when shadow_compare status is skipped", () => {
  const fixture = makeFixture();
  const reportPath = path.join(fixture.root, "acceptance_report.md");
  fs.writeFileSync(reportPath, "# acceptance\n", "utf8");
  writeJson(fixture.evidencePath, {
    schema: "cleaning_shadow_rollout.acceptance.v1",
    acceptance: "desktop_real_sample",
    ok: true,
    requested_rust_v2_mode: "default",
    effective_rust_v2_mode: "default",
    verify_on_default: true,
    run_mode_audit_path: fixture.runModeAuditPath,
    sample_result_path: fixture.sampleResultPath,
    acceptance_report_path: reportPath,
    execution: { execution_mode: "rust_v2" },
    shadow_compare: { status: "skipped" },
  });
  const result = runGate(fixture);
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}\n${result.stderr}`, /matched|shadow_compare/i);
});

test("cleaning rollout gate fails on reason_counts mismatches in consistency report", () => {
  const fixture = makeFixture({
    consistencyReport: {
      ok: false,
      skipped: [],
      items: [{ id: "case_reason_counts", mismatches: ["reason_counts[cast_failed]: python=0 rust=1"] }],
    },
  });
  const result = runGate(fixture);
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}\n${result.stderr}`, /reason_counts/i);
});
