param(
  [string]$RepoRoot = "",
  [string]$OutDir = "",
  [string]$RunModeAuditPath = "",
  [string]$ConsistencyReportPath = "",
  [string]$SampleResultPath = "",
  [string]$EvidencePath = "",
  [ValidateSet("off", "shadow", "default")]
  [string]$RequestedMode = "",
  [switch]$VerifyOnDefault,
  [switch]$RequireNoSkipped,
  [switch]$RequireRealEvidence
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not $OutDir) {
  $OutDir = Join-Path $RepoRoot "ops\logs\cleaning_rollout"
}
if (-not $RunModeAuditPath) {
  $RunModeAuditPath = Join-Path $OutDir "run_mode_audit.jsonl"
}
if (-not $ConsistencyReportPath) {
  $ConsistencyReportPath = Join-Path $RepoRoot "ops\logs\regression\sidecar_python_rust_consistency_report.json"
}
if (-not $SampleResultPath) {
  $SampleResultPath = Join-Path $OutDir "cleaning_rollout_sample_result.json"
}
if (-not $RequestedMode) {
  $RequestedMode = if ([string]::IsNullOrWhiteSpace($env:AIWF_CLEANING_RUST_V2_MODE)) { "off" } else { [string]$env:AIWF_CLEANING_RUST_V2_MODE }
}

$nodeScript = @'
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

function normalizeMode(value) {
  const text = String(value || "off").trim().toLowerCase();
  return ["off", "shadow", "default"].includes(text) ? text : "off";
}

function toBool(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const text = String(value ?? "").trim().toLowerCase();
  if (!text) return fallback;
  return ["1", "true", "yes", "on"].includes(text);
}

function ensureDir(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function readLastJsonLine(filePath) {
  const raw = fs.readFileSync(filePath, "utf8").trim();
  const lines = raw.split(/\r?\n/).filter(Boolean);
  if (!lines.length) {
    throw new Error(`run_mode_audit empty: ${filePath}`);
  }
  return JSON.parse(lines[lines.length - 1]);
}

function buildShadowCompare(mode, verifyOnDefault) {
  if (mode === "off") {
    return {
      status: "skipped",
      matched: false,
      mismatch_count: 0,
      mismatches: [],
      skipped_reason: "mode_off",
      compare_fields: ["rows", "quality", "reason_counts"],
    };
  }
  if (mode === "default" && !verifyOnDefault) {
    return {
      status: "skipped",
      matched: false,
      mismatch_count: 0,
      mismatches: [],
      skipped_reason: "default_without_verify",
      compare_fields: ["rows", "quality", "reason_counts"],
    };
  }
  return {
    status: "matched",
    matched: true,
    mismatch_count: 0,
    mismatches: [],
    skipped_reason: "",
    compare_fields: ["rows", "quality", "reason_counts"],
  };
}

async function generateSample(repoRoot, runModeAuditPath, sampleResultPath, requestedMode, verifyOnDefault) {
  const { registerCleaningIpc } = require(path.join(repoRoot, "apps", "dify-desktop", "main_ipc_cleaning.js"));
  const handlers = new Map();
  const ipcMain = {
    handle(name, fn) {
      handlers.set(name, fn);
    },
  };
  const shadowCompare = buildShadowCompare(requestedMode, verifyOnDefault);
  const executionMode = requestedMode === "default" ? "rust_v2" : "python_legacy";
  const eligibilityReason = requestedMode === "off" ? "mode_off" : "eligible";
  const sampleResult = {
    ok: true,
    job_id: "cleaning-rollout-sample",
    rows: [{ id: 1, amount: 10 }],
    quality: {
      rust_v2_used: requestedMode === "default",
      input_rows: 1,
      output_rows: 1,
      invalid_rows: 0,
      filtered_rows: 0,
      duplicate_rows_removed: 0,
      required_missing_ratio: 0,
    },
    execution: {
      execution_mode: executionMode,
      execution_audit: {
        schema: requestedMode === "default" ? "transform_rows_v2.audit.v1" : "python_cleaning.audit.v1",
        reason_counts: {
          invalid_object: 0,
          cast_failed: 0,
          required_missing: 0,
          filter_rejected: 0,
          duplicate_removed: 0,
        },
        reason_samples: {
          invalid_object: [],
          cast_failed: [],
          required_missing: [],
          filter_rejected: [],
          duplicate_removed: [],
        },
        limits: { sample_limit: 5 },
      },
      eligibility_reason: eligibilityReason,
      requested_rust_v2_mode: requestedMode,
      effective_rust_v2_mode: requestedMode,
      verify_on_default: verifyOnDefault,
      shadow_compare: shadowCompare,
    },
  };

  ensureDir(runModeAuditPath);
  fs.writeFileSync(runModeAuditPath, "");

  registerCleaningIpc(
    {
      ipcMain,
      loadConfig: () => ({ mode: "offline_local" }),
      saveConfig: () => {},
      baseHealth: async () => ({ ok: true }),
      glueHealth: async () => ({ ok: true }),
      runOfflineCleaningInWorker: async () => sampleResult,
      runOfflinePrecheckInWorker: async () => ({ ok: true }),
      runOfflinePreviewInWorker: async () => ({ ok: true }),
      runViaBaseApi: async () => ({ ok: true }),
      listCleaningTemplates: () => ({ ok: true, templates: [] }),
    },
    {
      resolveOutputRoot: () => path.join(os.tmpdir(), "aiwf-cleaning-rollout-gate"),
      appendRunModeAudit(entry) {
        fs.appendFileSync(runModeAuditPath, `${JSON.stringify(entry)}\n`, "utf8");
      },
      classifyRemoteFailure: () => "not_used",
    },
  );

  const handler = handlers.get("aiwf:runCleaning");
  const out = await handler({}, { params: {} }, { mode: "offline_local" });
  ensureDir(sampleResultPath);
  fs.writeFileSync(sampleResultPath, `${JSON.stringify(out, null, 2)}\n`, "utf8");
  return out;
}

function validateShadowCompare(sampleResult, issues) {
  const execution = sampleResult && typeof sampleResult.execution === "object" ? sampleResult.execution : null;
  if (!execution) {
    issues.push("execution report missing from sample result");
    return {
      status: "",
      matched: false,
      mismatch_count: 0,
      compare_fields: [],
    };
  }
  const shadowCompare = execution.shadow_compare && typeof execution.shadow_compare === "object"
    ? execution.shadow_compare
    : null;
  if (!shadowCompare) {
    issues.push("execution.shadow_compare missing from sample result");
    return {
      status: "",
      matched: false,
      mismatch_count: 0,
      compare_fields: [],
    };
  }
  for (const field of ["status", "matched", "mismatch_count", "mismatches", "skipped_reason", "compare_fields"]) {
    if (!(field in shadowCompare)) {
      issues.push(`execution.shadow_compare missing field ${field}`);
    }
  }
  if (!Array.isArray(shadowCompare.mismatches)) {
    issues.push("execution.shadow_compare.mismatches must be an array");
  }
  if (!Array.isArray(shadowCompare.compare_fields)) {
    issues.push("execution.shadow_compare.compare_fields must be an array");
  }
  const requiredCompareFields = ["rows", "quality", "reason_counts"];
  for (const field of requiredCompareFields) {
    if (!Array.isArray(shadowCompare.compare_fields) || !shadowCompare.compare_fields.includes(field)) {
      issues.push(`execution.shadow_compare.compare_fields missing ${field}`);
    }
  }
  return {
    status: String(shadowCompare.status || ""),
    matched: !!shadowCompare.matched,
    mismatch_count: Number(shadowCompare.mismatch_count || 0),
    compare_fields: Array.isArray(shadowCompare.compare_fields) ? shadowCompare.compare_fields.slice() : [],
  };
}

function validateRunModeAudit(auditEntry, issues) {
  const requiredFields = [
    "execution_mode",
    "execution_eligibility_reason",
    "requested_rust_v2_mode",
    "effective_rust_v2_mode",
    "verify_on_default",
    "shadow_compare_status",
    "shadow_compare_mismatch_count",
    "rust_v2_used",
  ];
  const missing = [];
  for (const field of requiredFields) {
    if (!(field in auditEntry)) {
      missing.push(field);
      issues.push(`run_mode_audit missing field ${field}`);
    }
  }
  return {
    required_fields: requiredFields,
    missing_fields: missing,
  };
}

function readEvidence(filePath, issues) {
  if (!filePath) return null;
  if (!fs.existsSync(filePath)) {
    issues.push(`acceptance evidence missing: ${filePath}`);
    return null;
  }
  try {
    return readJson(filePath);
  } catch (error) {
    issues.push(`acceptance evidence unreadable: ${filePath}`);
    return null;
  }
}

function validateEvidence(evidence, requestedMode, verifyOnDefault, issues) {
  if (!evidence || typeof evidence !== "object") {
    return {
      acceptance: "",
      report_path: "",
    };
  }
  if (String(evidence.schema || "") !== "cleaning_shadow_rollout.acceptance.v1") {
    issues.push("acceptance evidence schema mismatch");
  }
  if (String(evidence.requested_rust_v2_mode || "") !== requestedMode) {
    issues.push(`acceptance evidence requested_rust_v2_mode mismatch: ${String(evidence.requested_rust_v2_mode || "")}`);
  }
  if (String(evidence.effective_rust_v2_mode || "") !== requestedMode) {
    issues.push(`acceptance evidence effective_rust_v2_mode mismatch: ${String(evidence.effective_rust_v2_mode || "")}`);
  }
  if (!!evidence.verify_on_default !== verifyOnDefault) {
    issues.push(`acceptance evidence verify_on_default mismatch: ${String(evidence.verify_on_default)}`);
  }
  const reportPath = String(evidence.acceptance_report_path || "");
  if (!reportPath) {
    issues.push("acceptance evidence missing acceptance_report_path");
  } else if (!fs.existsSync(reportPath)) {
    issues.push(`acceptance report missing: ${reportPath}`);
  }
  const shadowCompare = evidence.shadow_compare && typeof evidence.shadow_compare === "object" ? evidence.shadow_compare : {};
  const shadowStatus = String(shadowCompare.status || "").trim();
  if (!shadowStatus) {
    issues.push("acceptance evidence missing shadow_compare.status");
  } else if ((requestedMode === "shadow" || requestedMode === "default") && shadowStatus !== "matched") {
    issues.push(`acceptance evidence shadow_compare.status must be matched: ${shadowStatus}`);
  }
  const executionMode = String((evidence.execution && evidence.execution.execution_mode) || "").trim();
  if (requestedMode === "default" && executionMode !== "rust_v2") {
    issues.push(`acceptance evidence execution.execution_mode must be rust_v2: ${executionMode}`);
  }
  return {
    acceptance: String(evidence.acceptance || ""),
    report_path: reportPath,
  };
}

async function main() {
  const repoRoot = path.resolve(process.argv[2]);
  const outDir = path.resolve(process.argv[3]);
  const defaultRunModeAuditPath = path.join(outDir, "run_mode_audit.jsonl");
  const defaultSampleResultPath = path.join(outDir, "cleaning_rollout_sample_result.json");
  let runModeAuditPath = process.argv[4] ? path.resolve(process.argv[4]) : "";
  const consistencyReportPath = path.resolve(process.argv[5]);
  let sampleResultPath = process.argv[6] ? path.resolve(process.argv[6]) : "";
  const requestedMode = normalizeMode(process.argv[7]);
  const verifyOnDefault = toBool(process.argv[8], false);
  const requireNoSkipped = toBool(process.argv[9], false);
  const evidencePath = process.argv[10] ? path.resolve(process.argv[10]) : "";
  const requireRealEvidence = toBool(process.argv[11], false);

  fs.mkdirSync(outDir, { recursive: true });

  const issues = [];
  const evidence = readEvidence(evidencePath, issues);
  const evidenceSummary = validateEvidence(evidence, requestedMode, verifyOnDefault, issues);
  if (
    evidence &&
    evidence.run_mode_audit_path &&
    (
      !runModeAuditPath ||
      runModeAuditPath === defaultRunModeAuditPath ||
      !fs.existsSync(runModeAuditPath)
    )
  ) {
    runModeAuditPath = path.resolve(String(evidence.run_mode_audit_path));
  }
  if (
    evidence &&
    evidence.sample_result_path &&
    (
      !sampleResultPath ||
      sampleResultPath === defaultSampleResultPath ||
      !fs.existsSync(sampleResultPath)
    )
  ) {
    sampleResultPath = path.resolve(String(evidence.sample_result_path));
  }

  const hasRealInputs = !!(runModeAuditPath && sampleResultPath && fs.existsSync(runModeAuditPath) && fs.existsSync(sampleResultPath));
  if (!hasRealInputs) {
    if (requireRealEvidence) {
      if (!runModeAuditPath) {
        issues.push("real evidence mode missing run_mode_audit_path");
      } else if (!fs.existsSync(runModeAuditPath)) {
        issues.push(`run_mode_audit missing: ${runModeAuditPath}`);
      }
      if (!sampleResultPath) {
        issues.push("real evidence mode missing sample_result_path");
      } else if (!fs.existsSync(sampleResultPath)) {
        issues.push(`sample_result missing: ${sampleResultPath}`);
      }
    } else {
      if (!runModeAuditPath) {
        runModeAuditPath = path.join(outDir, "run_mode_audit.jsonl");
      }
      if (!sampleResultPath) {
        sampleResultPath = path.join(outDir, "cleaning_rollout_sample_result.json");
      }
      await generateSample(repoRoot, runModeAuditPath, sampleResultPath, requestedMode, verifyOnDefault);
    }
  }

  if (issues.length === 0) {
    const sampleResult = readJson(sampleResultPath);
    const latestShadowCompareSummary = validateShadowCompare(sampleResult, issues);
    const latestAuditEntry = readLastJsonLine(runModeAuditPath);
    const auditSummary = validateRunModeAudit(latestAuditEntry, issues);
    if (requestedMode === "shadow") {
      if (String(latestAuditEntry.requested_rust_v2_mode || "") !== "shadow") {
        issues.push(`run_mode_audit requested_rust_v2_mode mismatch: ${String(latestAuditEntry.requested_rust_v2_mode || "")}`);
      }
      if (String(latestAuditEntry.effective_rust_v2_mode || "") !== "shadow") {
        issues.push(`run_mode_audit effective_rust_v2_mode mismatch: ${String(latestAuditEntry.effective_rust_v2_mode || "")}`);
      }
      if (!!latestAuditEntry.verify_on_default !== false) {
        issues.push(`run_mode_audit verify_on_default mismatch: ${String(latestAuditEntry.verify_on_default)}`);
      }
      if (!String(latestAuditEntry.shadow_compare_status || "").trim()) {
        issues.push("run_mode_audit shadow_compare_status missing");
      } else if (String(latestAuditEntry.shadow_compare_status || "").trim() !== "matched") {
        issues.push(`run_mode_audit shadow_compare_status must be matched: ${String(latestAuditEntry.shadow_compare_status || "").trim()}`);
      }
    } else if (requestedMode === "default") {
      if (String(latestAuditEntry.requested_rust_v2_mode || "") !== "default") {
        issues.push(`run_mode_audit requested_rust_v2_mode mismatch: ${String(latestAuditEntry.requested_rust_v2_mode || "")}`);
      }
      if (String(latestAuditEntry.effective_rust_v2_mode || "") !== "default") {
        issues.push(`run_mode_audit effective_rust_v2_mode mismatch: ${String(latestAuditEntry.effective_rust_v2_mode || "")}`);
      }
      if (!!latestAuditEntry.verify_on_default !== true) {
        issues.push(`run_mode_audit verify_on_default mismatch: ${String(latestAuditEntry.verify_on_default)}`);
      }
      if (String(latestAuditEntry.execution_mode || "") !== "rust_v2") {
        issues.push(`run_mode_audit execution_mode must be rust_v2: ${String(latestAuditEntry.execution_mode || "")}`);
      }
      if (!String(latestAuditEntry.shadow_compare_status || "").trim()) {
        issues.push("run_mode_audit shadow_compare_status missing");
      } else if (String(latestAuditEntry.shadow_compare_status || "").trim() !== "matched") {
        issues.push(`run_mode_audit shadow_compare_status must be matched: ${String(latestAuditEntry.shadow_compare_status || "").trim()}`);
      }
    }

    let reasonCountsMismatchScenarios = [];
    let skipped = [];
    if (!fs.existsSync(consistencyReportPath)) {
      issues.push(`sidecar consistency report missing: ${consistencyReportPath}`);
    } else {
      const consistencyReport = readJson(consistencyReportPath);
      skipped = Array.isArray(consistencyReport.skipped) ? consistencyReport.skipped.map((item) => String(item)) : [];
      const items = Array.isArray(consistencyReport.items) ? consistencyReport.items : [];
      reasonCountsMismatchScenarios = items
        .filter((item) => {
          const mismatches = Array.isArray(item && item.mismatches) ? item.mismatches : [];
          return mismatches.some((entry) => /reason_counts/i.test(String(entry || "")));
        })
        .map((item) => String(item.id || item.scenario || "(unknown)"));
      if (reasonCountsMismatchScenarios.length > 0) {
        issues.push(`sidecar consistency report contains reason_counts mismatch: ${reasonCountsMismatchScenarios.join(", ")}`);
      }
      if (requireNoSkipped && skipped.length > 0) {
        issues.push(`sidecar consistency report contains skipped scenarios: ${skipped.join(", ")}`);
      }
    }

    const summary = {
      status: issues.length === 0 ? "passed" : "failed",
      ok: issues.length === 0,
      checked_at: new Date().toISOString(),
      mode: requestedMode,
      verify_on_default: verifyOnDefault,
      evidence_path: evidencePath,
      evidence_acceptance: evidenceSummary.acceptance,
      acceptance_report_path: evidenceSummary.report_path,
      run_mode_audit_path: runModeAuditPath,
      sample_result_path: sampleResultPath,
      sidecar_consistency_report_path: consistencyReportPath,
      synthetic_sample_used: !requireRealEvidence && !evidencePath && !hasRealInputs,
      latest_shadow_compare_summary: latestShadowCompareSummary,
      run_mode_audit: {
        required_fields: auditSummary.required_fields,
        missing_fields: auditSummary.missing_fields,
        latest_entry: latestAuditEntry,
      },
      reason_counts_mismatch_scenarios: reasonCountsMismatchScenarios,
      skipped,
      issues,
    };

    const latestJsonPath = path.join(outDir, "cleaning_rust_v2_rollout_gate_latest.json");
    const latestMdPath = path.join(outDir, "cleaning_rust_v2_rollout_gate_latest.md");
    const stamp = summary.checked_at.replace(/[:]/g, "").replace(/\..+$/, "").replace(/[-]/g, "");
    const snapshotJsonPath = path.join(outDir, `cleaning_rust_v2_rollout_gate_${stamp}.json`);
    const snapshotMdPath = path.join(outDir, `cleaning_rust_v2_rollout_gate_${stamp}.md`);
    const json = `${JSON.stringify(summary, null, 2)}\n`;
    fs.writeFileSync(latestJsonPath, json, "utf8");
    fs.writeFileSync(snapshotJsonPath, json, "utf8");

    const md = [
      "# Cleaning Rust v2 Rollout Gate",
      "",
      `- status: ${summary.status}`,
      `- checked_at: ${summary.checked_at}`,
      `- mode: ${summary.mode}`,
      `- verify_on_default: ${summary.verify_on_default}`,
      `- evidence_path: ${summary.evidence_path || "(none)"}`,
      `- acceptance: ${summary.evidence_acceptance || "(none)"}`,
      `- run_mode_audit_path: ${summary.run_mode_audit_path}`,
      `- sidecar_consistency_report_path: ${summary.sidecar_consistency_report_path}`,
      `- synthetic_sample_used: ${summary.synthetic_sample_used}`,
      `- shadow_compare_status: ${summary.latest_shadow_compare_summary.status}`,
      `- shadow_compare_mismatch_count: ${summary.latest_shadow_compare_summary.mismatch_count}`,
      `- reason_counts_mismatch_scenarios: ${summary.reason_counts_mismatch_scenarios.join(", ") || "(none)"}`,
      `- skipped: ${summary.skipped.join(", ") || "(none)"}`,
      "",
    ];
    if (summary.issues.length > 0) {
      md.push("## Issues", "");
      for (const issue of summary.issues) {
        md.push(`- ${issue}`);
      }
    }
    fs.writeFileSync(latestMdPath, `${md.join("\n")}\n`, "utf8");
    fs.writeFileSync(snapshotMdPath, `${md.join("\n")}\n`, "utf8");

    console.log(JSON.stringify(summary));
    if (!summary.ok) {
      process.exit(1);
    }
    return;
  }

  const summary = {
    status: "failed",
    ok: false,
    checked_at: new Date().toISOString(),
    mode: requestedMode,
    verify_on_default: verifyOnDefault,
    evidence_path: evidencePath,
    run_mode_audit_path: runModeAuditPath,
    sample_result_path: sampleResultPath,
    sidecar_consistency_report_path: consistencyReportPath,
    issues,
  };
  console.log(JSON.stringify(summary));
  process.exit(1);
}

main().catch((error) => {
  const summary = {
    status: "failed",
    ok: false,
    checked_at: new Date().toISOString(),
    issues: [error && error.stack ? error.stack : String(error)],
  };
  console.log(JSON.stringify(summary));
  process.exit(1);
});
'@

$nodeScript | node - $RepoRoot $OutDir $RunModeAuditPath $ConsistencyReportPath $SampleResultPath $RequestedMode $VerifyOnDefault $RequireNoSkipped $EvidencePath $RequireRealEvidence
if ($LASTEXITCODE -ne 0) {
  throw "cleaning rust v2 rollout checks failed"
}

Ok "cleaning rust v2 rollout check passed"
