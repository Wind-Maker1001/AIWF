function normalizeShadowCompare(execution) {
  const shadowCompare = execution && typeof execution.shadow_compare === "object" ? execution.shadow_compare : {};
  return {
    status: String(shadowCompare.status || ""),
    matched: !!shadowCompare.matched,
    mismatch_count: Number(shadowCompare.mismatch_count || 0),
    mismatches: Array.isArray(shadowCompare.mismatches) ? shadowCompare.mismatches : [],
    skipped_reason: String(shadowCompare.skipped_reason || ""),
    compare_fields: Array.isArray(shadowCompare.compare_fields) ? shadowCompare.compare_fields : [],
  };
}

function executionAuditFields(result) {
  const execution = result && typeof result.execution === "object"
    ? result.execution
    : (result && result.profile && typeof result.profile.execution === "object" ? result.profile.execution : {});
  const quality = result && typeof result.quality === "object"
    ? result.quality
    : (result && result.profile && typeof result.profile.quality === "object" ? result.profile.quality : {});
  const qualitySummary = result && typeof result.quality_summary === "object"
    ? result.quality_summary
    : (result && result.profile && typeof result.profile.quality_summary === "object" ? result.profile.quality_summary : {});
  const shadowCompare = normalizeShadowCompare(execution);
  return {
    execution_mode: String(execution.execution_mode || ""),
    execution_eligibility_reason: String(execution.eligibility_reason || ""),
    requested_rust_v2_mode: String(execution.requested_rust_v2_mode || ""),
    effective_rust_v2_mode: String(execution.effective_rust_v2_mode || ""),
    verify_on_default: !!execution.verify_on_default,
    shadow_compare_status: String(shadowCompare.status || ""),
    shadow_compare_mismatch_count: Number(shadowCompare.mismatch_count || 0),
    rust_v2_used: !!quality.rust_v2_used,
    template: String(result && result.template ? result.template : ""),
    template_expected_profile: String(result && result.template_expected_profile ? result.template_expected_profile : ""),
    requested_profile: String(qualitySummary.requested_profile || result?.requested_profile || ""),
    recommended_profile: String(qualitySummary.recommended_profile || result?.recommended_profile || ""),
    profile_mismatch: !!qualitySummary.profile_mismatch,
    blocking_reason_codes: Array.isArray(qualitySummary.blocking_reason_codes)
      ? qualitySummary.blocking_reason_codes
      : (Array.isArray(result?.blocking_reason_codes) ? result.blocking_reason_codes : []),
    blank_output_expected: qualitySummary.blank_output_expected === undefined ? undefined : !!qualitySummary.blank_output_expected,
    zero_output_unexpected: qualitySummary.zero_output_unexpected === undefined ? undefined : !!qualitySummary.zero_output_unexpected,
  };
}

function appendRunModeAuditEntry({
  fs,
  path,
  filePath,
  mode,
  result,
  startedAt,
  extra = {},
}) {
  if (!fs || !path || !filePath) {
    throw new Error("appendRunModeAuditEntry requires fs/path/filePath");
  }
  const payload = {
    ts: new Date().toISOString(),
    mode: String(mode || ""),
    ok: !!(result && result.ok),
    job_id: result && result.job_id ? String(result.job_id) : "",
    duration_ms: Math.max(0, Date.now() - Number(startedAt || Date.now())),
    ...extra,
    ...executionAuditFields(result),
  };
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.appendFileSync(filePath, `${JSON.stringify(payload)}\n`, "utf8");
  return payload;
}

function buildCleaningShadowRolloutEvidence({
  acceptance,
  result,
  runModeAuditPath,
  reportPath,
  sampleResultPath,
}) {
  const execution = result && typeof result.execution === "object"
    ? result.execution
    : (result && result.profile && typeof result.profile.execution === "object" ? result.profile.execution : {});
  const quality = result && typeof result.quality === "object"
    ? result.quality
    : (result && result.profile && typeof result.profile.quality === "object" ? result.profile.quality : {});
  const shadowCompare = normalizeShadowCompare(execution);
  return {
    schema: "cleaning_shadow_rollout.acceptance.v1",
    acceptance: String(acceptance || ""),
    generated_at: new Date().toISOString(),
    ok: !!(result && result.ok),
    requested_rust_v2_mode: String(execution.requested_rust_v2_mode || ""),
    effective_rust_v2_mode: String(execution.effective_rust_v2_mode || ""),
    verify_on_default: !!execution.verify_on_default,
    execution: {
      execution_mode: String(execution.execution_mode || ""),
      execution_eligibility_reason: String(execution.eligibility_reason || ""),
      execution_audit_schema: String((execution.execution_audit && execution.execution_audit.schema) || ""),
    },
    quality: {
      rust_v2_used: !!quality.rust_v2_used,
      input_rows: Number(quality.input_rows || 0),
      output_rows: Number(quality.output_rows || 0),
      invalid_rows: Number(quality.invalid_rows || 0),
      filtered_rows: Number(quality.filtered_rows || 0),
      duplicate_rows_removed: Number(quality.duplicate_rows_removed || 0),
    },
    quality_summary: {
      requested_profile: String(result?.quality_summary?.requested_profile || ""),
      recommended_profile: String(result?.quality_summary?.recommended_profile || ""),
      blocking_reason_codes: Array.isArray(result?.quality_summary?.blocking_reason_codes) ? result.quality_summary.blocking_reason_codes : [],
      blank_output_expected: !!result?.quality_summary?.blank_output_expected,
      zero_output_unexpected: !!result?.quality_summary?.zero_output_unexpected,
    },
    shadow_compare: shadowCompare,
    run_mode_audit_path: String(runModeAuditPath || ""),
    sample_result_path: String(sampleResultPath || ""),
    acceptance_report_path: String(reportPath || ""),
    job_id: result && result.job_id ? String(result.job_id) : "",
  };
}

module.exports = {
  appendRunModeAuditEntry,
  buildCleaningShadowRolloutEvidence,
  executionAuditFields,
  normalizeShadowCompare,
};
