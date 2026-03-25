import { WORKFLOW_SCHEMA_VERSION } from "./workflow-contract.js";
import { assertRegisteredWorkflowNodeTypes } from "./node-catalog-contract.js";

function parseChipletIsolatedTypes(text) {
  return String(text || "")
    .split(/[;,]/)
    .map((x) => String(x || "").trim())
    .filter(Boolean);
}

function positiveIntegerOrDefault(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

function buildBaseRunPayload(els, graph, sandboxDedupWindowSec = 600) {
  const normalizedGraph = graph && typeof graph === "object" ? graph : {};
  assertRegisteredWorkflowNodeTypes(normalizedGraph, { stage: "run_payload" });
  const workflowVersion = String(normalizedGraph.version || "").trim() || WORKFLOW_SCHEMA_VERSION;
  return {
    workflow_id: normalizedGraph.workflow_id || "custom_v1",
    workflow_version: workflowVersion,
    workflow: {
      ...normalizedGraph,
      workflow_id: normalizedGraph.workflow_id || "custom_v1",
      version: workflowVersion,
    },
    quality_rule_set_id: String(els.qualityRuleSetId?.value || els.qualityRuleSetSelect?.value || "").trim(),
    params: {
      report_title: String(els.reportTitle?.value || "").trim(),
      input_files: String(els.inputFiles?.value || "").trim(),
      md_only: true,
      paper_markdown_enabled: true,
      export_canonical_bundle: !!els.exportCanonicalBundle?.checked,
      canonical_title: String(els.canonicalTitle?.value || "").trim() || "AIWF 鐔熻倝璇枡",
      ocr_lang: "chi_sim+eng",
    },
    breakpoint_node_id: String(els.breakpointNodeId?.value || "").trim(),
    ai: {
      endpoint: String(els.aiEndpoint?.value || "").trim(),
      api_key: String(els.aiKey?.value || "").trim(),
      model: String(els.aiModel?.value || "").trim(),
      temperature: 0.2,
    },
    rust: {
      endpoint: String(els.rustEndpoint?.value || "").trim(),
      required: !!els.rustRequired?.checked,
    },
    chiplet_isolation_enabled: els.chipletIsolationEnabled ? !!els.chipletIsolationEnabled.checked : true,
    chiplet_isolation_mode: String(els.chipletIsolationMode?.value || "high_risk").trim() || "high_risk",
    chiplet_isolated_types: parseChipletIsolatedTypes(els.chipletIsolatedTypes?.value || ""),
    sandbox_alert_dedup_window_sec: sandboxDedupWindowSec,
    sandbox_autofix_enabled: els.sandboxAutoFixEnabled ? !!els.sandboxAutoFixEnabled.checked : true,
    sandbox_autofix_pause_queue: els.sandboxAutoFixPauseQueue ? !!els.sandboxAutoFixPauseQueue.checked : true,
    sandbox_autofix_require_review: els.sandboxAutoFixRequireReview ? !!els.sandboxAutoFixRequireReview.checked : true,
    sandbox_autofix_force_isolation: els.sandboxAutoFixForceIsolation ? !!els.sandboxAutoFixForceIsolation.checked : true,
    sandbox_autofix_red_threshold: Number(els.sandboxAutoFixRedThreshold?.value || 3),
    sandbox_autofix_window_sec: Number(els.sandboxAutoFixWindowSec?.value || 900),
    sandbox_autofix_force_minutes: Number(els.sandboxAutoFixForceMinutes?.value || 60),
    sandbox_autofix_force_mode: String(els.sandboxAutoFixForceMode?.value || "process").trim() || "process",
    sandbox_limits: {
      max_duration_ms: positiveIntegerOrDefault(els.sandboxMaxDurationMs?.value, 180000),
      max_cpu_ms: positiveIntegerOrDefault(els.sandboxMaxCpuMs?.value, 120000),
      max_rss_mb: positiveIntegerOrDefault(els.sandboxMaxRssMb?.value, 512),
      max_output_bytes: positiveIntegerOrDefault(els.sandboxMaxOutputBytes?.value, 2000000),
    },
  };
}

function mergeRunPayload(base, extra = {}) {
  const ext = extra && typeof extra === "object" ? extra : {};
  const out = { ...base, ...ext };
  out.params = { ...(base.params || {}), ...(ext.params || {}) };
  return out;
}

export {
  buildBaseRunPayload,
  mergeRunPayload,
  parseChipletIsolatedTypes,
  positiveIntegerOrDefault,
};
