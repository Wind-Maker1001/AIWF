function createWorkflowRunPayloadUi(els, deps = {}) {
  const {
    store,
    sandboxDedupWindowSec = () => 600,
  } = deps;

  function graphPayload() {
    store.setWorkflowName(els.workflowName.value);
    return store.exportGraph();
  }

  function runPayload(extra = {}) {
    const graph = graphPayload();
    const isolatedTypes = String(els.chipletIsolatedTypes?.value || "")
      .split(/[;,]/)
      .map((x) => String(x || "").trim())
      .filter(Boolean);
    const toNum = (v, d) => {
      const n = Number(v);
      return Number.isFinite(n) && n > 0 ? Math.floor(n) : d;
    };
    const base = {
      workflow_id: graph.workflow_id || "custom_v1",
      workflow: graph,
      quality_rule_set_id: String(els.qualityRuleSetId?.value || els.qualityRuleSetSelect?.value || "").trim(),
      params: {
        report_title: String(els.reportTitle?.value || "").trim(),
        input_files: String(els.inputFiles?.value || "").trim(),
        md_only: true,
        paper_markdown_enabled: true,
        export_canonical_bundle: !!els.exportCanonicalBundle?.checked,
        canonical_title: String(els.canonicalTitle?.value || "").trim() || "AIWF 熟肉语料",
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
      chiplet_isolated_types: isolatedTypes,
      sandbox_alert_dedup_window_sec: sandboxDedupWindowSec(),
      sandbox_autofix_enabled: els.sandboxAutoFixEnabled ? !!els.sandboxAutoFixEnabled.checked : true,
      sandbox_autofix_pause_queue: els.sandboxAutoFixPauseQueue ? !!els.sandboxAutoFixPauseQueue.checked : true,
      sandbox_autofix_require_review: els.sandboxAutoFixRequireReview ? !!els.sandboxAutoFixRequireReview.checked : true,
      sandbox_autofix_force_isolation: els.sandboxAutoFixForceIsolation ? !!els.sandboxAutoFixForceIsolation.checked : true,
      sandbox_autofix_red_threshold: Number(els.sandboxAutoFixRedThreshold?.value || 3),
      sandbox_autofix_window_sec: Number(els.sandboxAutoFixWindowSec?.value || 900),
      sandbox_autofix_force_minutes: Number(els.sandboxAutoFixForceMinutes?.value || 60),
      sandbox_autofix_force_mode: String(els.sandboxAutoFixForceMode?.value || "process").trim() || "process",
      sandbox_limits: {
        max_duration_ms: toNum(els.sandboxMaxDurationMs?.value, 180000),
        max_cpu_ms: toNum(els.sandboxMaxCpuMs?.value, 120000),
        max_rss_mb: toNum(els.sandboxMaxRssMb?.value, 512),
        max_output_bytes: toNum(els.sandboxMaxOutputBytes?.value, 2000000),
      },
    };
    const ext = extra && typeof extra === "object" ? extra : {};
    const out = { ...base, ...ext };
    out.params = { ...(base.params || {}), ...(ext.params || {}) };
    return out;
  }

  return {
    graphPayload,
    runPayload,
  };
}

export { createWorkflowRunPayloadUi };
