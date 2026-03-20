function sandboxExportFormat(value) {
  return String(value || "md").trim() || "md";
}

function sandboxAlertsPayload(thresholds, dedupWindowSec, limit = 500) {
  return {
    limit,
    thresholds: thresholds || {},
    dedup_window_sec: dedupWindowSec,
  };
}

function sandboxRulesPayload(rules) {
  return { rules: rules || {} };
}

function sandboxMutePayload(els) {
  return {
    node_type: String(els.sandboxMuteNodeType?.value || "*").trim() || "*",
    node_id: String(els.sandboxMuteNodeId?.value || "*").trim() || "*",
    code: String(els.sandboxMuteCode?.value || "*").trim() || "*",
    minutes: Number(els.sandboxMuteMinutes?.value || 60),
  };
}

function sandboxPresetExportPayload(preset) {
  return { preset: preset || {} };
}

function sandboxListPayload(limit) {
  return { limit };
}

export {
  sandboxAlertsPayload,
  sandboxExportFormat,
  sandboxListPayload,
  sandboxMutePayload,
  sandboxPresetExportPayload,
  sandboxRulesPayload,
};
