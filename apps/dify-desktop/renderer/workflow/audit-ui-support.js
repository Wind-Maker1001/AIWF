function normalizeTimelineRunId(value) {
  return String(value || "").trim();
}

function timelineStatusMessage(out) {
  return out?.ok ? "时间线刷新完成" : `时间线刷新失败: ${out?.error || "unknown"}`;
}

function failureSummaryRequestPayload(limit = 500) {
  return { limit };
}

function auditLogRequestPayload(limit = 120) {
  return { limit };
}

export {
  auditLogRequestPayload,
  failureSummaryRequestPayload,
  normalizeTimelineRunId,
  timelineStatusMessage,
};
