import {
  auditLogRequestPayload,
  failureSummaryRequestPayload,
  normalizeTimelineRunId,
  timelineStatusMessage,
} from "./audit-ui-support.js";

function createWorkflowAuditUi(els, deps = {}) {
  const {
    setStatus = () => {},
    renderTimelineRows = () => {},
    renderFailureRows = () => {},
    renderAuditRows = () => {},
  } = deps;

  async function refreshTimeline() {
    const runId = normalizeTimelineRunId(els.timelineRunId?.value || "");
    if (!runId) {
      setStatus("请先填写 Run ID", false);
      return;
    }
    const out = await window.aiwfDesktop.getWorkflowRunTimeline({ run_id: runId });
    renderTimelineRows(out);
    setStatus(timelineStatusMessage(out), !!out?.ok);
  }

  async function refreshFailureSummary() {
    const out = await window.aiwfDesktop.getWorkflowFailureSummary(failureSummaryRequestPayload());
    renderFailureRows(out || {});
  }

  async function refreshAudit() {
    const out = await window.aiwfDesktop.listWorkflowAuditLogs(auditLogRequestPayload());
    renderAuditRows(out?.items || []);
  }

  return {
    refreshTimeline,
    refreshFailureSummary,
    refreshAudit,
  };
}

export { createWorkflowAuditUi };
