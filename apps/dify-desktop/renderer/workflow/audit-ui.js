function createWorkflowAuditUi(els, deps = {}) {
  const {
    setStatus = () => {},
    renderTimelineRows = () => {},
    renderFailureRows = () => {},
    renderAuditRows = () => {},
  } = deps;

  async function refreshTimeline() {
    const runId = String(els.timelineRunId?.value || "").trim();
    if (!runId) {
      setStatus("请先填写 Run ID", false);
      return;
    }
    const out = await window.aiwfDesktop.getWorkflowRunTimeline({ run_id: runId });
    renderTimelineRows(out);
    setStatus(out?.ok ? "时间线刷新完成" : `时间线刷新失败: ${out?.error || "unknown"}`, !!out?.ok);
  }

  async function refreshFailureSummary() {
    const out = await window.aiwfDesktop.getWorkflowFailureSummary({ limit: 500 });
    renderFailureRows(out || {});
  }

  async function refreshAudit() {
    const out = await window.aiwfDesktop.listWorkflowAuditLogs({ limit: 120 });
    renderAuditRows(out?.items || []);
  }

  return {
    refreshTimeline,
    refreshFailureSummary,
    refreshAudit,
  };
}

export { createWorkflowAuditUi };
