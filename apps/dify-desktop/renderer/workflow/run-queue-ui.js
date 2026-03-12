function createWorkflowRunQueueUi(deps = {}) {
  const {
    setStatus = () => {},
    renderRunHistoryRows = () => {},
    renderQueueRows = () => {},
    renderQueueControl = () => {},
  } = deps;

  async function refreshRunHistory() {
    try {
      const out = await window.aiwfDesktop.listWorkflowRuns({ limit: 80 });
      renderRunHistoryRows(out?.items || []);
    } catch {
      renderRunHistoryRows([]);
    }
  }

  async function refreshQueue() {
    try {
      const out = await window.aiwfDesktop.listWorkflowQueue({ limit: 120 });
      renderQueueRows(out?.items || []);
      renderQueueControl(out?.control || {});
    } catch {
      renderQueueRows([]);
      renderQueueControl({});
    }
  }

  async function pauseQueue() {
    const out = await window.aiwfDesktop.setWorkflowQueueControl({ paused: true });
    setStatus(out?.ok ? "队列已暂停" : `暂停失败: ${out?.error || "unknown"}`, !!out?.ok);
    await refreshQueue();
  }

  async function resumeQueue() {
    const out = await window.aiwfDesktop.setWorkflowQueueControl({ paused: false });
    setStatus(out?.ok ? "队列已恢复" : `恢复失败: ${out?.error || "unknown"}`, !!out?.ok);
    await refreshQueue();
  }

  return {
    refreshRunHistory,
    refreshQueue,
    pauseQueue,
    resumeQueue,
  };
}

export { createWorkflowRunQueueUi };
