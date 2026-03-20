import {
  normalizeQueueControl,
  normalizeQueueItems,
  queueControlStatusText,
  queueRequestPayload,
  runHistoryRequestPayload,
} from "./run-queue-support.js";

function createWorkflowRunQueueUi(deps = {}) {
  const {
    setStatus = () => {},
    renderRunHistoryRows = () => {},
    renderQueueRows = () => {},
    renderQueueControl = () => {},
  } = deps;

  async function refreshRunHistory() {
    try {
      const out = await window.aiwfDesktop.listWorkflowRuns(runHistoryRequestPayload());
      renderRunHistoryRows(normalizeQueueItems(out));
    } catch {
      renderRunHistoryRows([]);
    }
  }

  async function refreshQueue() {
    try {
      const out = await window.aiwfDesktop.listWorkflowQueue(queueRequestPayload());
      renderQueueRows(normalizeQueueItems(out));
      renderQueueControl(normalizeQueueControl(out));
    } catch {
      renderQueueRows([]);
      renderQueueControl({});
    }
  }

  async function pauseQueue() {
    const out = await window.aiwfDesktop.setWorkflowQueueControl({ paused: true });
    setStatus(queueControlStatusText(true, out?.error), !!out?.ok);
    await refreshQueue();
  }

  async function resumeQueue() {
    const out = await window.aiwfDesktop.setWorkflowQueueControl({ paused: false });
    setStatus(queueControlStatusText(false, out?.error), !!out?.ok);
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
