import { normalizeReviewQueueItems } from "./review-queue-support.js";

function createWorkflowReviewQueueUi(deps = {}) {
  const {
    renderReviewRows = () => {},
  } = deps;

  async function refreshReviewQueue() {
    try {
      const out = await window.aiwfDesktop.listManualReviews();
      renderReviewRows(normalizeReviewQueueItems(out));
    } catch {
      renderReviewRows([]);
    }
  }

  return { refreshReviewQueue };
}

export { createWorkflowReviewQueueUi };
