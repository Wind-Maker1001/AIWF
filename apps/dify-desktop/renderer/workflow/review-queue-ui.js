function createWorkflowReviewQueueUi(deps = {}) {
  const {
    renderReviewRows = () => {},
  } = deps;

  async function refreshReviewQueue() {
    try {
      const out = await window.aiwfDesktop.listManualReviews();
      renderReviewRows(out?.items || []);
    } catch {
      renderReviewRows([]);
    }
  }

  return { refreshReviewQueue };
}

export { createWorkflowReviewQueueUi };
