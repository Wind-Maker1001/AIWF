const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadReviewQueueUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/review-queue-ui.js")).href;
  return import(file);
}

test("workflow review queue ui refreshes review rows and falls back on errors", async () => {
  const { createWorkflowReviewQueueUi } = await loadReviewQueueUiModule();
  const renderCalls = [];
  global.window = {
    aiwfDesktop: {
      listManualReviews: async () => ({
        items: [{ run_id: "run_1", review_key: "gate_a", status: "pending" }],
      }),
    },
  };

  try {
    const ui = createWorkflowReviewQueueUi({
      renderReviewRows: (items) => renderCalls.push(items),
    });
    await ui.refreshReviewQueue();
    global.window.aiwfDesktop.listManualReviews = async () => {
      throw new Error("review queue unavailable");
    };
    await ui.refreshReviewQueue();
  } finally {
    delete global.window;
  }

  assert.deepEqual(renderCalls, [
    [{ run_id: "run_1", review_key: "gate_a", status: "pending" }],
    [],
  ]);
});
