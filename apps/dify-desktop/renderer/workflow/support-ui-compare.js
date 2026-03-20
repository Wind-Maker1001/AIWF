import { createWorkflowSupportReviewHistory } from "./support-ui-review-history.js";
import { createWorkflowSupportRunCompare } from "./support-ui-run-compare.js";

function createWorkflowSupportCompare(els, deps = {}) {
  return {
    ...createWorkflowSupportReviewHistory(els, deps),
    ...createWorkflowSupportRunCompare(els, deps),
  };
}

export { createWorkflowSupportCompare };
