import { statusColor } from "./panels-ui-run-shared.js";
import { createWorkflowPanelsRunHistoryRenderers } from "./panels-ui-run-history-renderers.js";
import { createWorkflowPanelsQueueRenderers } from "./panels-ui-queue-renderers.js";

function createWorkflowPanelsRunRenderers(els, deps = {}) {
  return {
    statusColor,
    ...createWorkflowPanelsRunHistoryRenderers(els, deps),
    ...createWorkflowPanelsQueueRenderers(els, deps),
  };
}

export { createWorkflowPanelsRunRenderers };
