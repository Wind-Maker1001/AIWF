import { createWorkflowPanelsGovernanceMetricsRenderers } from "./panels-ui-governance-metrics-renderers.js";
import { createWorkflowPanelsGovernanceSandboxRenderers } from "./panels-ui-governance-sandbox-renderers.js";
import { createWorkflowPanelsGovernanceReviewRenderers } from "./panels-ui-governance-review-renderers.js";

function createWorkflowPanelsGovernanceRenderers(els, deps = {}) {
  return {
    ...createWorkflowPanelsGovernanceMetricsRenderers(els, deps),
    ...createWorkflowPanelsGovernanceSandboxRenderers(els, deps),
    ...createWorkflowPanelsGovernanceReviewRenderers(els, deps),
  };
}

export { createWorkflowPanelsGovernanceRenderers };
