import { createWorkflowPanelsPrimaryRenderers } from "./panels-ui-primary-renderers.js";
import { createWorkflowPanelsGovernanceRenderers } from "./panels-ui-governance-renderers.js";

function createWorkflowPanelsUiRenderers(els, deps = {}) {
  return {
    ...createWorkflowPanelsPrimaryRenderers(els, deps),
    ...createWorkflowPanelsGovernanceRenderers(els, deps),
  };
}

export { createWorkflowPanelsUiRenderers };
