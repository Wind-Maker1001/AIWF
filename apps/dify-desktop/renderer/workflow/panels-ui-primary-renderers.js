import { createWorkflowPanelsRunRenderers } from "./panels-ui-run-renderers.js";
import { createWorkflowPanelsAdminRenderers } from "./panels-ui-admin-renderers.js";

function createWorkflowPanelsPrimaryRenderers(els, deps = {}) {
  return {
    ...createWorkflowPanelsRunRenderers(els, deps),
    ...createWorkflowPanelsAdminRenderers(els, deps),
  };
}

export { createWorkflowPanelsPrimaryRenderers };
