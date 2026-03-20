import { createWorkflowPanelsAdminVersionRenderers } from "./panels-ui-admin-version-renderers.js";
import { createWorkflowPanelsAdminAppRenderers } from "./panels-ui-admin-app-renderers.js";

function createWorkflowPanelsAdminRenderers(els, deps = {}) {
  return {
    ...createWorkflowPanelsAdminVersionRenderers(els, deps),
    ...createWorkflowPanelsAdminAppRenderers(els, deps),
  };
}

export { createWorkflowPanelsAdminRenderers };
