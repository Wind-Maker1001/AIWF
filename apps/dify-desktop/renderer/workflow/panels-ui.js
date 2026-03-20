import { createWorkflowPanelsUiRenderers } from "./panels-ui-renderers.js";

function createWorkflowPanelsUi(els, deps = {}) {
  return createWorkflowPanelsUiRenderers(els, deps);
}

export { createWorkflowPanelsUi };
