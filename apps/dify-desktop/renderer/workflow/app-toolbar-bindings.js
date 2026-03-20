import {
  bindWorkflowCoreToolbarActions,
  bindWorkflowGovernanceToolbarActions,
  bindWorkflowTemplateToolbarActions,
} from "./app-toolbar-bindings-support.js";

function bindWorkflowToolbarEvents(ctx = {}) {
  bindWorkflowCoreToolbarActions(ctx);
  bindWorkflowGovernanceToolbarActions(ctx);
  bindWorkflowTemplateToolbarActions(ctx);
}

export { bindWorkflowToolbarEvents };
