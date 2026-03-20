import { buildWorkflowBootCoreServices } from "./app-support-boot-services-core.js";
import { buildWorkflowBootGovernanceServices } from "./app-support-boot-services-governance.js";
import { buildWorkflowBootTemplateServices } from "./app-support-boot-services-template.js";
import { buildWorkflowBootEditorServices } from "./app-support-boot-services-editor.js";
import { buildWorkflowBootPanelServices } from "./app-support-boot-services-panels.js";

function buildWorkflowBootServices(ctx = {}) {
  return {
    ...buildWorkflowBootCoreServices(ctx),
    ...buildWorkflowBootGovernanceServices(ctx),
    ...buildWorkflowBootTemplateServices(ctx),
    ...buildWorkflowBootEditorServices(ctx),
    ...buildWorkflowBootPanelServices(ctx),
  };
}

export {
  buildWorkflowBootCoreServices,
  buildWorkflowBootEditorServices,
  buildWorkflowBootGovernanceServices,
  buildWorkflowBootPanelServices,
  buildWorkflowBootServices,
  buildWorkflowBootTemplateServices,
};
