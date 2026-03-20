import { createWorkflowSupportSandbox } from "./support-ui-sandbox.js";
import { createWorkflowSupportCompare } from "./support-ui-compare.js";

function createWorkflowSupportUi(els, deps = {}) {
  return {
    ...createWorkflowSupportSandbox(els, deps),
    ...createWorkflowSupportCompare(els, deps),
  };
}

export { createWorkflowSupportUi };
