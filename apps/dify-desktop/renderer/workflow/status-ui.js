import { applyWorkflowStatus } from "./status-ui-support.js";

function createWorkflowStatusUi(els) {
  function setStatus(text, ok = true) {
    applyWorkflowStatus(els, text, ok);
  }

  return { setStatus };
}

export { createWorkflowStatusUi };
