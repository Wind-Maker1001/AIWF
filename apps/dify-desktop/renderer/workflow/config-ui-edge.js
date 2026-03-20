import { createWorkflowConfigEdgeWhenSupport } from "./config-ui-edge-when.js";
import { createWorkflowConfigEdgeEditorSupport } from "./config-ui-edge-editor.js";

function createWorkflowConfigEdgeSupport(els, deps = {}) {
  const edgeWhenSupport = createWorkflowConfigEdgeWhenSupport(els);
  const edgeEditorSupport = createWorkflowConfigEdgeEditorSupport(els, {
    ...deps,
    applyEdgeWhenToBuilder: edgeWhenSupport.applyEdgeWhenToBuilder,
  });

  return {
    ...edgeWhenSupport,
    ...edgeEditorSupport,
  };
}

export { createWorkflowConfigEdgeSupport };
