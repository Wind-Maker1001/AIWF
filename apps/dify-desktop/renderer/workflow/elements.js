import { getWorkflowCoreElements } from "./elements-core.js";
import { getWorkflowCanvasElements } from "./elements-canvas.js";
import { getWorkflowEditorElements } from "./elements-editor.js";
import { getWorkflowCompareElements } from "./elements-compare.js";

function getWorkflowElements($) {
  return {
    ...getWorkflowCoreElements($),
    ...getWorkflowCanvasElements($),
    ...getWorkflowEditorElements($),
    ...getWorkflowCompareElements($),
  };
}

export { getWorkflowElements };
