import { installWorkflowCanvasStateMethods } from './canvas_class_methods_state.mjs';
import { installWorkflowCanvasViewRuntimeMethods } from './canvas_class_methods_view_runtime.mjs';

function installWorkflowCanvasStateViewMethods(WorkflowCanvas) {
  installWorkflowCanvasStateMethods(WorkflowCanvas);
  installWorkflowCanvasViewRuntimeMethods(WorkflowCanvas);
}

export { installWorkflowCanvasStateViewMethods };
