import { installWorkflowCanvasStateViewMethods } from './canvas_class_methods_state_view.mjs';
import { installWorkflowCanvasRenderGeometryMethods } from './canvas_class_methods_render_geometry.mjs';
import { installWorkflowCanvasInteractionMethods } from './canvas_class_methods_interactions.mjs';

function installWorkflowCanvasMethods(WorkflowCanvas) {
  installWorkflowCanvasStateViewMethods(WorkflowCanvas);
  installWorkflowCanvasRenderGeometryMethods(WorkflowCanvas);
  installWorkflowCanvasInteractionMethods(WorkflowCanvas);
}

export { installWorkflowCanvasMethods };
