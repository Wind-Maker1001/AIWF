import { bindWorkflowToolbarEvents } from "./app-toolbar-bindings.js";
import { bindWorkflowCanvasEvents } from "./app-canvas-bindings.js";
import { bindWorkflowEditorEvents } from "./app-editor-bindings.js";
import { initializeWorkflowApp } from "./app-startup.js";

function bootWorkflowApp(ctx = {}) {
  bindWorkflowToolbarEvents(ctx);
  bindWorkflowCanvasEvents(ctx);
  bindWorkflowEditorEvents(ctx);
  initializeWorkflowApp(ctx);
}

export { bootWorkflowApp };
