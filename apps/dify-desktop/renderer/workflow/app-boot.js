import { bindWorkflowToolbarEvents } from "./app-toolbar-bindings.js";
import { bindWorkflowCanvasEvents } from "./app-canvas-bindings.js";
import { bindWorkflowEditorEvents } from "./app-editor-bindings.js";
import { initializeWorkflowApp } from "./app-startup.js";
import {
  buildCanvasBindingDeps,
  buildEditorBindingDeps,
  buildStartupDeps,
  buildToolbarBindingDeps,
} from "./app-boot-support.js";

function bootWorkflowApp(ctx = {}) {
  bindWorkflowToolbarEvents(buildToolbarBindingDeps(ctx));
  bindWorkflowCanvasEvents(buildCanvasBindingDeps(ctx));
  bindWorkflowEditorEvents(buildEditorBindingDeps(ctx));
  initializeWorkflowApp(buildStartupDeps(ctx));
}

export { bootWorkflowApp };
