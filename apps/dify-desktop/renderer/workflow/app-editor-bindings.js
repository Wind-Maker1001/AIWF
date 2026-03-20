import {
  bindCanvasEditorInteractions,
  bindEdgeConfigEditorActions,
  bindNodeConfigEditorActions,
} from "./app-editor-bindings-support.js";

function bindWorkflowEditorEvents(ctx = {}) {
  bindNodeConfigEditorActions(ctx);
  bindEdgeConfigEditorActions(ctx);
  bindCanvasEditorInteractions(ctx);
}

export { bindWorkflowEditorEvents };
