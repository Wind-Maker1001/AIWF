import { initializeCanvasInstance } from './canvas_init.mjs';
import { installWorkflowCanvasMethods } from './canvas_class_methods.mjs';

export class WorkflowCanvas {
  constructor({
    store,
    nodeCatalog,
    canvasWrap,
    canvasSurface,
    nodesLayer,
    guideLayer,
    minimapCanvas,
    edgesSvg,
    onChange,
    onWarn,
    onSelectionChange,
    onEdgeSelect,
  }) {
    initializeCanvasInstance(this, {
      store,
      nodeCatalog,
      canvasWrap,
      canvasSurface,
      nodesLayer,
      guideLayer,
      minimapCanvas,
      edgesSvg,
      onChange,
      onWarn,
      onSelectionChange,
      onEdgeSelect,
    });

    this.bindCanvasEvents();
    this.bindMinimapEvents();
  }
}

installWorkflowCanvasMethods(WorkflowCanvas);
