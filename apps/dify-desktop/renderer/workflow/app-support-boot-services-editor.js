function buildWorkflowBootEditorServices(ctx = {}) {
  const {
    appServices = {},
    uiServices = {},
  } = ctx;

  return {
    singleSelectedNode: appServices.singleSelectedNode,
    renderNodeConfigEditor: appServices.renderNodeConfigEditor,
    setZoom: appServices.setZoom,
    fitCanvasToView: appServices.fitCanvasToView,
    applyArrange: appServices.applyArrange,
    parseNodeConfigText: appServices.parseNodeConfigText,
    parseNodeConfigForm: appServices.parseNodeConfigForm,
    prettyJson: appServices.prettyJson,
    renderNodeConfigForm: appServices.renderNodeConfigForm,
    setCfgMode: appServices.setCfgMode,
    parseEdgeWhenText: appServices.parseEdgeWhenText,
    applyEdgeWhenToBuilder: appServices.applyEdgeWhenToBuilder,
    renderEdgeConfigEditor: appServices.renderEdgeConfigEditor,
    setEdgeWhenBuilderVisibility: appServices.setEdgeWhenBuilderVisibility,
    syncEdgeTextFromBuilder: appServices.syncEdgeTextFromBuilder,
    handleCanvasDragOver: appServices.handleCanvasDragOver,
    handleCanvasDrop: appServices.handleCanvasDrop,
    rebuildEdgeHints: appServices.rebuildEdgeHints,
    renderNodeRuns: uiServices.renderNodeRuns,
    renderDiagRuns: uiServices.renderDiagRuns,
  };
}

export { buildWorkflowBootEditorServices };
