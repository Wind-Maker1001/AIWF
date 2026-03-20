import {
  createUpdatedNodeMapConfig,
  unlinkSelectedGraphEdges,
} from "./app-canvas-bindings-support.js";

function bindWorkflowCanvasEvents(ctx = {}) {
  const {
    els,
    store,
    canvas,
    setStatus = () => {},
    singleSelectedNode = () => null,
    renderNodeConfigEditor = () => {},
    renderAll = () => {},
    setZoom = () => {},
    fitCanvasToView = () => {},
    applyArrange = () => {},
    selectedEdgeRef = { get: () => null, set: () => {} },
  } = ctx;

  if (els.btnAddInputMap) {
    els.btnAddInputMap.addEventListener("click", () => {
      const node = singleSelectedNode();
      if (!node) return;
      store.updateNodeConfig(node.id, createUpdatedNodeMapConfig(node, "input"));
      renderNodeConfigEditor();
    });
  }

  if (els.btnAddOutputMap) {
    els.btnAddOutputMap.addEventListener("click", () => {
      const node = singleSelectedNode();
      if (!node) return;
      store.updateNodeConfig(node.id, createUpdatedNodeMapConfig(node, "output"));
      renderNodeConfigEditor();
    });
  }

  els.snapGrid.addEventListener("change", () => renderAll());
  els.btnZoomIn.addEventListener("click", () => setZoom(canvas.getZoom() + 0.1));
  els.btnZoomOut.addEventListener("click", () => setZoom(canvas.getZoom() - 0.1));
  els.btnZoomReset.addEventListener("click", () => setZoom(1));
  if (els.btnFitCanvas) els.btnFitCanvas.addEventListener("click", () => fitCanvasToView());

  els.btnAlignLeft.addEventListener("click", () => applyArrange("left", "左对齐"));
  els.btnAlignTop.addEventListener("click", () => applyArrange("top", "上对齐"));
  els.btnDistributeH.addEventListener("click", () => applyArrange("hspace", "水平分布"));
  els.btnDistributeV.addEventListener("click", () => applyArrange("vspace", "垂直分布"));

  els.btnUnlinkSelected.addEventListener("click", () => {
    const ids = canvas.getSelectedIds();
    if (ids.length < 2) {
      setStatus("请先框选至少两个节点再取消连线", false);
      return;
    }
    const out = unlinkSelectedGraphEdges(store.state.graph.edges, ids);
    store.state.graph.edges = out.edges;
    if (out.removed > 0) {
      const selectedEdge = selectedEdgeRef.get();
      if (selectedEdge && !store.getEdge(selectedEdge.from, selectedEdge.to)) selectedEdgeRef.set(null);
      renderAll();
      setStatus(`已取消 ${out.removed} 条框选节点连线`, true);
    } else {
      setStatus("框选节点之间不存在可取消的连线", false);
    }
  });
}

export { bindWorkflowCanvasEvents };
