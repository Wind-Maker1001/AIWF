function createWorkflowCanvasViewUi(els, deps = {}) {
  const {
    canvas,
    setStatus = () => {},
    renderNodeConfigEditor = () => {},
    renderEdgeConfigEditor = () => {},
    refreshOfflineBoundaryHint = () => {},
    getNode = () => null,
    selectNodeIds = () => {},
    renderAll = () => {},
  } = deps;

  function canvasViewportCenter() {
    const rect = els.canvasWrap.getBoundingClientRect();
    return {
      clientX: rect.left + els.canvasWrap.clientWidth / 2,
      clientY: rect.top + els.canvasWrap.clientHeight / 2,
    };
  }

  function syncCanvasPanels() {
    renderNodeConfigEditor();
    renderEdgeConfigEditor();
    refreshOfflineBoundaryHint();
    const pct = Math.round(canvas.getZoom() * 100);
    if (els.zoomText) els.zoomText.textContent = `${pct}%`;
  }

  function setZoom(z, focusClient = null) {
    canvas.setSnap(!!els.snapGrid?.checked);
    canvas.setArrangePolicy({ preventOverlapOnAlign: false });
    canvas.setZoom(z, focusClient || canvasViewportCenter());
    syncCanvasPanels();
  }

  function fitCanvasToView() {
    canvas.setSnap(!!els.snapGrid?.checked);
    canvas.setArrangePolicy({ preventOverlapOnAlign: false });
    const ok = canvas.fitToView(104);
    syncCanvasPanels();
    if (ok) setStatus("已适配当前流程视图", true);
    else setStatus("当前没有可适配的节点", false);
  }

  function applyArrange(mode, label) {
    const out = canvas.alignSelected(mode);
    if (!out || !out.ok) return;
    if (Number(out.moved || 0) <= 0) {
      setStatus(`${label}: 节点已处于目标布局`, true);
      return;
    }
    setStatus(`${label}: 已调整 ${out.moved}/${out.total} 个节点`, true);
  }

  function focusNodeInCanvas(nodeId) {
    const id = String(nodeId || "").trim();
    if (!id) return;
    const node = getNode(id);
    if (!node) return;
    selectNodeIds([id]);
    renderAll();
    const cx = Math.max(0, Number(node.x || 0) - Math.floor((els.canvasWrap?.clientWidth || 0) * 0.35));
    const cy = Math.max(0, Number(node.y || 0) - Math.floor((els.canvasWrap?.clientHeight || 0) * 0.35));
    els.canvasWrap?.scrollTo?.({ left: cx, top: cy, behavior: "smooth" });
  }

  return {
    syncCanvasPanels,
    setZoom,
    fitCanvasToView,
    applyArrange,
    focusNodeInCanvas,
  };
}

export { createWorkflowCanvasViewUi };
