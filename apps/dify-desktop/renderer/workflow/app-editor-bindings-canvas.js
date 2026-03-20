function bindCanvasEditorInteractions(ctx = {}) {
  const {
    els,
    canvas,
    setZoom = () => {},
    syncCanvasPanels = () => {},
    handleCanvasDragOver = () => {},
    handleCanvasDrop = () => {},
    windowTarget = window,
  } = ctx;

  els.canvasWrap.addEventListener(
    "wheel",
    (evt) => {
      if (!evt.ctrlKey) return;
      evt.preventDefault();
      setZoom(canvas.getZoom() + (evt.deltaY < 0 ? 0.08 : -0.08), {
        clientX: evt.clientX,
        clientY: evt.clientY,
      });
    },
    { passive: false }
  );

  windowTarget.addEventListener("resize", () => {
    syncCanvasPanels();
  }, { passive: true });

  els.canvasWrap.addEventListener("dragover", handleCanvasDragOver);
  els.canvasWrap.addEventListener("drop", handleCanvasDrop);
}

export { bindCanvasEditorInteractions };
