function setupWorkflowDebugApi(win, deps = {}) {
  const {
    store,
    canvas,
    renderAll = () => {},
  } = deps;

  const debugApiEnabled = (() => {
    try {
      const q = new URLSearchParams(win.location?.search || "");
      return q.get("debug") === "1";
    } catch {
      return false;
    }
  })();

  if (debugApiEnabled) {
    win.__aiwfDebug = Object.freeze({
      tryLink: (from, to) => {
        const a = String(from || "");
        const b = String(to || "");
        if (!a || !b) return { ok: false, reason: "empty" };
        if (store.hasEdge(a, b)) {
          store.unlink(a, b);
          renderAll();
          return { ok: true, toggled: true };
        }
        const out = store.linkToFrom(a, b);
        if (out?.ok) renderAll();
        return out;
      },
      graph: () => store.exportGraph(),
      routeStats: () => canvas.getRouteMetrics(),
      selectNodes: (ids) => {
        canvas.setSelectedIds(Array.isArray(ids) ? ids : []);
        renderAll();
        return canvas.getSelectedIds();
      },
      setGraph: (graph) => {
        store.importGraph(graph || {});
        renderAll();
        return store.exportGraph();
      },
    });
    return true;
  }

  try { delete win.__aiwfDebug; } catch {}
  return false;
}

export { setupWorkflowDebugApi };
