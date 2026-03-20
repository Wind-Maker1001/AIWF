function buildDiagnosticsPanelDeps(ctx = {}) {
  const {
    renderDiagRuns = () => {},
    fetchRustRuntimeStats = async () => ({}),
  } = ctx;

  return {
    renderDiagRuns,
    fetchRustRuntimeStats,
  };
}

function buildDebugApiDeps(ctx = {}) {
  const {
    store,
    canvas,
    renderAll = () => {},
  } = ctx;

  return {
    store,
    canvas,
    renderAll,
  };
}

function buildConnectivityUiDeps(ctx = {}) {
  const {
    setStatus = () => {},
    store,
  } = ctx;

  return {
    setStatus,
    exportGraph: () => store.exportGraph(),
  };
}

export {
  buildConnectivityUiDeps,
  buildDebugApiDeps,
  buildDiagnosticsPanelDeps,
};
