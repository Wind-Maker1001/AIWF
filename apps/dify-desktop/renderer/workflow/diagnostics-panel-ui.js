function createWorkflowDiagnosticsPanelUi(deps = {}) {
  const {
    renderDiagRuns = () => {},
    fetchRustRuntimeStats = async () => null,
  } = deps;

  async function refreshDiagnostics() {
    try {
      const [out, rust, perf] = await Promise.all([
        window.aiwfDesktop.getWorkflowDiagnostics({ limit: 80 }),
        fetchRustRuntimeStats(),
        window.aiwfDesktop.getWorkflowPerfDashboard({ limit: 200 }),
      ]);
      renderDiagRuns(out || {}, rust || {}, perf || {});
    } catch {}
  }

  return { refreshDiagnostics };
}

export { createWorkflowDiagnosticsPanelUi };
