import { createWorkflowDiagnosticsUi } from "./diagnostics-ui.js";
import { createWorkflowDiagnosticsPanelUi } from "./diagnostics-panel-ui.js";
import { createWorkflowConnectivityUi } from "./connectivity-ui.js";
import { setupWorkflowDebugApi } from "./debug-api-ui.js";
import { createWorkflowStatusUi } from "./status-ui.js";
import {
  buildConnectivityUiDeps,
  buildDebugApiDeps,
  buildDiagnosticsPanelDeps,
} from "./app-ui-services-support.js";

function createWorkflowUiServices(ctx = {}) {
  const {
    els,
    store,
    canvas,
    renderAll = () => {},
  } = ctx;

  const { setStatus } = createWorkflowStatusUi(els);
  const { renderNodeRuns, renderDiagRuns, fetchRustRuntimeStats } = createWorkflowDiagnosticsUi(els);
  const { refreshDiagnostics } = createWorkflowDiagnosticsPanelUi(
    buildDiagnosticsPanelDeps({ renderDiagRuns, fetchRustRuntimeStats })
  );

  setupWorkflowDebugApi(window, buildDebugApiDeps({
    store,
    canvas,
    renderAll,
  }));

  const { refreshOfflineBoundaryHint, applyDeepSeekDefaults } = createWorkflowConnectivityUi(
    els,
    buildConnectivityUiDeps({ setStatus, store })
  );

  return {
    applyDeepSeekDefaults,
    refreshDiagnostics,
    refreshOfflineBoundaryHint,
    renderDiagRuns,
    renderNodeRuns,
    setStatus,
  };
}

export { createWorkflowUiServices };
