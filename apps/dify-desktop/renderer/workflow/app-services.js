import { createWorkflowCoreServices } from "./app-core-services.js";
import { createWorkflowPanelsUi } from "./panels-ui.js";
import { createWorkflowPanelServices } from "./app-panel-services.js";
import { createWorkflowLateServices } from "./app-late-services.js";
import {
  buildLateServicesDeps,
  buildPanelsUiDeps,
  buildPanelServicesDeps,
} from "./app-services-support.js";
import {
  buildCoreServicesContext,
  combineWorkflowAppServices,
} from "./app-services-assembly-support.js";

function createWorkflowAppServices(ctx = {}) {
  const {
    els,
    store,
    canvas,
    setStatus = () => {},
    renderAll = () => {},
    refreshOfflineBoundaryHint = () => {},
    graphShellApi,
    state = {},
    staticConfig = {},
    defaultNodeConfig,
    migrateLoadedWorkflowGraph,
    renderNodeRuns = () => {},
    refreshDiagnostics = async () => {},
  } = ctx;

  let panelServices = null;
  let renderMigrationReport = () => {};
  const coreServicesRef = { current: null };

  const coreServices = createWorkflowCoreServices(buildCoreServicesContext({
    els,
    store,
    setStatus,
    state,
    renderMigrationReport: (...args) => renderMigrationReport(...args),
    staticConfig,
    renderAll,
    refreshOfflineBoundaryHint,
    coreServicesRef,
    canvas,
  }));
  coreServicesRef.current = coreServices;

  const panelsUi = createWorkflowPanelsUi(els, buildPanelsUiDeps({
    els,
    setStatus,
    getPanelServices: () => panelServices,
    coreServices,
    graphShellApi,
    refreshDiagnostics,
  }));

  renderMigrationReport = panelsUi.renderMigrationReport;

  panelServices = createWorkflowPanelServices(buildPanelServicesDeps({
    els,
    store,
    staticConfig,
    setStatus,
    panelsUi,
    coreServices,
  }));

  const lateServices = createWorkflowLateServices(buildLateServicesDeps({
    els,
    setStatus,
    coreServices,
    panelServices,
    migrateLoadedWorkflowGraph,
    graphShellApi,
    store,
    canvas,
    staticConfig,
    defaultNodeConfig,
    renderAll,
    refreshOfflineBoundaryHint,
    state,
    renderNodeRuns,
    refreshDiagnostics,
    panelsUi,
  }));

  return combineWorkflowAppServices(coreServices, panelsUi, panelServices, lateServices);
}

export { createWorkflowAppServices };
