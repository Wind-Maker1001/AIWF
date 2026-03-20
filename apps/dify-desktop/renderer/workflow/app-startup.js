import {
  renderInitialWorkflowAppState,
  runWorkflowStartupRefreshes,
} from "./app-startup-support.js";

function initializeWorkflowApp(ctx = {}) {
  const {
    setStatus = () => {},
  } = ctx;

  renderInitialWorkflowAppState(ctx);
  runWorkflowStartupRefreshes(ctx);
  setStatus("就绪。可拖拽节点并连线后运行。", true);
}

export { initializeWorkflowApp };
