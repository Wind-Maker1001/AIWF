import { WorkflowCanvas } from "./canvas.js";
import { createWorkflowGraphShellUi } from "./graph-shell-ui.js";
import {
  buildGraphShellDeps,
  buildWorkflowCanvasDeps,
} from "./app-canvas-shell-support.js";

function createWorkflowCanvasShell(ctx = {}) {
  const canvas = new WorkflowCanvas(buildWorkflowCanvasDeps(ctx));

  function attachGraphShell({
    assignGraphShellApi = () => {},
    getResetWorkflowName = () => "自由编排流程",
    renderMigrationReport = () => {},
  } = {}) {
    assignGraphShellApi(createWorkflowGraphShellUi(
      ctx.els,
      buildGraphShellDeps({
        ...ctx,
        getResetWorkflowName,
        renderMigrationReport,
      })
    ));
  }

  return {
    attachGraphShell,
    canvas,
  };
}

export { createWorkflowCanvasShell };
