function createWindowSupport({ app, BrowserWindow, Menu, shell, path }) {
  function createWorkflowWindow(options = {}) {
    const debugApi = !!options.debugApi;
    const win = new BrowserWindow({
      width: 1440,
      height: 920,
      minWidth: 1180,
      minHeight: 760,
      backgroundColor: "#f3f6fb",
      webPreferences: {
        preload: path.join(__dirname, "preload.js"),
        contextIsolation: true,
        nodeIntegration: false,
      },
    });
    win.loadFile(path.join(__dirname, "renderer", "workflow.html"), {
      query: debugApi ? { debug: "1" } : undefined,
    });
  }

  function appMenu() {
    const tpl = [{
      label: "帮助",
      submenu: [
        { label: "打开 Workflow Studio", click: () => createWorkflowWindow() },
        { label: "打开配置目录", click: () => shell.openPath(app.getPath("userData")) },
        { label: "打开输出目录", click: () => shell.openPath(path.join(app.getPath("documents"), "AIWF-Offline")) },
      ],
    }];
    Menu.setApplicationMenu(Menu.buildFromTemplate(tpl));
  }

  function createWindow() {
    appMenu();
    const win = new BrowserWindow({
      width: 1360,
      height: 900,
      minWidth: 1100,
      minHeight: 760,
      backgroundColor: "#eef6ff",
      webPreferences: {
        preload: path.join(__dirname, "preload.js"),
        contextIsolation: true,
        nodeIntegration: false,
      },
    });
    win.loadFile(path.join(__dirname, "renderer", "index.html"));
  }

  function shouldEnableWorkflowDebugApi(args = []) {
    const requested = args.includes("--workflow-debug-api");
    if (!requested) return false;
    if (String(process.env.AIWF_RELEASE || "").trim() === "1") return false;
    if (!app.isPackaged) return true;
    const allow = String(process.env.AIWF_ALLOW_WORKFLOW_DEBUG_API || "").trim() === "1";
    const devMode = String(process.env.NODE_ENV || "").toLowerCase() === "development";
    return allow && devMode;
  }

  function bootFromArgv(argv = []) {
    const args = argv.map((x) => String(x || "").toLowerCase());
    const openWorkflowOnly = args.includes("--workflow") || args.includes("/workflow");
    const debugWorkflowApi = shouldEnableWorkflowDebugApi(args);
    if (openWorkflowOnly) createWorkflowWindow({ debugApi: debugWorkflowApi });
    else createWindow();
    if (String(process.env.AIWF_RELEASE || "").trim() === "1") {
      // Release self-check: explicitly record debug API hard-disable status.
      // eslint-disable-next-line no-console
      console.log(`[AIWF_RELEASE] workflow_debug_api=${debugWorkflowApi ? "enabled" : "disabled"}`);
    }
  }

  return {
    createWindow,
    createWorkflowWindow,
    shouldEnableWorkflowDebugApi,
    bootFromArgv,
  };
}

module.exports = { createWindowSupport };

