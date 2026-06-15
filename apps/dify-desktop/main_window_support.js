function createWindowSupport({ app, BrowserWindow, Menu, shell, path, loadConfig }) {
  function createWorkflowWindow(options = {}) {
    const legacyAdmin = !!options.legacyAdmin;
    const debugApi = !!options.debugApi && legacyAdmin;
    const query = {};
    if (debugApi) query.debug = "1";
    if (legacyAdmin) query.legacyAdmin = "1";
    const win = new BrowserWindow({
      width: 1560,
      height: 980,
      minWidth: 900,
      minHeight: 680,
      backgroundColor: "#f3f6fb",
      webPreferences: {
        preload: path.join(__dirname, "preload.js"),
        contextIsolation: true,
        nodeIntegration: false,
      },
    });
    win.loadFile(path.join(__dirname, "renderer", "workflow.html"), {
      query: Object.keys(query).length > 0 ? query : undefined,
    });
  }

  function appMenu() {
    const tpl = [{
      label: "帮助",
      submenu: [
        { label: "打开配置目录", click: () => shell.openPath(app.getPath("userData")) },
        { label: "打开输出目录", click: () => shell.openPath((() => { try { const cfg = typeof loadConfig === "function" ? loadConfig() : null; const fromCfg = String(cfg?.outputRoot || "").trim(); if (fromCfg) return fromCfg; } catch {} return path.join(app.getPath("desktop"), "AIWF_Builds"); })()) },
      ],
    }];
    Menu.setApplicationMenu(Menu.buildFromTemplate(tpl));
  }

  function createWindow() {
    appMenu();
    const win = new BrowserWindow({
      width: 1440,
      height: 940,
      minWidth: 860,
      minHeight: 680,
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

  function openWindowForArgv(argv = []) {
    const args = argv.map((x) => String(x || "").toLowerCase());
    const openOfflineHome = args.includes("--offline-home") || args.includes("/offline-home");
    const openWorkflowAdmin = args.includes("--workflow-admin") || args.includes("/workflow-admin");
    const openWorkflowOnly = args.includes("--workflow") || args.includes("/workflow") || openWorkflowAdmin;
    const debugWorkflowApi = openWorkflowAdmin && shouldEnableWorkflowDebugApi(args);
    if (openWorkflowOnly) createWorkflowWindow({ debugApi: debugWorkflowApi, legacyAdmin: openWorkflowAdmin });
    else createWindow();
    return {
      openOfflineHome,
      openWorkflowOnly,
      openWorkflowAdmin,
      debugWorkflowApi,
    };
  }

  function bootFromArgv(argv = []) {
    const launch = openWindowForArgv(argv);
    if (String(process.env.AIWF_RELEASE || "").trim() === "1") {
      // Release self-check: explicitly record debug API hard-disable status.
      // eslint-disable-next-line no-console
      console.log(`[AIWF_RELEASE] workflow_debug_api=${launch.debugWorkflowApi ? "enabled" : "disabled"}`);
    }
    return launch;
  }

  return {
    createWindow,
    createWorkflowWindow,
    openWindowForArgv,
    shouldEnableWorkflowDebugApi,
    bootFromArgv,
  };
}

module.exports = { createWindowSupport };
