const test = require("node:test");
const assert = require("node:assert/strict");
const { createWindowSupport } = require("../main_window_support");

function makeSupport(isPackaged, env = {}) {
  Object.assign(process.env, env);
  const created = [];
  let lastMenuTemplate = null;
  const support = createWindowSupport({
    app: { isPackaged, getPath: () => "D:\\tmp" },
    BrowserWindow: class {
      constructor(options) {
        this.options = options;
        created.push(this);
      }
      loadFile(file, loadOptions) {
        this.loaded = { file, loadOptions };
      }
    },
    Menu: {
      setApplicationMenu() {},
      buildFromTemplate(v) {
        lastMenuTemplate = v;
        return v;
      },
    },
    shell: { openPath() {} },
    path: require("path"),
  });
  return { support, created, getMenuTemplate: () => lastMenuTemplate };
}

function withEnv(env, fn) {
  const keys = ["AIWF_ALLOW_WORKFLOW_DEBUG_API", "NODE_ENV", "AIWF_RELEASE"];
  const prev = Object.fromEntries(keys.map((k) => [k, process.env[k]]));
  for (const k of keys) delete process.env[k];
  Object.assign(process.env, env || {});
  try {
    return fn();
  } finally {
    for (const k of keys) {
      if (typeof prev[k] === "undefined") delete process.env[k];
      else process.env[k] = prev[k];
    }
  }
}

test("debug api can be enabled in dev when flag requested", () => {
  withEnv({}, () => {
    const { support } = makeSupport(false);
    assert.equal(support.shouldEnableWorkflowDebugApi(["--workflow-debug-api"]), true);
  });
});

test("debug api stays disabled in packaged without strict env pair", () => {
  withEnv({ AIWF_ALLOW_WORKFLOW_DEBUG_API: "1", NODE_ENV: "production" }, () => {
    const { support } = makeSupport(true);
    assert.equal(support.shouldEnableWorkflowDebugApi(["--workflow-debug-api"]), false);
  });
});

test("debug api requires allow + development in packaged mode", () => {
  withEnv({ AIWF_ALLOW_WORKFLOW_DEBUG_API: "1", NODE_ENV: "development" }, () => {
    const { support } = makeSupport(true);
    assert.equal(support.shouldEnableWorkflowDebugApi(["--workflow-debug-api"]), true);
  });
});

test("debug api is hard-disabled in release mode", () => {
  withEnv({ AIWF_ALLOW_WORKFLOW_DEBUG_API: "1", NODE_ENV: "development", AIWF_RELEASE: "1" }, () => {
    const { support } = makeSupport(true);
    assert.equal(support.shouldEnableWorkflowDebugApi(["--workflow-debug-api"]), false);
  });
});

test("main window allows responsive stacked layout widths", () => {
  const { support, created } = makeSupport(false);
  support.createWindow();
  assert.equal(created.length, 1);
  assert.equal(created[0].options?.minWidth, 860);
  assert.equal(created[0].options?.minHeight, 680);
});

test("workflow window allows responsive canvas layout widths", () => {
  const { support, created } = makeSupport(false);
  support.createWorkflowWindow({ debugApi: true, legacyAdmin: true });
  assert.equal(created.length, 1);
  assert.equal(created[0].options?.minWidth, 900);
  assert.equal(created[0].options?.minHeight, 680);
  assert.deepEqual(created[0].loaded?.loadOptions?.query, { debug: "1", legacyAdmin: "1" });
});

test("workflow admin argv opens explicit legacy admin mode", () => {
  const { support, created } = makeSupport(false);
  support.bootFromArgv(["--workflow-admin"]);
  assert.equal(created.length, 1);
  assert.deepEqual(created[0].loaded?.loadOptions?.query, { legacyAdmin: "1" });
});

test("workflow admin mode can combine with debug api in dev", () => {
  withEnv({}, () => {
    const { support, created } = makeSupport(false);
    support.bootFromArgv(["--workflow-admin", "--workflow-debug-api"]);
    assert.equal(created.length, 1);
    assert.deepEqual(created[0].loaded?.loadOptions?.query, { debug: "1", legacyAdmin: "1" });
  });
});

test("workflow debug arg without admin does not expose debug query", () => {
  withEnv({}, () => {
    const { support, created } = makeSupport(false);
    support.bootFromArgv(["--workflow", "--workflow-debug-api"]);
    assert.equal(created.length, 1);
    assert.equal(created[0].loaded?.loadOptions?.query, undefined);
  });
});

test("offline home argv opens the compatibility home shell explicitly", () => {
  const { support, created } = makeSupport(false);
  const launch = support.openWindowForArgv(["--offline-home"]);
  assert.equal(created.length, 1);
  assert.equal(launch.launchMode, "offline_home");
  assert.equal(launch.openOfflineHome, true);
  assert.equal(launch.openWorkflowOnly, false);
  assert.equal(launch.openWorkflowAdmin, false);
  assert.equal(created[0].loaded?.file?.endsWith("index.html"), true);
});

test("openWindowForArgv preserves workflow compatibility mode for reopen", () => {
  const { support, created } = makeSupport(false);
  const launch = support.openWindowForArgv(["--workflow-admin", "--workflow-debug-api"]);
  assert.equal(created.length, 1);
  assert.equal(launch.launchMode, "workflow_admin");
  assert.equal(launch.openWorkflowOnly, true);
  assert.equal(launch.openWorkflowAdmin, true);
  assert.equal(launch.debugWorkflowApi, true);
  assert.deepEqual(created[0].loaded?.loadOptions?.query, { debug: "1", legacyAdmin: "1" });
});

test("openWindowForArgv falls back to home shell without workflow args", () => {
  const { support, created } = makeSupport(false);
  const launch = support.openWindowForArgv([]);
  assert.equal(created.length, 1);
  assert.equal(launch.launchMode, "home_default");
  assert.equal(launch.openOfflineHome, false);
  assert.equal(launch.openWorkflowOnly, false);
  assert.equal(launch.openWorkflowAdmin, false);
  assert.equal(created[0].loaded?.file?.endsWith("index.html"), true);
});

test("main menu hides legacy workflow launchers by default", () => {
  const { support, getMenuTemplate } = makeSupport(false);
  support.createWindow();
  const template = getMenuTemplate();
  assert.ok(Array.isArray(template));
  const helpMenu = template.find((item) => item?.label === "帮助");
  assert.ok(helpMenu);
  const labels = Array.isArray(helpMenu.submenu) ? helpMenu.submenu.map((item) => item?.label) : [];
  assert.ok(!labels.includes("打开 Legacy Workflow Studio"));
  assert.ok(!labels.includes("打开 Legacy Workflow 管理面"));
});
