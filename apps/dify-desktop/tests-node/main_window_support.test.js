const test = require("node:test");
const assert = require("node:assert/strict");
const { createWindowSupport } = require("../main_window_support");

function makeSupport(isPackaged, env = {}) {
  Object.assign(process.env, env);
  const created = [];
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
    Menu: { setApplicationMenu() {}, buildFromTemplate(v) { return v; } },
    shell: { openPath() {} },
    path: require("path"),
  });
  return { support, created };
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
  support.createWorkflowWindow({ debugApi: true });
  assert.equal(created.length, 1);
  assert.equal(created[0].options?.minWidth, 900);
  assert.equal(created[0].options?.minHeight, 680);
  assert.deepEqual(created[0].loaded?.loadOptions?.query, { debug: "1" });
});
