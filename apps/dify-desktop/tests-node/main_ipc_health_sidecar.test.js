const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const { registerIpcHandlers } = require("../main_ipc");

function createIpcMain() {
  const handlers = new Map();
  return {
    handle(name, fn) {
      handlers.set(name, fn);
    },
    handlers,
  };
}

function createCtx(overrides = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-ipc-health-"));
  const ipcMain = createIpcMain();
  const ctx = {
    app: {
      getPath(name) {
        if (name === "documents") return path.join(root, "documents");
        if (name === "userData") return path.join(root, "userdata");
        return root;
      },
    },
    ipcMain,
    shell: { openPath: async () => "" },
    dialog: {},
    fs,
    path,
    loadConfig: () => ({ mode: "offline_local", glueUrl: "http://127.0.0.1:18081" }),
    saveConfig: () => {},
    baseHealth: async () => ({ ok: true }),
    glueHealth: async () => ({ ok: false, glueUrl: "http://127.0.0.1:18081", error: "down" }),
    runOfflineCleaningInWorker: async () => ({ ok: true }),
    runOfflinePrecheckInWorker: async () => ({ ok: true }),
    runOfflinePreviewInWorker: async () => ({ ok: true }),
    runViaBaseApi: async () => ({ ok: true }),
    listCleaningTemplates: () => ({ ok: true, templates: [] }),
    routeMetricsLogPath: () => path.join(root, "logs", "route_metrics.jsonl"),
    routeMetricsSummaryPath: () => path.join(root, "logs", "route_metrics_summary.json"),
    runModeAuditLogPath: () => path.join(root, "logs", "run_mode_audit.jsonl"),
    rotateLogIfNeeded: () => {},
    createWorkflowWindow: () => {},
    runMinimalWorkflow: async () => ({ ok: true, run_id: "r1" }),
    inspectFileEncoding: () => ({ path: "", ok: true }),
    toUtf8FileIfNeeded: (p) => ({ source: p, output: p, converted: false }),
    checkChineseOfficeFonts: () => ({ ok: true }),
    installBundledFontsForCurrentUser: async () => ({ ok: true }),
    checkTesseractRuntime: () => ({ ok: true }),
    checkTesseractLangs: () => ({ langs: ["chi_sim"] }),
    checkPdftoppmRuntime: () => ({ ok: true }),
    getTaskStoreStatus: async () => ({ ok: true, enabled: false, healthy: true }),
    ...overrides,
  };
  registerIpcHandlers(ctx);
  return ipcMain;
}

test("aiwf:health reports glue sidecar status in offline_local mode", async () => {
  const ipcMain = createCtx();
  const handler = ipcMain.handlers.get("aiwf:health");
  const out = await handler({}, { mode: "offline_local" });
  assert.equal(out.ok, true);
  assert.equal(out.mode, "offline_local");
  assert.equal(out.glue_sidecar.ok, false);
  assert.match(String(out.message || ""), /glue sidecar/i);
});

test("aiwf:precheckCleaning uses local precheck worker even in base_api mode", async () => {
  let seen = null;
  const ipcMain = createCtx({
    loadConfig: () => ({ mode: "base_api", baseUrl: "http://127.0.0.1:18080" }),
    runOfflinePrecheckInWorker: async (payload, outRoot, merged) => {
      seen = { payload, outRoot, merged };
      return { ok: true, precheck: { ok: true, precheck_action: "allow" } };
    },
    runViaBaseApi: async () => {
      throw new Error("should not be called");
    },
  });
  const handler = ipcMain.handlers.get("aiwf:precheckCleaning");
  const out = await handler({}, { params: { cleaning_template: "finance_report_v1" } }, { mode: "base_api" });
  assert.equal(out.ok, true);
  assert.equal(out.precheck.precheck_action, "allow");
  assert.equal(seen.merged.mode, "base_api");
  assert.match(String(seen.outRoot || ""), /AIWF/i);
});
