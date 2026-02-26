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
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-ipc-"));
  const ipcMain = createIpcMain();
  const runModeAudit = path.join(root, "logs", "run_mode_audit.jsonl");
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
    loadConfig: () => ({ mode: "base_api", baseUrl: "http://127.0.0.1:18080", apiKey: "" }),
    saveConfig: () => {},
    baseHealth: async () => ({ ok: true }),
    runOfflineCleaningInWorker: async () => ({ ok: true, job_id: "local-1", artifacts: [] }),
    runOfflinePrecheckInWorker: async () => ({ ok: true, precheck: { ok: true } }),
    runViaBaseApi: async () => ({ ok: true, job_id: "remote-1", artifacts: [] }),
    listCleaningTemplates: () => ({ ok: true, templates: [] }),
    routeMetricsLogPath: () => path.join(root, "logs", "route_metrics.jsonl"),
    routeMetricsSummaryPath: () => path.join(root, "logs", "route_metrics_summary.json"),
    runModeAuditLogPath: () => runModeAudit,
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
  return { ctx, root, ipcMain, runModeAudit };
}

function readLastAuditLine(fp) {
  const raw = fs.readFileSync(fp, "utf8").trim();
  const lines = raw.split(/\r?\n/).filter(Boolean);
  return JSON.parse(lines[lines.length - 1]);
}

test("aiwf:runCleaning falls back to offline when base_api throws", async () => {
  const { ipcMain, runModeAudit } = createCtx({
    runViaBaseApi: async () => {
      throw new Error("network down");
    },
    runOfflineCleaningInWorker: async () => ({ ok: true, job_id: "local-fallback-1", artifacts: [{ kind: "md" }] }),
  });
  const h = ipcMain.handlers.get("aiwf:runCleaning");
  assert.equal(typeof h, "function");
  const out = await h({}, { params: { report_title: "x" } }, { mode: "base_api", enableOfflineFallback: true });
  assert.equal(out.ok, true);
  assert.equal(out.fallback_applied, true);
  assert.equal(out.fallback_reason, "remote_request_failed");
  assert.equal(out.job_id, "local-fallback-1");
  assert.equal(fs.existsSync(runModeAudit), true);
  const last = readLastAuditLine(runModeAudit);
  assert.equal(last.mode, "base_api");
  assert.equal(last.fallback_applied, true);
  assert.equal(last.reason, "remote_request_failed");
});

test("aiwf:runCleaning returns remote not-ok when fallback disabled", async () => {
  const { ipcMain, runModeAudit } = createCtx({
    runViaBaseApi: async () => ({ ok: false, job_id: "remote-failed", error: "bad request" }),
    runOfflineCleaningInWorker: async () => {
      throw new Error("should not be called");
    },
  });
  const h = ipcMain.handlers.get("aiwf:runCleaning");
  const out = await h({}, { params: {} }, { mode: "base_api", enableOfflineFallback: false });
  assert.equal(out.ok, false);
  assert.equal(out.job_id, "remote-failed");
  const last = readLastAuditLine(runModeAudit);
  assert.equal(last.mode, "base_api");
  assert.equal(last.fallback_applied, false);
  assert.equal(last.reason, "remote_returned_not_ok");
});

test("aiwf:runCleaning keeps remote success without fallback marker", async () => {
  const { ipcMain, runModeAudit } = createCtx({
    runViaBaseApi: async () => ({ ok: true, job_id: "remote-ok", artifacts: [{ kind: "xlsx" }] }),
  });
  const h = ipcMain.handlers.get("aiwf:runCleaning");
  const out = await h({}, { params: {} }, { mode: "base_api", enableOfflineFallback: true });
  assert.equal(out.ok, true);
  assert.equal(out.job_id, "remote-ok");
  assert.equal(out.fallback_applied, undefined);
  const last = readLastAuditLine(runModeAudit);
  assert.equal(last.mode, "base_api");
  assert.equal(last.fallback_applied, false);
  assert.equal(last.ok, true);
});

test("aiwf:runCleaning smart policy does not fallback on client_4xx error", async () => {
  const { ipcMain } = createCtx({
    runViaBaseApi: async () => {
      throw new Error("INPUT_INVALID: bad request http_400");
    },
    runOfflineCleaningInWorker: async () => ({ ok: true, job_id: "should-not-hit" }),
  });
  const h = ipcMain.handlers.get("aiwf:runCleaning");
  await assert.rejects(
    () => h({}, { params: {} }, { mode: "base_api", enableOfflineFallback: true, fallbackPolicy: "smart" }),
    /INPUT_INVALID|bad request|http_400/i,
  );
});
