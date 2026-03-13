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

function createCtx() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-build-guard-"));
  const ipcMain = createIpcMain();
  const ctx = {
    app: {
      getPath(name) {
        if (name === "documents") return path.join(root, "documents");
        if (name === "desktop") return path.join(root, "desktop");
        if (name === "userData") return path.join(root, "userdata");
        return root;
      },
    },
    ipcMain,
    shell: { openPath: async () => "" },
    dialog: {},
    fs,
    path,
    loadConfig: () => ({ mode: "offline_local", baseUrl: "http://127.0.0.1:18080", apiKey: "" }),
    saveConfig: () => {},
    baseHealth: async () => ({ ok: true }),
    runOfflineCleaningInWorker: async () => ({ ok: true, job_id: "local-1", artifacts: [] }),
    runOfflinePrecheckInWorker: async () => ({ ok: true, precheck: { ok: true } }),
    runOfflinePreviewInWorker: async () => ({ ok: true, preview: {} }),
    runViaBaseApi: async () => ({ ok: true, job_id: "remote-1", artifacts: [] }),
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
  };
  registerIpcHandlers(ctx);
  return { root, ipcMain };
}

function gateLogPath(root) {
  return path.join(root, "userdata", "logs", "local_gate_checks.jsonl");
}

function appendGate(root, entry) {
  const fp = gateLogPath(root);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.appendFileSync(fp, `${JSON.stringify(entry)}\n`, "utf8");
}

test("aiwf:getBuildGuardStatus uses latest gate result per script", async () => {
  const { root, ipcMain } = createCtx();
  const now = Date.now();
  ["test:unit", "smoke", "test:regression", "test:regression:dirty", "test:office-gate"].forEach((script, idx) => {
    appendGate(root, { ts: new Date(now + idx * 1000).toISOString(), script, ok: true });
  });
  appendGate(root, { ts: new Date(now + 10_000).toISOString(), script: "test:unit", ok: false });

  const handler = ipcMain.handlers.get("aiwf:getBuildGuardStatus");
  const out = await handler();

  assert.equal(out.ok, true);
  assert.equal(out.gate_ok, false);
  assert.equal(out.latest_gate["test:unit"].ok, false);
  assert.deepEqual(out.missing, ["test:unit"]);
});
