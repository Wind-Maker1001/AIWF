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

test("aiwf:runCleaning surfaces base_api request failure without offline fallback", async () => {
  const { ipcMain, runModeAudit } = createCtx({
    runViaBaseApi: async () => {
      throw new Error("network down");
    },
    runOfflineCleaningInWorker: async () => {
      throw new Error("should not be called");
    },
  });
  const h = ipcMain.handlers.get("aiwf:runCleaning");
  assert.equal(typeof h, "function");
  await assert.rejects(
    () => h({}, { params: { report_title: "x" } }, { mode: "base_api" }),
    /network down/i,
  );
  assert.equal(fs.existsSync(runModeAudit), true);
  const last = readLastAuditLine(runModeAudit);
  assert.equal(last.mode, "base_api");
  assert.equal(last.ok, false);
  assert.equal(last.reason, "remote_request_failed");
  assert.match(String(last.remote_error || ""), /network down/i);
});

test("aiwf:runCleaning returns remote not-ok without offline fallback", async () => {
  const { ipcMain, runModeAudit } = createCtx({
    runViaBaseApi: async () => ({ ok: false, job_id: "remote-failed", error: "bad request" }),
    runOfflineCleaningInWorker: async () => {
      throw new Error("should not be called");
    },
  });
  const h = ipcMain.handlers.get("aiwf:runCleaning");
  const out = await h({}, { params: {} }, { mode: "base_api" });
  assert.equal(out.ok, false);
  assert.equal(out.job_id, "remote-failed");
  const last = readLastAuditLine(runModeAudit);
  assert.equal(last.mode, "base_api");
  assert.equal(last.ok, false);
  assert.equal(last.reason, "remote_returned_not_ok");
});

test("aiwf:runCleaning keeps remote success without fallback marker", async () => {
  const { ipcMain, runModeAudit } = createCtx({
    runViaBaseApi: async () => ({ ok: true, job_id: "remote-ok", artifacts: [{ kind: "xlsx" }] }),
  });
  const h = ipcMain.handlers.get("aiwf:runCleaning");
  const out = await h({}, { params: {} }, { mode: "base_api" });
  assert.equal(out.ok, true);
  assert.equal(out.job_id, "remote-ok");
  assert.equal(out.fallback_applied, undefined);
  const last = readLastAuditLine(runModeAudit);
  assert.equal(last.mode, "base_api");
  assert.equal(last.ok, true);
});

test("aiwf:runCleaning surfaces client_4xx error directly", async () => {
  const { ipcMain } = createCtx({
    runViaBaseApi: async () => {
      throw new Error("INPUT_INVALID: bad request http_400");
    },
    runOfflineCleaningInWorker: async () => ({ ok: true, job_id: "should-not-hit" }),
  });
  const h = ipcMain.handlers.get("aiwf:runCleaning");
  await assert.rejects(
    () => h({}, { params: {} }, { mode: "base_api" }),
    /INPUT_INVALID|bad request|http_400/i,
  );
});

test("aiwf:runCleaning writes execution fields into offline_local run mode audit", async () => {
  const { ipcMain, runModeAudit } = createCtx({
    loadConfig: () => ({ mode: "offline_local" }),
    runOfflineCleaningInWorker: async () => ({
      ok: true,
      job_id: "local-shadow-1",
      quality: { rust_v2_used: false },
      execution: {
        execution_mode: "python_legacy",
        eligibility_reason: "mode_off",
        requested_rust_v2_mode: "off",
        effective_rust_v2_mode: "off",
        verify_on_default: false,
        shadow_compare: {
          status: "skipped",
          mismatch_count: 0,
        },
      },
    }),
  });
  const h = ipcMain.handlers.get("aiwf:runCleaning");
  const out = await h({}, { params: {} }, { mode: "offline_local" });
  assert.equal(out.ok, true);
  const last = readLastAuditLine(runModeAudit);
  assert.equal(last.mode, "offline_local");
  assert.equal(last.execution_mode, "python_legacy");
  assert.equal(last.execution_eligibility_reason, "mode_off");
  assert.equal(last.requested_rust_v2_mode, "off");
  assert.equal(last.effective_rust_v2_mode, "off");
  assert.equal(last.verify_on_default, false);
  assert.equal(last.shadow_compare_status, "skipped");
  assert.equal(last.shadow_compare_mismatch_count, 0);
  assert.equal(last.rust_v2_used, false);
});

test("aiwf:runCleaning keeps structured remote guardrail block and audits blocking fields", async () => {
  const { ipcMain, runModeAudit } = createCtx({
    runViaBaseApi: async () => ({
      ok: false,
      job_id: "remote-guardrail",
      error: "profile mismatch blocked",
      error_code: "profile_mismatch_blocked",
      reason_codes: ["profile_mismatch_blocked"],
      template_id: "finance_report_v1",
      template_expected_profile: "finance_statement",
      quality_summary: {
        requested_profile: "finance_statement",
        recommended_profile: "debate_evidence",
        profile_mismatch: true,
        blocking_reason_codes: ["profile_mismatch", "profile_mismatch_blocked"],
        blank_output_expected: false,
        zero_output_unexpected: false,
      },
    }),
    runOfflineCleaningInWorker: async () => {
      throw new Error("should not be called");
    },
  });
  const h = ipcMain.handlers.get("aiwf:runCleaning");
  const out = await h({}, { params: {} }, { mode: "base_api" });
  assert.equal(out.ok, false);
  assert.equal(out.error_code, "profile_mismatch_blocked");
  assert.equal(out.quality_summary.recommended_profile, "debate_evidence");
  const last = readLastAuditLine(runModeAudit);
  assert.equal(last.mode, "base_api");
  assert.equal(last.ok, false);
  assert.equal(last.reason, "remote_returned_not_ok");
  assert.equal(last.template, "");
  assert.equal(last.template_expected_profile, "finance_statement");
  assert.equal(last.requested_profile, "finance_statement");
  assert.equal(last.recommended_profile, "debate_evidence");
  assert.equal(last.profile_mismatch, true);
  assert.deepEqual(last.blocking_reason_codes, ["profile_mismatch", "profile_mismatch_blocked"]);
  assert.equal(last.blank_output_expected, false);
  assert.equal(last.zero_output_unexpected, false);
});
