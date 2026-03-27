const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const {
  createWorkflowReportSupport,
  registerWorkflowReportIpc,
} = require("../workflow_ipc_reports");

test("workflow ipc reports exports preflight json via versioned envelope", async () => {
  const handlers = {};
  const support = createWorkflowReportSupport({
    deepClone: (value) => JSON.parse(JSON.stringify(value)),
  });
  registerWorkflowReportIpc(
    {
      ipcMain: {
        handle(name, fn) {
          handlers[name] = fn;
        },
      },
      dialog: {},
      app: {
        getPath() {
          return path.join(os.tmpdir(), "aiwf-workflow-ipc-reports");
        },
      },
      fs,
      path,
    },
    {
      isMockIoAllowed: () => true,
      resolveMockFilePath: (filePath) => ({ ok: true, path: filePath }),
      nowIso: () => "2026-03-25T12:00:00.000Z",
      appendAudit: () => {},
      getRun: async () => null,
      listRunBaselines: async () => ({ ok: true, items: [] }),
      saveRunBaseline: async () => ({ ok: true }),
      buildRunCompare: async () => ({ ok: false, error: "not-used" }),
      buildRunRegressionAgainstBaseline: async () => ({ ok: false, error: "not-used" }),
      buildPreflightReportEnvelope: support.buildPreflightReportEnvelope,
      renderCompareHtml: () => "",
      renderCompareMarkdown: () => "",
      renderPreflightMarkdown: () => "not-used",
      renderTemplateAcceptanceMarkdown: () => "",
    },
  );

  const exportWorkflowPreflightReport = handlers["aiwf:exportWorkflowPreflightReport"];
  assert.equal(typeof exportWorkflowPreflightReport, "function");

  const targetPath = path.join(os.tmpdir(), `aiwf_preflight_${Date.now()}.json`);
  const out = await exportWorkflowPreflightReport(null, {
    mock: true,
    path: targetPath,
    format: "json",
    report: {
      ok: false,
      ts: "2026-03-25T11:00:00.000Z",
      risk: { score: 40, label: "medium" },
      issues: [{ level: "error", kind: "graph_contract", message: "workflow.version is required" }],
    },
  });

  assert.equal(out.ok, true);
  const payload = JSON.parse(fs.readFileSync(targetPath, "utf8"));
  assert.equal(payload.schema_version, "workflow_preflight_report_export.v1");
  assert.equal(payload.report.schema_version, "workflow_preflight_report.v1");
  assert.equal(payload.report.ts, "2026-03-25T11:00:00.000Z");
});
