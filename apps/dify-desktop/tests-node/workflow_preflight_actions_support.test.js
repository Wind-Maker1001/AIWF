const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPreflightActionsSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/preflight-actions-support.js")).href;
  return import(file);
}

test("workflow preflight actions support normalizes export format and template lookup", async () => {
  const {
    exportFormatFromValue,
    findTemplateById,
  } = await loadPreflightActionsSupportModule();

  assert.equal(exportFormatFromValue("json"), "json");
  assert.equal(exportFormatFromValue("MD"), "md");
  assert.equal(exportFormatFromValue(""), "md");

  const tpl = findTemplateById([
    { id: "tpl_1", name: "One" },
    { id: "tpl_2", name: "Two" },
  ], "tpl_2");
  assert.deepEqual(tpl, { id: "tpl_2", name: "Two" });
  assert.equal(findTemplateById([], "tpl_x"), null);
});

test("workflow preflight actions support builds acceptance report", async () => {
  const { buildTemplateAcceptanceReport } = await loadPreflightActionsSupportModule();
  const report = buildTemplateAcceptanceReport({
    templateId: "tpl_1",
    templateName: "Template One",
    accepted: true,
    governance: { mode: "strict" },
    before: { ok: false },
    autoFix: { changed: true },
    after: { ok: true },
    now: () => "2026-03-19T00:00:00.000Z",
  });

  assert.deepEqual(report, {
    ts: "2026-03-19T00:00:00.000Z",
    template_id: "tpl_1",
    template_name: "Template One",
    accepted: true,
    governance: { mode: "strict" },
    before: { ok: false },
    auto_fix: { changed: true },
    after: { ok: true },
  });
});

test("workflow report support builds versioned preflight report envelope", async () => {
  const { createWorkflowReportSupport } = require("../workflow_ipc_reports");
  const support = createWorkflowReportSupport({
    deepClone: (value) => JSON.parse(JSON.stringify(value)),
  });

  const envelope = support.buildPreflightReportEnvelope({
    ok: false,
    ts: "2026-03-19T00:00:00.000Z",
    risk: { score: 32, label: "medium" },
    issues: [
      {
        level: "error",
        kind: "graph_contract",
        message: "workflow.version is required",
        error_code: "required",
        error_path: "workflow.version",
        error_contract: "contracts/desktop/node_config_validation_errors.v1.json",
      },
    ],
  });

  assert.deepEqual(envelope, {
    schema_version: "workflow_preflight_report_export.v1",
    authority: "contracts/desktop/preflight_report_contract.v1.json",
    report: {
      schema_version: "workflow_preflight_report.v1",
      ok: false,
      ts: "2026-03-19T00:00:00.000Z",
      risk: { score: 32, label: "medium" },
      issues: [
        {
          level: "error",
          kind: "graph_contract",
          message: "workflow.version is required",
          error_code: "required",
          error_path: "workflow.version",
          error_contract: "contracts/desktop/node_config_validation_errors.v1.json",
        },
      ],
    },
  });
});
