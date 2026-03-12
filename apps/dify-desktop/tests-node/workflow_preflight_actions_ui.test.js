const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPreflightActionsUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/preflight-actions-ui.js")).href;
  return import(file);
}

test("workflow preflight actions ui exports cached preflight report", async () => {
  const { createWorkflowPreflightActionsUi } = await loadPreflightActionsUiModule();
  const statuses = [];
  const cached = { ok: true, issues: [], ts: "2026-03-12T00:00:00Z" };
  global.window = {
    aiwfDesktop: {
      exportWorkflowPreflightReport: async (payload) => {
        assert.deepEqual(payload, { report: cached, format: "json" });
        return { ok: true, path: "D:/exports/preflight.json" };
      },
    },
  };

  try {
    const ui = createWorkflowPreflightActionsUi({
      preflightExportFormat: { value: "json" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      getLastPreflightReport: () => cached,
    });
    await ui.exportPreflightReport();
  } finally {
    delete global.window;
  }

  assert.deepEqual(statuses, [{ text: "预检报告已导出: D:/exports/preflight.json", ok: true }]);
});

test("workflow preflight actions ui runs template acceptance and exports report", async () => {
  const { createWorkflowPreflightActionsUi } = await loadPreflightActionsUiModule();
  const statuses = [];
  const log = { textContent: "" };
  const seen = { autoFix: null, report: null };
  let preflightCount = 0;
  global.window = {
    aiwfDesktop: {
      exportWorkflowTemplateAcceptanceReport: async (payload) => {
        assert.equal(payload.format, "md");
        assert.equal(payload.report.template_id, "tpl_1");
        return { ok: true, path: "D:/exports/template-acceptance.md" };
      },
    },
  };

  try {
    const ui = createWorkflowPreflightActionsUi({
      templateSelect: { value: "tpl_1" },
      templateAcceptanceExportFormat: { value: "md" },
      log,
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      runWorkflowPreflight: async () => {
        preflightCount += 1;
        return preflightCount === 1
          ? { ok: false, issues: [{ level: "error", message: "bad input" }] }
          : { ok: true, issues: [] };
      },
      allTemplates: () => [{ id: "tpl_1", name: "Template One" }],
      currentTemplateGovernance: () => ({ mode: "strict" }),
      autoFixGraphStructure: () => ({ changed: true, removed_dup_edges: 1 }),
      renderAutoFixDiff: (summary) => { seen.autoFix = summary; },
      setLastAutoFixSummary: (summary) => { seen.autoFix = summary; },
      setLastTemplateAcceptanceReport: (report) => { seen.report = report; },
      getLastTemplateAcceptanceReport: () => seen.report,
    });

    const report = await ui.runTemplateAcceptance();
    await ui.exportTemplateAcceptanceReport();
    assert.equal(report.accepted, true);
  } finally {
    delete global.window;
  }

  assert.equal(preflightCount, 2);
  assert.deepEqual(seen.autoFix, { changed: true, removed_dup_edges: 1 });
  assert.equal(seen.report?.template_name, "Template One");
  assert.match(log.textContent, /"template_id": "tpl_1"/);
  assert.deepEqual(statuses, [
    { text: "模板验收通过", ok: true },
    { text: "模板验收报告已导出: D:/exports/template-acceptance.md", ok: true },
  ]);
});

test("workflow preflight actions ui reports export failures", async () => {
  const { createWorkflowPreflightActionsUi } = await loadPreflightActionsUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      exportWorkflowPreflightReport: async () => ({ ok: false, canceled: false, error: "denied" }),
    },
  };

  try {
    const ui = createWorkflowPreflightActionsUi({
      preflightExportFormat: { value: "md" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      getLastPreflightReport: () => ({ ok: true, issues: [] }),
    });
    await ui.exportPreflightReport();
  } finally {
    delete global.window;
  }

  assert.deepEqual(statuses, [{ text: "导出预检报告失败: denied", ok: false }]);
});
