const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadQualityGateUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/quality-gate-ui.js")).href;
  return import(file);
}

test("workflow quality gate ui saves and loads preferences", async () => {
  const { createWorkflowQualityGateUi } = await loadQualityGateUiModule();
  const store = new Map();
  global.localStorage = {
    getItem: (key) => store.get(key) || null,
    setItem: (key, value) => store.set(key, value),
  };

  const els = {
    qualityGateRunIdFilter: { value: "run_123" },
    qualityGateStatusFilter: { value: "blocked" },
    qualityGateExportFormat: { value: "json" },
  };

  try {
    const ui = createWorkflowQualityGateUi(els, {
      qualityGatePrefsPayload: () => ({
        filter: { run_id: "run_123", status: "blocked" },
        format: "json",
      }),
    });
    ui.saveQualityGatePrefs();
    els.qualityGateRunIdFilter.value = "";
    els.qualityGateStatusFilter.value = "all";
    els.qualityGateExportFormat.value = "md";
    ui.loadQualityGatePrefs();
  } finally {
    delete global.localStorage;
  }

  assert.equal(els.qualityGateRunIdFilter.value, "run_123");
  assert.equal(els.qualityGateStatusFilter.value, "blocked");
  assert.equal(els.qualityGateExportFormat.value, "json");
});

test("workflow quality gate ui refreshes and exports reports", async () => {
  const { createWorkflowQualityGateUi } = await loadQualityGateUiModule();
  const statuses = [];
  const rows = [];
  global.localStorage = {
    getItem: () => null,
    setItem: () => {},
  };
  global.window = {
    aiwfDesktop: {
      listWorkflowQualityGateReports: async (payload) => {
        assert.deepEqual(payload, {
          limit: 120,
          filter: { run_id: "run_abc", status: "blocked" },
        });
        return { items: [{ run_id: "run_abc" }] };
      },
      exportWorkflowQualityGateReports: async (payload) => {
        assert.deepEqual(payload, {
          limit: 500,
          format: "json",
          filter: { run_id: "run_abc", status: "blocked" },
        });
        return { ok: true, path: "D:/exports/quality-gate.json" };
      },
    },
  };

  try {
    const ui = createWorkflowQualityGateUi({
      qualityGateExportFormat: { value: "json" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      qualityGatePrefsPayload: () => ({
        filter: { run_id: "run_abc", status: "blocked" },
        format: "json",
      }),
      qualityGateFilterPayload: () => ({ run_id: "run_abc", status: "blocked" }),
      renderQualityGateRows: (items) => rows.push(items),
    });
    await ui.refreshQualityGateReports();
    await ui.exportQualityGateReports();
  } finally {
    delete global.localStorage;
    delete global.window;
  }

  assert.deepEqual(rows, [[{ run_id: "run_abc" }]]);
  assert.deepEqual(statuses, [{ text: "质量门禁报告已导出: D:/exports/quality-gate.json", ok: true }]);
});

test("workflow quality gate ui formats structured export failure", async () => {
  const { createWorkflowQualityGateUi } = await loadQualityGateUiModule();
  const statuses = [];
  global.localStorage = {
    getItem: () => null,
    setItem: () => {},
  };
  global.window = {
    aiwfDesktop: {
      exportWorkflowQualityGateReports: async () => ({
        ok: false,
        error: "workflow contract invalid: workflow.version is required",
        error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
      }),
    },
  };

  try {
    const ui = createWorkflowQualityGateUi({
      qualityGateExportFormat: { value: "json" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      qualityGatePrefsPayload: () => ({ filter: {}, format: "json" }),
      qualityGateFilterPayload: () => ({}),
    });
    await ui.exportQualityGateReports();
  } finally {
    delete global.localStorage;
    delete global.window;
  }

  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].ok, false);
  assert.match(statuses[0].text, /\[required\]/);
  assert.match(statuses[0].text, /workflow\.version/);
});
