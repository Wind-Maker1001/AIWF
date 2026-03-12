const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadDiagnosticsPanelUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/diagnostics-panel-ui.js")).href;
  return import(file);
}

test("workflow diagnostics panel ui refreshes diagnostics with runtime and perf data", async () => {
  const { createWorkflowDiagnosticsPanelUi } = await loadDiagnosticsPanelUiModule();
  const calls = [];
  global.window = {
    aiwfDesktop: {
      getWorkflowDiagnostics: async (payload) => {
        assert.deepEqual(payload, { limit: 80 });
        return { by_chiplet: { ai_refine: { failure_rate: 0.1 } } };
      },
      getWorkflowPerfDashboard: async (payload) => {
        assert.deepEqual(payload, { limit: 200 });
        return { items: [{ chiplet: "ai_refine", p95_seconds: 1.23 }] };
      },
    },
  };

  try {
    const ui = createWorkflowDiagnosticsPanelUi({
      fetchRustRuntimeStats: async () => ({ items: [{ operator: "clean_md", calls: 10 }] }),
      renderDiagRuns: (summary, rust, perf) => calls.push({ summary, rust, perf }),
    });
    await ui.refreshDiagnostics();
  } finally {
    delete global.window;
  }

  assert.deepEqual(calls, [{
    summary: { by_chiplet: { ai_refine: { failure_rate: 0.1 } } },
    rust: { items: [{ operator: "clean_md", calls: 10 }] },
    perf: { items: [{ chiplet: "ai_refine", p95_seconds: 1.23 }] },
  }]);
});

test("workflow diagnostics panel ui swallows refresh failures", async () => {
  const { createWorkflowDiagnosticsPanelUi } = await loadDiagnosticsPanelUiModule();
  let rendered = false;
  global.window = {
    aiwfDesktop: {
      getWorkflowDiagnostics: async () => {
        throw new Error("diag unavailable");
      },
      getWorkflowPerfDashboard: async () => ({ items: [] }),
    },
  };

  try {
    const ui = createWorkflowDiagnosticsPanelUi({
      fetchRustRuntimeStats: async () => ({ items: [] }),
      renderDiagRuns: () => { rendered = true; },
    });
    await ui.refreshDiagnostics();
  } finally {
    delete global.window;
  }

  assert.equal(rendered, false);
});
