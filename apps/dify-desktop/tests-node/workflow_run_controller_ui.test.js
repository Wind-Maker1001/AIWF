const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadRunControllerUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/run-controller-ui.js")).href;
  return import(file);
}

test("workflow run controller runs workflow and refreshes follow-up panels", async () => {
  const { createWorkflowRunControllerUi } = await loadRunControllerUiModule();
  const statuses = [];
  const payloads = [];
  const nodeRuns = [];
  const refreshes = [];
  const els = {
    workflowName: { value: "Flow Alpha" },
    log: { textContent: "" },
  };
  global.window = {
    aiwfDesktop: {
      runWorkflow: async (payload) => {
        payloads.push(payload);
        return {
          ok: true,
          run_id: "run_1",
          node_runs: [{ id: "n1", status: "done" }],
          sla: { passed: true },
          lineage: { edge_count: 3 },
          governance: { ai_budget: { calls: 2 } },
        };
      },
    },
  };

  try {
    const ui = createWorkflowRunControllerUi(els, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      runWorkflowPreflight: async () => ({
        ok: false,
        issues: [{ level: "warning", message: "warn-a" }],
        risk: { score: 12, label: "low" },
      }),
      runPayload: (extra) => ({ workflow_id: "wf_1", ...extra }),
      renderNodeRuns: (items) => nodeRuns.push(items),
      refreshDiagnostics: async () => refreshes.push("diag"),
      refreshRunHistory: async () => refreshes.push("history"),
      refreshReviewQueue: async () => refreshes.push("review"),
    });

    await ui.runWorkflow();
  } finally {
    delete global.window;
  }

  assert.deepEqual(payloads, [{
    workflow_id: "wf_1",
    params: {
      strict_output_gate: true,
      preflight_passed: false,
      preflight_risk_score: 12,
      preflight_risk_label: "low",
    },
  }]);
  assert.deepEqual(nodeRuns, [[{ id: "n1", status: "done" }]]);
  assert.deepEqual(refreshes, ["diag", "history", "review"]);
  assert.match(els.log.textContent, /"run_id": "run_1"/);
  assert.deepEqual(statuses, [
    { text: "预检警告: warn-a", ok: true },
    { text: "预检未通过，已启用严格产物门禁：本次仅输出 Markdown 熟肉。", ok: true },
    { text: "工作流运行中...", ok: undefined },
    { text: "运行完成: run_1 | SLA:通过 | 血缘边:3 | AI调用:2", ok: true },
  ]);
});

test("workflow run controller enqueues workflow and refreshes queue", async () => {
  const { createWorkflowRunControllerUi } = await loadRunControllerUiModule();
  const statuses = [];
  const requests = [];
  const refreshes = [];
  global.window = {
    aiwfDesktop: {
      enqueueWorkflowTask: async (payload) => {
        requests.push(payload);
        return { ok: false, error: "busy" };
      },
    },
  };

  try {
    const ui = createWorkflowRunControllerUi({
      workflowName: { value: "Flow Beta" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      runWorkflowPreflight: async () => ({
        ok: true,
        issues: [],
        risk: { score: 0, label: "low" },
      }),
      runPayload: (extra) => ({ workflow_id: "wf_2", ...extra }),
      refreshQueue: async () => refreshes.push("queue"),
    });

    await ui.enqueueWorkflowRun();
  } finally {
    delete global.window;
  }

  assert.deepEqual(requests, [{
    label: "Flow Beta",
    payload: {
      workflow_id: "wf_2",
      params: {
        strict_output_gate: true,
        preflight_passed: true,
        preflight_risk_score: 0,
        preflight_risk_label: "low",
      },
    },
    cfg: {},
    priority: 100,
  }]);
  assert.deepEqual(refreshes, ["queue"]);
  assert.deepEqual(statuses, [{ text: "入队失败: busy", ok: false }]);
});
