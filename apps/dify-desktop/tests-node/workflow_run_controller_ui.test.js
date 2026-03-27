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
  assert.equal(statuses.length, 4);
  assert.equal(statuses[0].ok, true);
  assert.equal(statuses[1].ok, true);
  assert.equal(statuses[3].ok, true);
  assert.match(statuses[3].text, /run_1/);
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

test("workflow run controller formats structured workflow contract exception on run", async () => {
  const { createWorkflowRunControllerUi } = await loadRunControllerUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      runWorkflow: async () => ({ ok: true }),
    },
  };

  try {
    const ui = createWorkflowRunControllerUi({
      workflowName: { value: "Flow Gamma" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      runWorkflowPreflight: async () => ({ ok: true, issues: [], risk: { score: 0, label: "low" } }),
      runPayload: () => {
        const error = new Error("workflow contract invalid: workflow.version is required");
        error.code = "workflow_contract_invalid";
        error.details = {
          errors: ["workflow.version is required"],
          error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
        };
        throw error;
      },
    });

    await ui.runWorkflow();
  } finally {
    delete global.window;
  }

  assert.equal(statuses.length, 2);
  assert.equal(statuses[1].ok, false);
  assert.match(statuses[1].text, /\[required\] workflow\.version/);
});

test("workflow run controller formats structured workflow contract response on run", async () => {
  const { createWorkflowRunControllerUi } = await loadRunControllerUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      runWorkflow: async () => ({
        ok: false,
        error: "workflow contract invalid: workflow.version is required",
        error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
      }),
    },
  };

  try {
    const ui = createWorkflowRunControllerUi({
      workflowName: { value: "Flow Delta" },
      log: { textContent: "" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      runWorkflowPreflight: async () => ({ ok: true, issues: [], risk: { score: 0, label: "low" } }),
      runPayload: (extra) => ({ workflow_id: "wf_4", ...extra }),
      renderNodeRuns: () => {},
      refreshDiagnostics: async () => {},
      refreshRunHistory: async () => {},
      refreshReviewQueue: async () => {},
    });

    await ui.runWorkflow();
  } finally {
    delete global.window;
  }

  assert.equal(statuses.at(-1).ok, false);
  assert.match(statuses.at(-1).text, /\[required\] workflow\.version/);
});

test("workflow run controller formats structured queue failure response", async () => {
  const { createWorkflowRunControllerUi } = await loadRunControllerUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      enqueueWorkflowTask: async () => ({
        ok: false,
        error: "workflow contract invalid: workflow.version is required",
        error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
      }),
    },
  };

  try {
    const ui = createWorkflowRunControllerUi({
      workflowName: { value: "Flow Queue" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      runWorkflowPreflight: async () => ({ ok: true, issues: [], risk: { score: 0, label: "low" } }),
      runPayload: (extra) => ({ workflow_id: "wf_queue", ...extra }),
      refreshQueue: async () => {},
    });

    await ui.enqueueWorkflowRun();
  } finally {
    delete global.window;
  }

  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].ok, false);
  assert.match(statuses[0].text, /\[required\] workflow\.version/);
});
