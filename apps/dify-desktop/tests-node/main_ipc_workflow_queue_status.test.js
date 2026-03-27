const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const { registerIpcHandlers } = require("../main_ipc");
const { governanceBoundaryResponseFromEntries } = require("./governance_test_support");
const { createWorkflowStoreRemoteError } = require("../workflow_store_remote_error");

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
  const fetchImplOverride = typeof overrides.fetchImpl === "function" ? overrides.fetchImpl : null;
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-workflow-queue-"));
  const ipcMain = createIpcMain();
  const remote = {
    sandboxRules: {
      whitelist_codes: [],
      whitelist_node_types: [],
      whitelist_keys: [],
      mute_until_by_key: {},
    },
    sandboxAutoFixState: {
      violation_events: [],
      forced_isolation_mode: "",
      forced_until: "",
      last_actions: [],
      green_streak: 0,
    },
    manualReviews: [],
  };
  const prevFetch = global.fetch;
  global.fetch = fetchImplOverride || (async (url, init = {}) => {
    const method = String(init.method || "GET").toUpperCase();
    const target = String(url);
    if (method === "GET" && target.endsWith("/governance/meta/control-plane")) {
      return governanceBoundaryResponseFromEntries([
        {
          capability: "workflow_sandbox_rules",
          route_prefix: "/governance/workflow-sandbox/rules",
          owned_route_prefixes: ["/governance/workflow-sandbox/rules", "/governance/workflow-sandbox/rule-versions"],
        },
        {
          capability: "workflow_sandbox_autofix",
          route_prefix: "/governance/workflow-sandbox/autofix-state",
          owned_route_prefixes: ["/governance/workflow-sandbox/autofix-state", "/governance/workflow-sandbox/autofix-actions"],
        },
        {
          capability: "manual_reviews",
          route_prefix: "/governance/manual-reviews",
          owned_route_prefixes: ["/governance/manual-reviews"],
        },
        {
          capability: "workflow_run_audit",
          route_prefix: "/governance/workflow-runs",
          owned_route_prefixes: ["/governance/workflow-runs", "/governance/workflow-audit-events"],
        },
      ]);
    }
    if (method === "GET" && target.endsWith("/governance/workflow-sandbox/rules")) {
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({ ok: true, rules: remote.sandboxRules });
        },
      };
    }
    if (method === "GET" && target.endsWith("/governance/workflow-sandbox/autofix-state")) {
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({ ok: true, state: remote.sandboxAutoFixState });
        },
      };
    }
    if (method === "PUT" && target.endsWith("/governance/workflow-sandbox/autofix-state")) {
      remote.sandboxAutoFixState = JSON.parse(String(init.body || "{}"));
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({ ok: true, state: remote.sandboxAutoFixState });
        },
      };
    }
    if (method === "POST" && target.endsWith("/governance/manual-reviews/enqueue")) {
      const body = JSON.parse(String(init.body || "{}"));
      remote.manualReviews = Array.isArray(body.items) ? body.items : [];
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({ ok: true, items: remote.manualReviews });
        },
      };
    }
    if (method === "PUT" && target.includes("/governance/workflow-runs/")) {
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({ ok: true, item: {} });
        },
      };
    }
    if (method === "POST" && target.endsWith("/governance/workflow-audit-events")) {
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({ ok: true, item: {} });
        },
      };
    }
    throw new Error(`unexpected fetch: ${method} ${target}`);
  });
  const ctx = {
    app: {
      isPackaged: false,
      getPath(name) {
        if (name === "documents") return path.join(root, "documents");
        if (name === "userData") return path.join(root, "userdata");
        if (name === "desktop") return path.join(root, "desktop");
        return root;
      },
    },
    ipcMain,
    shell: { openPath: async () => "" },
    dialog: {},
    fs,
    path,
    loadConfig: () => ({ mode: "offline_local" }),
    saveConfig: () => {},
    baseHealth: async () => ({ ok: true }),
    runOfflineCleaningInWorker: async () => ({ ok: true, job_id: "local-1", artifacts: [] }),
    runOfflinePrecheckInWorker: async () => ({ ok: true, precheck: { ok: true } }),
    runViaBaseApi: async () => ({ ok: true, job_id: "remote-1", artifacts: [] }),
    listCleaningTemplates: () => ({ ok: true, templates: [] }),
    routeMetricsLogPath: () => path.join(root, "logs", "route_metrics.jsonl"),
    routeMetricsSummaryPath: () => path.join(root, "logs", "route_metrics_summary.json"),
    runModeAuditLogPath: () => path.join(root, "logs", "run_mode_audit.jsonl"),
    rotateLogIfNeeded: () => {},
    createWorkflowWindow: () => {},
    runMinimalWorkflow: async () => ({ ok: true, run_id: "r1", status: "passed" }),
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
  return {
    ipcMain,
    restore() {
      if (typeof prevFetch === "undefined") {
        delete global.fetch;
      } else {
        global.fetch = prevFetch;
      }
    },
  };
}

async function waitForQueueStatus(listHandler, taskId, expectedStatus) {
  for (let i = 0; i < 50; i += 1) {
    const out = await listHandler({}, { limit: 20 });
    const item = (out?.items || []).find((x) => String(x.task_id || "") === taskId);
    if (item && String(item.status || "") === expectedStatus) {
      return item;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`queue status did not reach ${expectedStatus}`);
}

test("workflow queue preserves pending_review terminal status", async () => {
  const { ipcMain, restore } = createCtx({
    runMinimalWorkflow: async () => ({
      ok: false,
      run_id: "run_review_1",
      status: "pending_review",
      pending_reviews: [{
        run_id: "run_review_1",
        workflow_id: "wf_1",
        node_id: "n_review",
        review_key: "gate_a",
        reviewer: "reviewer",
        comment: "",
        created_at: "2026-03-25T00:00:00Z",
        status: "pending",
      }],
    }),
  });
  try {
    const enqueue = ipcMain.handlers.get("aiwf:enqueueWorkflowTask");
    const listQueue = ipcMain.handlers.get("aiwf:listWorkflowQueue");
    assert.equal(typeof enqueue, "function");
    assert.equal(typeof listQueue, "function");

    const enqueued = await enqueue({}, {
      task_id: "task_review_1",
      label: "review workflow",
      payload: { workflow: { workflow_id: "wf_1", nodes: [], edges: [] } },
      cfg: {},
      priority: 100,
    });
    assert.equal(enqueued.ok, true);

    const item = await waitForQueueStatus(listQueue, "task_review_1", "pending_review");
    assert.equal(String(item.run_id || ""), "run_review_1");
    assert.equal(String(item.result?.status || ""), "pending_review");
  } finally {
    restore();
  }
});

test("workflow queue normalizes successful terminal status to done", async () => {
  const { ipcMain, restore } = createCtx({
    runMinimalWorkflow: async () => ({
      ok: true,
      run_id: "run_pass_1",
      status: "passed",
    }),
  });
  try {
    const enqueue = ipcMain.handlers.get("aiwf:enqueueWorkflowTask");
    const listQueue = ipcMain.handlers.get("aiwf:listWorkflowQueue");

    const enqueued = await enqueue({}, {
      task_id: "task_pass_1",
      label: "pass workflow",
      payload: { workflow: { workflow_id: "wf_2", nodes: [], edges: [] } },
      cfg: {},
      priority: 100,
    });
    assert.equal(enqueued.ok, true);

    const item = await waitForQueueStatus(listQueue, "task_pass_1", "done");
    assert.equal(String(item.result?.status || ""), "passed");
  } finally {
    restore();
  }
});

test("workflow queue cancels queued task but rejects terminal task cancellation", async () => {
  const { ipcMain, restore } = createCtx({
    runMinimalWorkflow: async () => ({
      ok: false,
      run_id: "run_review_2",
      status: "pending_review",
      pending_reviews: [{
        run_id: "run_review_2",
        workflow_id: "wf_3",
        node_id: "n_review",
        review_key: "gate_b",
        reviewer: "reviewer",
        comment: "",
        created_at: "2026-03-25T00:00:00Z",
        status: "pending",
      }],
    }),
  });
  try {
    const enqueue = ipcMain.handlers.get("aiwf:enqueueWorkflowTask");
    const listQueue = ipcMain.handlers.get("aiwf:listWorkflowQueue");
    const cancel = ipcMain.handlers.get("aiwf:cancelWorkflowTask");
    const setControl = ipcMain.handlers.get("aiwf:setWorkflowQueueControl");

    const paused = await setControl({}, { paused: true });
    assert.equal(paused.ok, true);

    const queued = await enqueue({}, {
      task_id: "task_queue_1",
      label: "queued workflow",
      payload: { workflow: { workflow_id: "wf_queue", nodes: [], edges: [] } },
      cfg: { chiplet_isolation_enabled: false },
      priority: 100,
    });
    assert.equal(queued.ok, true);

    const canceled = await cancel({}, { task_id: "task_queue_1" });
    assert.equal(canceled.ok, true);
    const canceledItem = await waitForQueueStatus(listQueue, "task_queue_1", "canceled");
    assert.equal(String(canceledItem.status || ""), "canceled");

    const resumed = await setControl({}, { paused: false });
    assert.equal(resumed.ok, true);

    const enqueuedReview = await enqueue({}, {
      task_id: "task_review_2",
      label: "review workflow 2",
      payload: { workflow: { workflow_id: "wf_3", nodes: [], edges: [] } },
      cfg: {},
      priority: 100,
    });
    assert.equal(enqueuedReview.ok, true);
    const terminalItem = await waitForQueueStatus(listQueue, "task_review_2", "pending_review");
    assert.equal(String(terminalItem.status || ""), "pending_review");

    const rejected = await cancel({}, { task_id: "task_review_2" });
    assert.equal(rejected.ok, false);
    assert.match(String(rejected.error || ""), /not cancellable/i);
    const afterReject = await waitForQueueStatus(listQueue, "task_review_2", "pending_review");
    assert.equal(String(afterReject.status || ""), "pending_review");
  } finally {
    restore();
  }
});

test("workflow queue preserves structured failure payloads when background execution fails", async () => {
  const { ipcMain, restore } = createCtx({
    runMinimalWorkflow: async () => ({ ok: true, run_id: "unused", status: "done" }),
    fetchImpl: async (url, init = {}) => {
      const method = String(init.method || "GET").toUpperCase();
      const target = String(url);
      if (method === "GET" && target.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponseFromEntries([
          {
            capability: "workflow_sandbox_rules",
            route_prefix: "/governance/workflow-sandbox/rules",
            owned_route_prefixes: ["/governance/workflow-sandbox/rules", "/governance/workflow-sandbox/rule-versions"],
          },
        ]);
      }
      if (method === "GET" && target.endsWith("/governance/workflow-sandbox/rules")) {
        return {
          ok: false,
          status: 400,
          async text() {
            return JSON.stringify({
              ok: false,
              error: "workflow graph invalid: workflow contains unregistered node types: unknown_future_node",
              error_code: "workflow_graph_invalid",
              error_items: [{
                path: "workflow.nodes",
                code: "unknown_node_type",
                message: "workflow contains unregistered node types: unknown_future_node",
              }],
            });
          },
        };
      }
      throw new Error(`unexpected fetch: ${method} ${target}`);
    },
  });
  try {
    const enqueue = ipcMain.handlers.get("aiwf:enqueueWorkflowTask");
    const listQueue = ipcMain.handlers.get("aiwf:listWorkflowQueue");
    assert.equal(typeof enqueue, "function");
    assert.equal(typeof listQueue, "function");

    const enqueued = await enqueue({}, {
      task_id: "task_fail_structured",
      label: "structured failure workflow",
      payload: { workflow: { workflow_id: "wf_fail", version: "1.0.0", nodes: [], edges: [] } },
      cfg: {},
      priority: 100,
    });
    assert.equal(enqueued.ok, true);

    const item = await waitForQueueStatus(listQueue, "task_fail_structured", "failed");
    assert.equal(String(item.error || "").includes("unknown_future_node"), true);
    assert.equal(item.result?.error_code, "workflow_graph_invalid");
    assert.ok(Array.isArray(item.result?.error_items));
    assert.ok(item.result.error_items.some((entry) => entry.path === "workflow.nodes" && entry.code === "unknown_node_type"));
  } finally {
    restore();
  }
});

test("workflow queue preserves pending-review status when review enqueue fails", async () => {
  const { ipcMain, restore } = createCtx({
    runMinimalWorkflow: async () => ({
      ok: false,
      run_id: "run_pending_fail",
      status: "pending_review",
      pending_reviews: [{ review_key: "gate_fail", status: "pending" }],
    }),
    fetchImpl: async (url, init = {}) => {
      const method = String(init.method || "GET").toUpperCase();
      const target = String(url);
      if (method === "GET" && target.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponseFromEntries([
          {
            capability: "workflow_sandbox_rules",
            route_prefix: "/governance/workflow-sandbox/rules",
            owned_route_prefixes: ["/governance/workflow-sandbox/rules", "/governance/workflow-sandbox/rule-versions"],
          },
          {
            capability: "workflow_sandbox_autofix",
            route_prefix: "/governance/workflow-sandbox/autofix-state",
            owned_route_prefixes: ["/governance/workflow-sandbox/autofix-state", "/governance/workflow-sandbox/autofix-actions"],
          },
          {
            capability: "manual_reviews",
            route_prefix: "/governance/manual-reviews",
            owned_route_prefixes: ["/governance/manual-reviews"],
          },
        ]);
      }
      if (method === "GET" && target.endsWith("/governance/workflow-sandbox/rules")) {
        return {
          ok: true,
          status: 200,
          async text() {
            return JSON.stringify({ ok: true, rules: { whitelist_codes: [], whitelist_node_types: [], whitelist_keys: [], mute_until_by_key: {} } });
          },
        };
      }
      if (method === "GET" && target.endsWith("/governance/workflow-sandbox/autofix-state")) {
        return {
          ok: true,
          status: 200,
          async text() {
            return JSON.stringify({
              ok: true,
              state: {
                violation_events: [],
                forced_isolation_mode: "",
                forced_until: "",
                last_actions: [],
                green_streak: 0,
              },
            });
          },
        };
      }
      if (method === "PUT" && target.endsWith("/governance/workflow-sandbox/autofix-state")) {
        return {
          ok: true,
          status: 200,
          async text() {
            return JSON.stringify({ ok: true, state: {} });
          },
        };
      }
      if (method === "POST" && target.endsWith("/governance/manual-reviews/enqueue")) {
        return {
          ok: false,
          status: 503,
          async text() {
            return JSON.stringify({
              ok: false,
              error: "manual review queue unavailable",
              error_code: "manual_review_store_unavailable",
              error_items: [{ path: "review_queue", code: "unavailable", message: "manual review queue unavailable" }],
            });
          },
        };
      }
      if (method === "PUT" && target.includes("/governance/workflow-runs/")) {
        return {
          ok: true,
          status: 200,
          async text() {
            return JSON.stringify({ ok: true, item: {} });
          },
        };
      }
      if (method === "POST" && target.endsWith("/governance/workflow-audit-events")) {
        return {
          ok: true,
          status: 200,
          async text() {
            return JSON.stringify({ ok: true, item: {} });
          },
        };
      }
      throw new Error(`unexpected fetch: ${method} ${target}`);
    },
  });
  try {
    const enqueue = ipcMain.handlers.get("aiwf:enqueueWorkflowTask");
    const listQueue = ipcMain.handlers.get("aiwf:listWorkflowQueue");
    assert.equal(typeof enqueue, "function");
    assert.equal(typeof listQueue, "function");

    const enqueued = await enqueue({}, {
      task_id: "task_pending_fail",
      label: "pending review enqueue fail",
      payload: { workflow: { workflow_id: "wf_pending_fail", version: "1.0.0", nodes: [], edges: [] } },
      cfg: {},
      priority: 100,
    });
    assert.equal(enqueued.ok, true);

    const item = await waitForQueueStatus(listQueue, "task_pending_fail", "pending_review");
    assert.equal(item.result?.status, "pending_review");
    assert.equal(item.result?.review_enqueue_failed, true);
    assert.equal(item.result?.review_enqueue?.error_code, "manual_review_store_unavailable");
    assert.ok(item.result?.review_enqueue?.error_items.some((entry) => entry.path === "review_queue"));
  } finally {
    restore();
  }
});
