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
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-workflow-queue-"));
  const ipcMain = createIpcMain();
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
  return { ipcMain };
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
  const { ipcMain } = createCtx({
    runMinimalWorkflow: async () => ({
      ok: false,
      run_id: "run_review_1",
      status: "pending_review",
      pending_reviews: [{ review_key: "gate_a", status: "pending" }],
    }),
  });
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
});

test("workflow queue normalizes successful terminal status to done", async () => {
  const { ipcMain } = createCtx({
    runMinimalWorkflow: async () => ({
      ok: true,
      run_id: "run_pass_1",
      status: "passed",
    }),
  });
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
});

test("workflow queue cancels queued task but rejects terminal task cancellation", async () => {
  const { ipcMain } = createCtx({
    runMinimalWorkflow: async () => ({
      ok: false,
      run_id: "run_review_2",
      status: "pending_review",
      pending_reviews: [{ review_key: "gate_b", status: "pending" }],
    }),
  });
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
});
