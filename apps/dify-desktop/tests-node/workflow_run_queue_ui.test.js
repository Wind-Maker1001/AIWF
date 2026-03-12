const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadRunQueueUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/run-queue-ui.js")).href;
  return import(file);
}

test("workflow run queue ui refreshes run history and falls back on errors", async () => {
  const { createWorkflowRunQueueUi } = await loadRunQueueUiModule();
  const renderCalls = [];
  global.window = {
    aiwfDesktop: {
      listWorkflowRuns: async (payload) => {
        assert.deepEqual(payload, { limit: 80 });
        return { items: [{ run_id: "run_1", status: "done" }] };
      },
    },
  };

  try {
    const ui = createWorkflowRunQueueUi({
      renderRunHistoryRows: (items) => renderCalls.push(items),
    });
    await ui.refreshRunHistory();
    global.window.aiwfDesktop.listWorkflowRuns = async () => {
      throw new Error("history unavailable");
    };
    await ui.refreshRunHistory();
  } finally {
    delete global.window;
  }

  assert.deepEqual(renderCalls, [
    [{ run_id: "run_1", status: "done" }],
    [],
  ]);
});

test("workflow run queue ui refreshes queue rows and control with fallback", async () => {
  const { createWorkflowRunQueueUi } = await loadRunQueueUiModule();
  const rowCalls = [];
  const controlCalls = [];
  global.window = {
    aiwfDesktop: {
      listWorkflowQueue: async (payload) => {
        assert.deepEqual(payload, { limit: 120 });
        return {
          items: [{ task_id: "task_1", status: "queued" }],
          control: { paused: false, quotas: { ai: 2 } },
        };
      },
    },
  };

  try {
    const ui = createWorkflowRunQueueUi({
      renderQueueRows: (items) => rowCalls.push(items),
      renderQueueControl: (control) => controlCalls.push(control),
    });
    await ui.refreshQueue();
    global.window.aiwfDesktop.listWorkflowQueue = async () => {
      throw new Error("queue unavailable");
    };
    await ui.refreshQueue();
  } finally {
    delete global.window;
  }

  assert.deepEqual(rowCalls, [
    [{ task_id: "task_1", status: "queued" }],
    [],
  ]);
  assert.deepEqual(controlCalls, [
    { paused: false, quotas: { ai: 2 } },
    {},
  ]);
});

test("workflow run queue ui pauses and resumes queue then refreshes queue state", async () => {
  const { createWorkflowRunQueueUi } = await loadRunQueueUiModule();
  const statuses = [];
  const controlRequests = [];
  const rowCalls = [];
  const controlCalls = [];
  global.window = {
    aiwfDesktop: {
      setWorkflowQueueControl: async (payload) => {
        controlRequests.push(payload);
        return payload.paused ? { ok: true } : { ok: false, error: "busy" };
      },
      listWorkflowQueue: async () => ({
        items: [{ task_id: "task_1", status: "queued" }],
        control: { paused: true },
      }),
    },
  };

  try {
    const ui = createWorkflowRunQueueUi({
      setStatus: (text, ok) => statuses.push({ text, ok }),
      renderQueueRows: (items) => rowCalls.push(items),
      renderQueueControl: (control) => controlCalls.push(control),
    });
    await ui.pauseQueue();
    await ui.resumeQueue();
  } finally {
    delete global.window;
  }

  assert.deepEqual(controlRequests, [{ paused: true }, { paused: false }]);
  assert.deepEqual(statuses, [
    { text: "队列已暂停", ok: true },
    { text: "恢复失败: busy", ok: false },
  ]);
  assert.deepEqual(rowCalls, [
    [{ task_id: "task_1", status: "queued" }],
    [{ task_id: "task_1", status: "queued" }],
  ]);
  assert.deepEqual(controlCalls, [{ paused: true }, { paused: true }]);
});
