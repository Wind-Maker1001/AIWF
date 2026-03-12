const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAuditUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/audit-ui.js")).href;
  return import(file);
}

test("workflow audit ui blocks timeline refresh when run id is blank", async () => {
  const { createWorkflowAuditUi } = await loadAuditUiModule();
  const statuses = [];
  const els = { timelineRunId: { value: "   " } };
  global.window = {
    aiwfDesktop: {
      getWorkflowRunTimeline: async () => {
        throw new Error("should not be called");
      },
    },
  };

  try {
    const ui = createWorkflowAuditUi(els, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
    });
    await ui.refreshTimeline();
  } finally {
    delete global.window;
  }

  assert.deepEqual(statuses, [{ text: "请先填写 Run ID", ok: false }]);
});

test("workflow audit ui refreshes timeline, failure summary, and audit rows", async () => {
  const { createWorkflowAuditUi } = await loadAuditUiModule();
  const timelineCalls = [];
  const failureCalls = [];
  const auditCalls = [];
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      getWorkflowRunTimeline: async (payload) => {
        timelineCalls.push(payload);
        return { ok: true, timeline: [{ node_id: "n1", type: "clean_md", status: "ok", seconds: 1.25 }] };
      },
      getWorkflowFailureSummary: async (payload) => {
        failureCalls.push(payload);
        return { by_node: { n2: { failed: 3, samples: ["boom"] } } };
      },
      listWorkflowAuditLogs: async (payload) => {
        auditCalls.push(payload);
        return { items: [{ ts: "2026-03-11T00:00:00Z", action: "refresh", detail: { ok: true } }] };
      },
    },
  };

  const rendered = {
    timeline: null,
    failure: null,
    audit: null,
  };

  try {
    const ui = createWorkflowAuditUi({ timelineRunId: { value: "run_123" } }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      renderTimelineRows: (out) => { rendered.timeline = out; },
      renderFailureRows: (out) => { rendered.failure = out; },
      renderAuditRows: (items) => { rendered.audit = items; },
    });

    await ui.refreshTimeline();
    await ui.refreshFailureSummary();
    await ui.refreshAudit();
  } finally {
    delete global.window;
  }

  assert.deepEqual(timelineCalls, [{ run_id: "run_123" }]);
  assert.deepEqual(failureCalls, [{ limit: 500 }]);
  assert.deepEqual(auditCalls, [{ limit: 120 }]);
  assert.equal(rendered.timeline?.ok, true);
  assert.deepEqual(rendered.failure?.by_node?.n2, { failed: 3, samples: ["boom"] });
  assert.deepEqual(rendered.audit, [{ ts: "2026-03-11T00:00:00Z", action: "refresh", detail: { ok: true } }]);
  assert.deepEqual(statuses, [{ text: "时间线刷新完成", ok: true }]);
});
