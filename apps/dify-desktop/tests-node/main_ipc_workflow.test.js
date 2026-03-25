const test = require("node:test");
const assert = require("node:assert/strict");

const { createWorkflowAuditMirrorSupport } = require("../main_ipc_workflow");

test("workflow audit mirror support skips local audit jsonl for base_api", async () => {
  const localAudit = [];
  const localRuns = [];
  const remoteAudit = [];
  const remoteRuns = [];
  const support = createWorkflowAuditMirrorSupport({
    appendAudit: (action, detail) => localAudit.push({ action, detail }),
    appendRunHistory: (run, payload, config) => localRuns.push({ run, payload, config }),
    loadConfig: () => ({ mode: "base_api" }),
    workflowRunAuditStore: {
      resolveProvider: () => "glue_http",
      mirrorAudit: async (action, detail) => remoteAudit.push({ action, detail }),
      mirrorRun: async (run, payload, config) => remoteRuns.push({ run, payload, config }),
    },
  });

  support.appendAuditMirrored("run_workflow", { run_id: "run_1" });
  support.appendRunHistoryMirrored({ run_id: "run_1" }, { workflow_id: "wf_1" }, { mode: "base_api" });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(support.shouldWriteLocalAuditMirror({ mode: "base_api" }), false);
  assert.equal(support.shouldWriteLocalRunHistoryMirror({ mode: "base_api" }), false);
  assert.equal(localAudit.length, 0);
  assert.equal(localRuns.length, 0);
  assert.equal(remoteAudit.length, 1);
  assert.equal(remoteRuns.length, 1);
});

test("workflow audit mirror support no longer writes local mirrors after retirement", async () => {
  const localAudit = [];
  const localRuns = [];
  const remoteAudit = [];
  const remoteRuns = [];
  const support = createWorkflowAuditMirrorSupport({
    appendAudit: (action, detail) => localAudit.push({ action, detail }),
    appendRunHistory: (run, payload, config) => localRuns.push({ run, payload, config }),
    loadConfig: () => ({ mode: "offline_local", workflowRunAuditProvider: "local_legacy" }),
    workflowRunAuditStore: {
      resolveProvider: () => "local_legacy",
      mirrorAudit: async (action, detail) => remoteAudit.push({ action, detail }),
      mirrorRun: async (run, payload, config) => remoteRuns.push({ run, payload, config }),
    },
  });

  support.appendAuditMirrored("run_workflow", { run_id: "run_1" });
  support.appendRunHistoryMirrored({ run_id: "run_1" }, { workflow_id: "wf_1" }, { mode: "offline_local" });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(support.shouldWriteLocalAuditMirror({ mode: "offline_local" }), false);
  assert.equal(support.shouldWriteLocalRunHistoryMirror({ mode: "offline_local" }), false);
  assert.equal(localAudit.length, 0);
  assert.equal(localRuns.length, 0);
  assert.equal(remoteAudit.length, 1);
  assert.equal(remoteRuns.length, 1);
});
