const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const {
  GLUE_PROVIDER,
  WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION,
  WORKFLOW_FAILURE_SUMMARY_SCHEMA_VERSION,
  WORKFLOW_RUN_ENTRY_SCHEMA_VERSION,
  WORKFLOW_RUN_TIMELINE_SCHEMA_VERSION,
  createWorkflowRunAuditStore,
} = require("../workflow_run_audit_store");
const {
  jsonResponse,
  governanceBoundaryResponse,
} = require("./governance_test_support");

test("workflow run audit store honors glue env override in offline_local mode", () => {
  const store = createWorkflowRunAuditStore({
    loadConfig: () => ({ mode: "offline_local" }),
    fs,
    runHistoryPath: () => "",
    workflowAuditPath: () => "",
    env: {
      AIWF_WORKFLOW_RUN_AUDIT_PROVIDER: "glue_http",
    },
  });

  assert.equal(store.resolveProvider({ mode: "offline_local" }), GLUE_PROVIDER);
});

test("workflow run audit store rejects retired local legacy provider", async () => {
  const store = createWorkflowRunAuditStore({
    loadConfig: () => ({ mode: "offline_local" }),
    fs,
    runHistoryPath: () => "",
    workflowAuditPath: () => "",
  });

  assert.equal(store.resolveProvider({ mode: "offline_local" }), GLUE_PROVIDER);

  const runs = await store.listRuns(20, {
    mode: "offline_local",
    workflowRunAuditProvider: "local_legacy",
  });
  assert.equal(runs.ok, false);
  assert.match(String(runs.error || ""), /retired/i);
});

test("workflow run audit store mirrors and queries glue backend", async () => {
  const remote = {
    runs: [],
    audits: [],
  };

  const store = createWorkflowRunAuditStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fs,
    runHistoryPath: () => "",
    workflowAuditPath: () => "",
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse(
          "workflow_run_audit",
          "/governance/workflow-runs",
          ["/governance/workflow-runs", "/governance/workflow-audit-events"],
        );
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "PUT" && url.includes("/governance/workflow-runs/")) {
        const run = JSON.parse(String(init.body || "{}")).run;
        remote.runs.unshift(run);
        return jsonResponse(200, { ok: true, item: run });
      }
      if (method === "POST" && url.endsWith("/governance/workflow-audit-events")) {
        const event = JSON.parse(String(init.body || "{}")).event;
        remote.audits.unshift(event);
        return jsonResponse(200, { ok: true, item: event });
      }
      if (method === "GET" && url.includes("/governance/workflow-runs?")) {
        return jsonResponse(200, { ok: true, items: remote.runs });
      }
      if (method === "GET" && url.endsWith("/governance/workflow-runs/run_1/timeline")) {
        return jsonResponse(200, {
          ok: true,
          provider: "glue-python",
          run_id: "run_1",
          status: "failed",
          timeline: [{ node_id: "n1", type: "quality_check_v3", status: "failed", seconds: 1 }],
        });
      }
      if (method === "GET" && url.includes("/governance/workflow-runs/failure-summary?")) {
        return jsonResponse(200, {
          ok: true,
          provider: "glue-python",
          total_runs: 1,
          failed_runs: 1,
          by_node: { quality_check_v3: { failed: 1, samples: ["boom"] } },
        });
      }
      if (method === "GET" && url.includes("/governance/workflow-audit-events?")) {
        return jsonResponse(200, { ok: true, items: remote.audits });
      }
      return jsonResponse(500, { ok: false, error: `unexpected request: ${method} ${url}` });
    },
  });

  await store.mirrorRun({
    run_id: "run_1",
    workflow_id: "wf_finance",
    status: "failed",
    ok: false,
    node_runs: [{ id: "n1", type: "quality_check_v3", status: "failed", error: "boom" }],
  }, { workflow_id: "wf_finance" }, { mode: "base_api" }, { mode: "base_api" });
  await store.mirrorAudit("run_workflow", { run_id: "run_1" }, { mode: "base_api" });

  const runs = await store.listRuns(20, { mode: "base_api" });
  assert.equal(runs.items.length, 1);
  assert.equal(runs.items[0].schema_version, WORKFLOW_RUN_ENTRY_SCHEMA_VERSION);
  assert.equal(runs.items[0].provider, GLUE_PROVIDER);

  const timeline = await store.getRunTimeline("run_1", { mode: "base_api" });
  assert.equal(timeline.provider, "glue-python");
  assert.equal(timeline.schema_version, WORKFLOW_RUN_TIMELINE_SCHEMA_VERSION);

  const failure = await store.getFailureSummary(20, { mode: "base_api" });
  assert.equal(failure.failed_runs, 1);
  assert.equal(failure.schema_version, WORKFLOW_FAILURE_SUMMARY_SCHEMA_VERSION);

  const audit = await store.listAuditLogs(20, "run_workflow", { mode: "base_api" });
  assert.equal(audit.items.length, 1);
  assert.equal(audit.items[0].schema_version, WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION);
  assert.equal(audit.items[0].provider, GLUE_PROVIDER);
});
