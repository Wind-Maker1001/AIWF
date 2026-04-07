const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const {
  BASE_PROVIDER,
  LOCAL_PROVIDER,
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

test("workflow run audit store defaults to local runtime provider in offline_local mode", () => {
  const store = createWorkflowRunAuditStore({
    loadConfig: () => ({ mode: "offline_local" }),
    fs,
    runHistoryPath: () => "",
    workflowAuditPath: () => "",
  });

  assert.equal(store.resolveProvider({ mode: "offline_local" }), LOCAL_PROVIDER);
});

test("workflow run audit store defaults to local runtime provider even in base_api mode", () => {
  const store = createWorkflowRunAuditStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fs,
    runHistoryPath: () => "",
    workflowAuditPath: () => "",
  });

  assert.equal(store.resolveProvider({ mode: "base_api" }), LOCAL_PROVIDER);
});

test("workflow run audit store rejects unsupported provider override", async () => {
  const store = createWorkflowRunAuditStore({
    loadConfig: () => ({ mode: "offline_local" }),
    fs,
    runHistoryPath: () => "",
    workflowAuditPath: () => "",
  });

  assert.equal(store.resolveProvider({ mode: "offline_local" }), LOCAL_PROVIDER);

  const runs = await store.listRuns(20, {
    mode: "offline_local",
    workflowRunAuditProvider: "unsupported_provider",
  });
  assert.equal(runs.ok, false);
  assert.match(String(runs.error || ""), /unsupported|provider/i);
});

test("workflow run audit store reads local runtime history in offline_local mode", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-run-audit-"));
  const runHistoryFile = path.join(root, "run_history.jsonl");
  const auditFile = path.join(root, "workflow_audit.jsonl");
  fs.writeFileSync(runHistoryFile, `${JSON.stringify({
    ts: "2026-03-28T00:00:00Z",
    run_id: "run_local_1",
    workflow_id: "wf_local",
    status: "failed",
    ok: false,
    run_request_kind: "reference",
    version_id: "ver_local_1",
    published_version_id: "ver_local_1",
    workflow_definition_source: "version_reference",
    payload: { version_id: "ver_local_1", published_version_id: "ver_local_1" },
    config: { mode: "offline_local" },
    result: {
      ok: false,
      status: "failed",
      node_runs: [{ id: "n1", type: "quality_check_v3", status: "failed", error: "boom", started_at: "2026-03-28T00:00:00Z", ended_at: "2026-03-28T00:00:01Z", seconds: 1 }],
    },
  })}\n`, "utf8");
  fs.writeFileSync(auditFile, `${JSON.stringify({
    ts: "2026-03-28T00:00:02Z",
    action: "run_workflow",
    detail: { run_id: "run_local_1" },
  })}\n`, "utf8");

  const store = createWorkflowRunAuditStore({
    loadConfig: () => ({ mode: "offline_local" }),
    fs,
    runHistoryPath: () => runHistoryFile,
    workflowAuditPath: () => auditFile,
  });

  const runs = await store.listRuns(20, { mode: "offline_local" });
  assert.equal(runs.provider, LOCAL_PROVIDER);
  assert.equal(runs.items.length, 1);
  assert.equal(runs.items[0].source_of_truth, "desktop.workflow_runtime.run_history");
  assert.equal(runs.items[0].run_request_kind, "reference");
  assert.equal(runs.items[0].version_id, "ver_local_1");
  assert.equal(runs.items[0].published_version_id, "ver_local_1");

  const hit = await store.getRun("run_local_1", { mode: "offline_local" });
  assert.equal(hit.provider, LOCAL_PROVIDER);
  assert.equal(hit.run_id, "run_local_1");
  assert.equal(hit.workflow_definition_source, "version_reference");

  const timeline = await store.getRunTimeline("run_local_1", { mode: "offline_local" });
  assert.equal(timeline.provider, LOCAL_PROVIDER);
  assert.equal(timeline.schema_version, WORKFLOW_RUN_TIMELINE_SCHEMA_VERSION);
  assert.equal(timeline.timeline.length, 1);

  const failure = await store.getFailureSummary(20, { mode: "offline_local" });
  assert.equal(failure.provider, LOCAL_PROVIDER);
  assert.equal(failure.failed_runs, 1);
  assert.equal(failure.schema_version, WORKFLOW_FAILURE_SUMMARY_SCHEMA_VERSION);

  const audit = await store.listAuditLogs(20, "run_workflow", { mode: "offline_local" });
  assert.equal(audit.provider, LOCAL_PROVIDER);
  assert.equal(audit.items.length, 1);
  assert.equal(audit.items[0].schema_version, WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION);
});

test("workflow run audit store rejects deprecated glue_http provider", async () => {
  const store = createWorkflowRunAuditStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081", workflowRunAuditProvider: "glue_http" }),
    fs,
    runHistoryPath: () => "",
    workflowAuditPath: () => "",
  });

  assert.throws(() => store.resolveProvider({ mode: "base_api", workflowRunAuditProvider: "glue_http" }), /unsupported|glue_http/i);
});

test("workflow run audit store queries lifecycle backend when base_http is explicitly requested", async () => {
  const store = createWorkflowRunAuditStore({
    loadConfig: () => ({ mode: "base_api", baseUrl: "http://127.0.0.1:18080", workflowRunAuditProvider: "base_http" }),
    fs,
    runHistoryPath: () => "",
    workflowAuditPath: () => "",
    fetchImpl: async (url, init = {}) => {
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.includes("/api/v1/jobs/history?")) {
        return jsonResponse(200, [{
          schema_version: "lifecycle_run_record.v1",
          owner: "base-java",
          source_of_truth: "base-java.jobs",
          run_id: "job_1",
          ts: "2026-03-28T00:00:00Z",
          run_request_kind: "reference",
          version_id: "ver_backend_1",
          published_version_id: "ver_backend_1",
          workflow_definition_source: "version_reference",
          workflow_id: "cleaning",
          status: "DONE",
          ok: true,
          payload: { version_id: "ver_backend_1", published_version_id: "ver_backend_1" },
          config: {},
          result: { steps: [], artifacts: [] },
        }]);
      }
      if (method === "GET" && url.endsWith("/api/v1/jobs/job_1/record")) {
        return jsonResponse(200, {
          schema_version: "lifecycle_run_record.v1",
          owner: "base-java",
          source_of_truth: "base-java.jobs",
          run_id: "job_1",
          ts: "2026-03-28T00:00:00Z",
          run_request_kind: "reference",
          version_id: "ver_backend_1",
          published_version_id: "ver_backend_1",
          workflow_definition_source: "version_reference",
          workflow_id: "cleaning",
          status: "DONE",
          ok: true,
          payload: { version_id: "ver_backend_1", published_version_id: "ver_backend_1" },
          config: {},
          result: { steps: [], artifacts: [] },
        });
      }
      if (method === "GET" && url.endsWith("/api/v1/jobs/job_1/timeline")) {
        return jsonResponse(200, {
          schema_version: "lifecycle_run_timeline.v1",
          ok: true,
          owner: "base-java",
          source_of_truth: "base-java.jobs",
          run_id: "job_1",
          status: "DONE",
          timeline: [{ node_id: "cleaning", type: "cleaning", status: "DONE", started_at: "2026-03-28T00:00:00Z", ended_at: "2026-03-28T00:00:01Z", seconds: 1 }],
        });
      }
      if (method === "GET" && url.includes("/api/v1/jobs/failure-summary?")) {
        return jsonResponse(200, {
          schema_version: "lifecycle_failure_summary.v1",
          ok: true,
          owner: "base-java",
          source_of_truth: "base-java.jobs",
          total_runs: 1,
          failed_runs: 0,
          by_node: {},
        });
      }
      if (method === "GET" && url.includes("/api/v1/jobs/audit-events?")) {
        return jsonResponse(200, [{
          schema_version: "lifecycle_audit_event.v1",
          owner: "base-java",
          source_of_truth: "base-java.jobs",
          ts: "2026-03-28T00:00:02Z",
          actor: "glue",
          action: "STEP_DONE",
          job_id: "job_1",
          step_id: "cleaning",
          detail: { ok: true },
        }]);
      }
      return jsonResponse(500, { ok: false, error: `unexpected request: ${method} ${url}` });
    },
  });

  assert.equal(store.resolveProvider({ mode: "base_api", workflowRunAuditProvider: "base_http" }), BASE_PROVIDER);
  const runs = await store.listRuns(20, { mode: "base_api", workflowRunAuditProvider: "base_http" });
  assert.equal(runs.provider, BASE_PROVIDER);
  assert.equal(runs.items[0].owner, "base-java");
  assert.equal(runs.items[0].run_request_kind, "reference");
  assert.equal(runs.items[0].version_id, "ver_backend_1");
  assert.equal(runs.items[0].published_version_id, "ver_backend_1");

  const hit = await store.getRun("job_1", { mode: "base_api", workflowRunAuditProvider: "base_http" });
  assert.equal(hit.provider, BASE_PROVIDER);
  assert.equal(hit.run_id, "job_1");
  assert.equal(hit.workflow_definition_source, "version_reference");

  const timeline = await store.getRunTimeline("job_1", { mode: "base_api", workflowRunAuditProvider: "base_http" });
  assert.equal(timeline.provider, BASE_PROVIDER);
  assert.equal(timeline.timeline.length, 1);

  const failure = await store.getFailureSummary(20, { mode: "base_api", workflowRunAuditProvider: "base_http" });
  assert.equal(failure.provider, BASE_PROVIDER);
  assert.equal(failure.failed_runs, 0);

  const audit = await store.listAuditLogs(20, "STEP_DONE", { mode: "base_api", workflowRunAuditProvider: "base_http" });
  assert.equal(audit.provider, BASE_PROVIDER);
  assert.equal(audit.items.length, 1);
  assert.equal(audit.items[0].owner, "base-java");
});
