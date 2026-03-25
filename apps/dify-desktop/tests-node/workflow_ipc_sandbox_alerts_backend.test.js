const test = require("node:test");
const assert = require("node:assert/strict");

const { createWorkflowSandboxSupport } = require("../workflow_ipc_sandbox_alerts");

function createFsStub() {
  return {
    existsSync: () => false,
    statSync: () => ({ size: 0 }),
    readFileSync: () => "",
    appendFileSync: () => {},
  };
}

function createSupport(listRunRecords) {
  return createWorkflowSandboxSupport({
    fs: createFsStub(),
    readJsonFile: (_path, fallback) => fallback,
    writeJsonFile: () => {},
    nowIso: () => "2026-03-22T00:00:00Z",
    appendAudit: () => {},
    extractSandboxViolations: (run) => Array.isArray(run?.violations) ? run.violations : [],
    sandboxAutoFixStatePath: () => "",
    workflowAuditPath: () => "",
    listRunHistory: () => [{
      ts: "2026-03-21T00:00:00Z",
      run_id: "local_only",
      workflow_id: "wf_local",
      result: {
        ok: false,
        status: "quality_blocked",
        quality_gate: { blocked: true, passed: false, issues: ["local_only"] },
      },
    }],
    listRunRecords,
    queueState: { control: { paused: false } },
    defaultQueueControl: () => ({ paused: false, quotas: {} }),
    saveQueueControl: () => {},
    enqueueReviews: () => {},
  });
}

test("workflow sandbox support quality gate reports prefer backend run records", async () => {
  const support = createSupport(async () => ({
    ok: true,
    provider: "glue_http",
    items: [{
      ts: "2026-03-22T00:00:00Z",
      run_id: "run_backend",
      workflow_id: "wf_backend",
      result: {
        ok: false,
        status: "quality_blocked",
        quality_gate: { blocked: true, passed: false, issues: ["backend_only"] },
      },
    }],
  }));

  const reports = await support.listQualityGateReports(20, { status: "blocked" });
  assert.equal(reports.ok, true);
  assert.deepEqual(reports.items.map((item) => item.run_id), ["run_backend"]);
});

test("workflow sandbox support alerts prefer backend run records", async () => {
  const support = createSupport(async () => ({
    ok: true,
    provider: "glue_http",
    items: [{
      ts: "2026-03-22T00:00:00Z",
      run_id: "run_backend",
      workflow_id: "wf_backend",
      result: {
        violations: [{
          run_id: "run_backend",
          workflow_id: "wf_backend",
          node_id: "n1",
          node_type: "rust_compute",
          error: "sandbox_limit_exceeded:output",
        }],
      },
    }],
  }));

  const alerts = await support.sandboxAlerts(20, null, 0);
  assert.equal(alerts.ok, true);
  assert.equal(alerts.total, 1);
  assert.equal(alerts.items[0].run_id, "run_backend");
});
