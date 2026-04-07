const test = require("node:test");
const assert = require("node:assert/strict");

const { registerWorkflowHistoryIpc } = require("../workflow_ipc_history");
const { registerWorkflowReportIpc } = require("../workflow_ipc_reports");
const { registerWorkflowRunIpc } = require("../workflow_ipc_run");
const { registerWorkflowReviewIpc } = require("../workflow_ipc_review");
const { createWorkflowStoreRemoteError } = require("../workflow_store_remote_error");

test("workflow history ipc preserves structured getRun failure details", async () => {
  const handlers = {};
  registerWorkflowHistoryIpc(
    { ipcMain: { handle: (name, fn) => { handlers[name] = fn; } } },
    {
      readDiagnostics: () => ({ ok: true, items: [] }),
      buildPerfDashboard: async () => ({ ok: true, items: [] }),
      listRunHistory: async () => ({ ok: true, items: [] }),
      getRun: async () => {
        throw createWorkflowStoreRemoteError({
          ok: false,
          error: "workflow graph invalid: workflow contains unregistered node types: unknown_future_node",
          error_code: "workflow_graph_invalid",
          error_items: [{ path: "workflow.nodes", code: "unknown_node_type", message: "workflow contains unregistered node types: unknown_future_node" }],
        });
      },
      runTimeline: async () => ({ ok: true, timeline: [] }),
      failureSummary: async () => ({ ok: true, by_node: {} }),
    },
  );

  const out = await handlers["aiwf:getWorkflowLineage"]({}, { run_id: "run_bad" });
  assert.equal(out.ok, false);
  assert.equal(out.error_code, "workflow_graph_invalid");
  assert.ok(out.error_items.some((item) => item.path === "workflow.nodes"));
});

test("workflow reports ipc preserves structured compare failure details", async () => {
  const handlers = {};
  registerWorkflowReportIpc(
    { ipcMain: { handle: (name, fn) => { handlers[name] = fn; } }, dialog: {}, app: {}, fs: {}, path: {} },
    {
      isMockIoAllowed: () => true,
      resolveMockFilePath: () => ({ ok: true, path: "D:/tmp/out.json" }),
      nowIso: () => "2026-03-25T00:00:00.000Z",
      appendAudit: () => {},
      getRun: async () => null,
      listRunBaselines: async () => ({ ok: true, items: [] }),
      saveRunBaseline: async () => ({ ok: true }),
      buildRunCompare: async () => {
        throw createWorkflowStoreRemoteError({
          ok: false,
          error: "workflow graph invalid: workflow contains unregistered node types: unknown_future_node",
          error_code: "workflow_graph_invalid",
          error_items: [{ path: "workflow.nodes", code: "unknown_node_type", message: "workflow contains unregistered node types: unknown_future_node" }],
        });
      },
      buildRunRegressionAgainstBaseline: async () => ({ ok: true }),
      buildPreflightReportEnvelope: () => ({}),
      renderCompareHtml: () => "",
      renderCompareMarkdown: () => "",
      renderPreflightMarkdown: () => "",
      renderTemplateAcceptanceMarkdown: () => "",
    },
  );

  const out = await handlers["aiwf:compareWorkflowRuns"]({}, { run_a: "run_a", run_b: "run_b" });
  assert.equal(out.ok, false);
  assert.equal(out.error_code, "workflow_graph_invalid");
  assert.ok(out.error_items.some((item) => item.path === "workflow.nodes"));
});

test("workflow reports ipc preserves structured export compare failure details", async () => {
  const handlers = {};
  registerWorkflowReportIpc(
    {
      ipcMain: { handle: (name, fn) => { handlers[name] = fn; } },
      dialog: {},
      app: { getPath: () => "D:/tmp" },
      fs: require("node:fs"),
      path: require("node:path"),
    },
    {
      isMockIoAllowed: () => true,
      resolveMockFilePath: (filePath) => ({ ok: true, path: filePath }),
      nowIso: () => "2026-03-25T00:00:00.000Z",
      appendAudit: () => {},
      getRun: async () => null,
      listRunBaselines: async () => ({ ok: true, items: [] }),
      saveRunBaseline: async () => ({ ok: true }),
      buildRunCompare: async () => {
        throw createWorkflowStoreRemoteError({
          ok: false,
          error: "workflow graph invalid: workflow contains unregistered node types: unknown_future_node",
          error_code: "workflow_graph_invalid",
          error_items: [{ path: "workflow.nodes", code: "unknown_node_type", message: "workflow contains unregistered node types: unknown_future_node" }],
        });
      },
      buildRunRegressionAgainstBaseline: async () => ({ ok: true }),
      buildPreflightReportEnvelope: () => ({}),
      renderCompareHtml: () => "",
      renderCompareMarkdown: () => "",
      renderPreflightMarkdown: () => "",
      renderTemplateAcceptanceMarkdown: () => "",
    },
  );

  const out = await handlers["aiwf:exportCompareReport"]({}, { run_a: "run_a", run_b: "run_b", mock: true, path: "D:/tmp/report.md", format: "md" });
  assert.equal(out.ok, false);
  assert.equal(out.error_code, "workflow_graph_invalid");
  assert.ok(out.error_items.some((item) => item.path === "workflow.nodes"));
});

test("workflow run ipc preserves structured replay failure details", async () => {
  const handlers = {};
  registerWorkflowRunIpc(
    { ipcMain: { handle: (name, fn) => { handlers[name] = fn; } }, createWorkflowWindow: () => {}, loadConfig: () => ({}), runMinimalWorkflow: async () => ({ ok: true }) },
    {
      normalizeWorkflowConfig: (cfg) => cfg,
      resolveOutputRoot: () => "D:/tmp",
      createNodeCacheApi: () => ({}),
      appendDiagnostics: () => {},
      appendRunHistory: () => {},
      extractSandboxViolations: () => [],
      appendAudit: () => {},
      getRun: async () => {
        throw createWorkflowStoreRemoteError({
          ok: false,
          error: "workflow graph invalid: workflow contains unregistered node types: unknown_future_node",
          error_code: "workflow_graph_invalid",
          error_items: [{ path: "workflow.nodes", code: "unknown_node_type", message: "workflow contains unregistered node types: unknown_future_node" }],
        });
      },
      enqueueReviews: async () => ({ ok: true }),
      reportSupport: { applyQualityRuleSetToPayload: async (payload) => payload },
      sandboxSupport: { attachQualityGate: (value) => value, appendSandboxViolationAudit: () => {} },
      sandboxRuleStore: { getRuntimeRules: async () => ({ ok: true, rules: {} }) },
      sandboxAutoFixStore: { applyPayload: async (payload) => payload, processRunAutoFix: async () => ({ ok: true }) },
    },
  );

  const out = await handlers["aiwf:replayWorkflowRun"]({}, { run_id: "run_bad", node_id: "n1" }, {});
  assert.equal(out.ok, false);
  assert.equal(out.error_code, "workflow_graph_invalid");
  assert.ok(out.error_items.some((item) => item.path === "workflow.nodes"));
});

test("workflow run ipc preserves structured runWorkflow failure details", async () => {
  const handlers = {};
  registerWorkflowRunIpc(
    { ipcMain: { handle: (name, fn) => { handlers[name] = fn; } }, createWorkflowWindow: () => {}, loadConfig: () => ({}), runMinimalWorkflow: async () => ({ ok: true }) },
    {
      normalizeWorkflowConfig: (cfg) => cfg,
      resolveOutputRoot: () => "D:/tmp",
      createNodeCacheApi: () => ({}),
      appendDiagnostics: () => {},
      appendRunHistory: () => {},
      extractSandboxViolations: () => [],
      appendAudit: () => {},
      getRun: async () => null,
      enqueueReviews: async () => ({ ok: true }),
      reportSupport: {
        applyQualityRuleSetToPayload: async () => {
          throw createWorkflowStoreRemoteError({
            ok: false,
            error: "workflow graph invalid: workflow contains unregistered node types: unknown_future_node",
            error_code: "workflow_graph_invalid",
            error_items: [{ path: "workflow.nodes", code: "unknown_node_type", message: "workflow contains unregistered node types: unknown_future_node" }],
          });
        },
      },
      sandboxSupport: { attachQualityGate: (value) => value, appendSandboxViolationAudit: () => {} },
      sandboxRuleStore: { getRuntimeRules: async () => ({ ok: true, rules: {} }) },
      sandboxAutoFixStore: { applyPayload: async (payload) => payload, processRunAutoFix: async () => ({ ok: true }) },
    },
  );

  const out = await handlers["aiwf:runWorkflow"]({}, { workflow: { workflow_id: "wf_bad" } }, {});
  assert.equal(out.ok, false);
  assert.equal(out.error_code, "workflow_graph_invalid");
  assert.ok(out.error_items.some((item) => item.path === "workflow.nodes"));
});

test("workflow queue apps ipc preserves structured enqueue failure details", async () => {
  const handlers = {};
  const { registerWorkflowQueueAppsIpc } = require("../workflow_ipc_queue_apps");
  registerWorkflowQueueAppsIpc(
    { ipcMain: { handle: (name, fn) => { handlers[name] = fn; } }, loadConfig: () => ({}), runMinimalWorkflow: async () => ({ ok: true }) },
    {
      queueState: { running: new Map(), draining: false, control: { paused: false, quotas: {} } },
      defaultQueueControl: () => ({ paused: false, quotas: {} }),
      loadWorkflowQueue: () => [],
      saveWorkflowQueue: () => {},
      loadQueueControl: () => ({ paused: false, quotas: {} }),
      saveQueueControl: () => {},
      normalizeQueueControl: (value) => value,
      normalizeWorkflowConfig: (cfg) => cfg,
      resolveOutputRoot: () => "D:/tmp",
      createNodeCacheApi: () => ({}),
      appendDiagnostics: () => {},
      appendRunHistory: () => {},
      extractSandboxViolations: () => [],
      appendAudit: () => {},
      enqueueReviews: async () => ({ ok: true }),
      cacheStats: () => ({}),
      clearNodeCache: () => {},
      nowIso: () => "2026-03-25T00:00:00.000Z",
      reportSupport: {
        applyQualityRuleSetToPayload: async () => {
          throw createWorkflowStoreRemoteError({
            ok: false,
            error: "quality rule set not found: finance_missing",
            error_code: "governance_validation_invalid",
            error_scope: "quality_rule_set",
            error_items: [{ path: "set.id", code: "validation_error", message: "quality rule set not found: finance_missing" }],
          });
        },
      },
      sandboxSupport: { attachQualityGate: (value) => value, appendSandboxViolationAudit: () => {} },
      sandboxRuleStore: { getRuntimeRules: async () => ({ ok: true, rules: {} }) },
      sandboxAutoFixStore: { applyPayload: async (payload) => payload, processRunAutoFix: async () => ({ ok: true }) },
      workflowAppRegistryStore: { getApp: async () => null, publishApp: async () => ({ ok: true }), listApps: async () => ({ ok: true, items: [] }) },
      workflowVersionStore: { recordVersion: async () => ({ ok: true, item: { version_id: "ver_1" } }), listVersions: async () => ({ ok: true, items: [] }), getVersion: async () => null, compareVersions: async () => ({ ok: true }) },
    },
  );

  const out = await handlers["aiwf:enqueueWorkflowTask"]({}, {
    task_id: "task_1",
    label: "workflow_task",
    payload: { workflow: { workflow_id: "wf_bad", version: "1.0.0", nodes: [], edges: [] } },
    cfg: {},
    priority: 100,
  });
  assert.equal(out.ok, false);
  assert.equal(out.error_code, "governance_validation_invalid");
  assert.equal(out.error_scope, "quality_rule_set");
  assert.ok(out.error_items.some((item) => item.path === "set.id"));
});

test("workflow run ipc preserves run status and surfaces review enqueue failure separately", async () => {
  const handlers = {};
  registerWorkflowRunIpc(
    { ipcMain: { handle: (name, fn) => { handlers[name] = fn; } }, createWorkflowWindow: () => {}, loadConfig: () => ({}), runMinimalWorkflow: async () => ({ ok: true }) },
    {
      normalizeWorkflowConfig: (cfg) => cfg,
      resolveOutputRoot: () => "D:/tmp",
      createNodeCacheApi: () => ({}),
      appendDiagnostics: () => {},
      appendRunHistory: () => {},
      extractSandboxViolations: () => [],
      appendAudit: () => {},
      getRun: async () => null,
      enqueueReviews: async () => ({
        ok: false,
        error: "manual review queue unavailable",
        error_code: "manual_review_store_unavailable",
        error_items: [{ path: "review_queue", code: "unavailable", message: "manual review queue unavailable" }],
      }),
      reportSupport: { applyQualityRuleSetToPayload: async (payload) => payload },
      sandboxSupport: { attachQualityGate: (value) => value, appendSandboxViolationAudit: () => {} },
      sandboxRuleStore: { getRuntimeRules: async () => ({ ok: true, rules: {} }) },
      sandboxAutoFixStore: { applyPayload: async (payload) => payload, processRunAutoFix: async () => ({ ok: true }) },
      workflowExecutionSupport: {
        executeDraftWorkflowAuthoritatively: async () => ({
          ok: false,
          run_id: "run_review",
          status: "pending_review",
          pending_reviews: [{ review_key: "gate_a", status: "pending" }],
          node_runs: [],
          node_outputs: {},
          artifacts: [],
        }),
      },
    },
  );

  const out = await handlers["aiwf:runWorkflow"]({}, { workflow: { workflow_id: "wf_review" } }, {});
  assert.equal(out.ok, false);
  assert.equal(out.status, "pending_review");
  assert.equal(out.review_enqueue_failed, true);
  assert.equal(out.review_enqueue.error_code, "manual_review_store_unavailable");
  assert.ok(out.review_enqueue.error_items.some((item) => item.path === "review_queue"));
});

test("workflow review ipc preserves structured auto-resume failure details", async () => {
  const handlers = {};
  registerWorkflowReviewIpc(
    { ipcMain: { handle: (name, fn) => { handlers[name] = fn; } }, dialog: {}, app: {}, fs: {}, path: {}, loadConfig: () => ({}), runMinimalWorkflow: async () => ({ ok: true }) },
    {
      isMockIoAllowed: () => true,
      resolveMockFilePath: () => ({ ok: true, path: "D:/tmp/out.json" }),
      getRun: async () => {
        throw createWorkflowStoreRemoteError({
          ok: false,
          error: "workflow graph invalid: workflow contains unregistered node types: unknown_future_node",
          error_code: "workflow_graph_invalid",
          error_items: [{ path: "workflow.nodes", code: "unknown_node_type", message: "workflow contains unregistered node types: unknown_future_node" }],
        });
      },
      normalizeWorkflowConfig: (cfg) => cfg,
      applyQualityRuleSetToPayload: async (payload) => payload,
      applySandboxAutoFixPayload: async (payload) => payload,
      attachQualityGate: (value) => value,
      resolveOutputRoot: () => "D:/tmp",
      createNodeCacheApi: () => ({}),
      appendDiagnostics: () => {},
      appendRunHistory: () => {},
      extractSandboxViolations: () => [],
      appendSandboxViolationAudit: () => {},
      maybeApplySandboxAutoFix: async () => ({ ok: true }),
      enqueueReviews: async () => ({ ok: true }),
      sandboxRuleStore: { getRuntimeRules: async () => ({ ok: true, rules: {} }) },
      sandboxAutoFixStore: { applyPayload: async (payload) => payload, processRunAutoFix: async () => ({ ok: true }) },
      workflowManualReviewStore: {
        listQueue: async () => ({ ok: true, items: [] }),
        listHistory: async () => ({ ok: true, items: [] }),
        submit: async () => ({ ok: true, item: { approved: true, reviewer: "reviewer", comment: "", node_id: "n1" }, remaining: 0 }),
      },
    },
  );

  const out = await handlers["aiwf:submitManualReview"]({}, { run_id: "run_bad", review_key: "gate_1", approved: true, auto_resume: true }, {});
  assert.equal(out.ok, false);
  assert.equal(out.error_code, "workflow_graph_invalid");
  assert.ok(out.error_items.some((item) => item.path === "workflow.nodes"));
});
