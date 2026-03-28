const test = require("node:test");
const assert = require("node:assert/strict");

const { registerWorkflowQueueAppsIpc } = require("../workflow_ipc_queue_apps");
const { createWorkflowStoreRemoteError } = require("../workflow_store_remote_error");

function createQueueAppsHarness(overrides = {}) {
  const handlers = {};
  registerWorkflowQueueAppsIpc(
    {
      ipcMain: {
        handle(name, fn) {
          handlers[name] = fn;
        },
      },
      loadConfig: () => ({}),
      runMinimalWorkflow: async () => ({ ok: true }),
    },
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
      reportSupport: { applyQualityRuleSetToPayload: async (payload) => payload },
      sandboxSupport: { attachQualityGate: (value) => value, appendSandboxViolationAudit: () => {} },
      sandboxRuleStore: { getRuntimeRules: async () => ({ ok: true, rules: {} }) },
      sandboxAutoFixStore: { applyPayload: async (payload) => payload, processRunAutoFix: async () => ({ ok: true }) },
      workflowAppRegistryStore: {
        getApp: async () => null,
        publishApp: async () => ({ ok: true }),
        listApps: async () => ({ ok: true, items: [] }),
      },
      workflowVersionStore: {
        recordVersion: async () => ({ ok: true, item: { version_id: "ver_1" } }),
        listVersions: async () => ({ ok: true, items: [] }),
        getVersion: async () => null,
        compareVersions: async () => ({ ok: true }),
      },
      ...overrides,
    },
  );
  return handlers;
}

test("workflow queue apps ipc preserves structured restore version failure details", async () => {
  const remoteError = createWorkflowStoreRemoteError({
    ok: false,
    error: "workflow graph invalid: workflow contains unregistered node types: unknown_future_node",
    error_code: "workflow_graph_invalid",
    error_items: [{
      path: "workflow.nodes",
      code: "unknown_node_type",
      message: "workflow contains unregistered node types: unknown_future_node",
    }],
  });
  const handlers = createQueueAppsHarness({
    workflowVersionStore: {
      recordVersion: async () => ({ ok: true, item: { version_id: "ver_1" } }),
      listVersions: async () => ({ ok: true, items: [] }),
      getVersion: async () => { throw remoteError; },
      compareVersions: async () => ({ ok: true }),
    },
  });

  const restoreWorkflowVersion = handlers["aiwf:restoreWorkflowVersion"];
  const out = await restoreWorkflowVersion({}, { version_id: "ver_bad" });

  assert.equal(out.ok, false);
  assert.equal(out.error_code, "workflow_graph_invalid");
  assert.ok(Array.isArray(out.error_items));
  assert.ok(out.error_items.some((item) => item.path === "workflow.nodes" && item.code === "unknown_node_type"));
});

test("workflow queue apps ipc preserves structured run app fetch failure details", async () => {
  const remoteError = createWorkflowStoreRemoteError({
    ok: false,
    error: "workflow app graph node config invalid: workflow.nodes[0].config.manifest.command is required when workflow.nodes[0].config.op is register",
    error_code: "workflow_graph_invalid",
    error_scope: "workflow_app",
    error_items: [{
      path: "workflow.nodes[0].config.manifest.command",
      code: "conditional_required",
      message: "workflow.nodes[0].config.manifest.command is required when workflow.nodes[0].config.op is register",
    }],
  });
  const handlers = createQueueAppsHarness({
    workflowAppRegistryStore: {
      getApp: async () => { throw remoteError; },
      publishApp: async () => ({ ok: true }),
      listApps: async () => ({ ok: true, items: [] }),
    },
  });

  const runWorkflowApp = handlers["aiwf:runWorkflowApp"];
  const out = await runWorkflowApp({}, { app_id: "app_bad" }, {});

  assert.equal(out.ok, false);
  assert.equal(out.error_code, "workflow_graph_invalid");
  assert.equal(out.error_scope, "workflow_app");
  assert.ok(Array.isArray(out.error_items));
  assert.ok(out.error_items.some((item) => item.path === "workflow.nodes[0].config.manifest.command"));
});

test("workflow queue apps ipc publishes app via version snapshot reference", async () => {
  const calls = [];
  const handlers = createQueueAppsHarness({
    workflowAppRegistryStore: {
      getApp: async () => null,
      publishApp: async (item) => {
        calls.push({ kind: "publishApp", item });
        return { ok: true, provider: "glue_http", item: { ...item, app_id: item.app_id || "app_finance" } };
      },
      listApps: async () => ({ ok: true, items: [] }),
    },
    workflowVersionStore: {
      recordVersion: async (item) => {
        calls.push({ kind: "recordVersion", item });
        return { ok: true, item: { ...item, version_id: "ver_finance_001" } };
      },
      listVersions: async () => ({ ok: true, items: [] }),
      getVersion: async () => null,
      compareVersions: async () => ({ ok: true }),
    },
  });

  const publishWorkflowApp = handlers["aiwf:publishWorkflowApp"];
  const out = await publishWorkflowApp({}, {
    app_id: "finance_app",
    name: "Finance App",
    graph: {
      workflow_id: "wf_finance",
      version: "workflow.v1",
      nodes: [{ id: "n1", type: "ingest_files" }],
      edges: [],
    },
    params_schema: { region: { type: "string" } },
    template_policy: { version: 1 },
  });

  assert.equal(out.ok, true);
  assert.equal(out.published_version_id, "ver_finance_001");
  assert.equal(calls.length, 2);
  assert.equal(calls[0].kind, "recordVersion");
  assert.equal(calls[0].item.graph.workflow_id, "wf_finance");
  assert.equal(calls[1].kind, "publishApp");
  assert.equal(calls[1].item.published_version_id, "ver_finance_001");
  assert.equal(calls[1].item.graph, undefined);
});
