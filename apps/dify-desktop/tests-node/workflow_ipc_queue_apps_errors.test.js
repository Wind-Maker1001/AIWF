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

test("workflow queue apps ipc restore version returns canonical workflow_definition metadata only", async () => {
  const handlers = createQueueAppsHarness({
    workflowValidationSupport: {
      validateWorkflowDefinitionAuthoritatively: async ({ workflowDefinition }) => ({
        ok: true,
        normalized_workflow_definition: workflowDefinition,
        notes: [{ level: "info", message: "no-op" }],
      }),
    },
    workflowVersionStore: {
      recordVersion: async () => ({ ok: true, item: { version_id: "ver_1" } }),
      listVersions: async () => ({ ok: true, items: [] }),
      getVersion: async () => ({
        schema_version: "workflow_version_entry.v1",
        provider: "glue_http",
        owner: "glue-python",
        source_of_truth: "glue-python.governance.workflow_versions",
        version_id: "ver_ok",
        ts: "2026-03-25T00:00:00.000Z",
        workflow_name: "Finance Flow",
        workflow_id: "wf_finance",
        workflow_definition: {
          workflow_id: "wf_finance",
          version: "workflow.v1",
          nodes: [{ id: "n1", type: "ingest_files" }],
          edges: [],
        },
      }),
      compareVersions: async () => ({ ok: true }),
    },
  });

  const restoreWorkflowVersion = handlers["aiwf:restoreWorkflowVersion"];
  const out = await restoreWorkflowVersion({}, { version_id: "ver_ok" });

  assert.equal(out.ok, true);
  assert.equal(out.workflow_definition.workflow_id, "wf_finance");
  assert.equal(out.meta.version_id, "ver_ok");
  assert.equal(out.meta.workflow_definition.workflow_id, "wf_finance");
  assert.equal(out.meta.graph, undefined);
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
    workflow_definition: {
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
  assert.equal(out.item.app_id, "finance_app");
  assert.equal(out.item.published_version_id, "ver_finance_001");
  assert.equal(out.item.graph, undefined);
  assert.equal(out.item.workflow_definition, undefined);
  assert.equal(out.published_version.version_id, "ver_finance_001");
  assert.equal(out.published_version.workflow_definition.workflow_id, "wf_finance");
  assert.equal(out.published_version.graph, undefined);
  assert.equal(calls.length, 2);
  assert.equal(calls[0].kind, "recordVersion");
  assert.equal(calls[0].item.workflow_definition.workflow_id, "wf_finance");
  assert.equal(calls[0].item.graph, undefined);
  assert.equal(calls[1].kind, "publishApp");
  assert.equal(calls[1].item.published_version_id, "ver_finance_001");
  assert.equal(calls[1].item.graph, undefined);
});

test("workflow queue apps ipc runs published app through canonical workflow_definition payload", async () => {
  const runPayloads = [];
  const handlers = createQueueAppsHarness({
    workflowAppRegistryStore: {
      getApp: async () => ({
        app_id: "finance_app",
        name: "Finance App",
        provider: "glue_http",
        workflow_id: "wf_finance",
        published_version_id: "ver_finance_001",
      }),
      publishApp: async () => ({ ok: true }),
      listApps: async () => ({ ok: true, items: [] }),
    },
    workflowVersionStore: {
      recordVersion: async () => ({ ok: true, item: { version_id: "ver_1" } }),
      listVersions: async () => ({ ok: true, items: [] }),
      getVersion: async () => ({
        workflow_definition: {
          workflow_id: "wf_finance",
          version: "workflow.v1",
          nodes: [{ id: "n1", type: "ingest_files", config: { region: "{{region}}" } }],
          edges: [],
        },
      }),
      compareVersions: async () => ({ ok: true }),
    },
    workflowExecutionSupport: {
      executeReferenceWorkflowAuthoritatively: async ({ payload }) => {
        runPayloads.push(payload);
        return {
          ok: true,
          run_id: "run_finance_1",
          status: "done",
          workflow_id: payload?.workflow_definition?.workflow_id || "",
        };
      },
    },
  });

  const runWorkflowApp = handlers["aiwf:runWorkflowApp"];
  const out = await runWorkflowApp({}, {
    app_id: "finance_app",
    params: { region: "cn" },
    payload: {
      workflow: {
        workflow_id: "wf_legacy",
        version: "workflow.v0",
        nodes: [{ id: "legacy", type: "unknown_future_node" }],
        edges: [],
      },
    },
  }, {});

  assert.equal(out.ok, true);
  assert.equal(runPayloads.length, 1);
  assert.equal(runPayloads[0].workflow_definition_source, "version_reference");
  assert.equal(runPayloads[0].workflow_definition.workflow_id, "wf_finance");
  assert.equal(runPayloads[0].workflow_definition.nodes[0].config.region, "cn");
  assert.equal(Object.prototype.hasOwnProperty.call(runPayloads[0], "workflow"), false);
});

test("workflow queue apps ipc publish fails closed when authoritative validation is unavailable", async () => {
  const handlers = createQueueAppsHarness({
    workflowValidationSupport: {
      validateWorkflowDefinitionAuthoritatively: async () => {
        throw createWorkflowStoreRemoteError({
          ok: false,
          error: "workflow validation unavailable: connection refused",
          error_code: "workflow_validation_unavailable",
          validation_scope: "publish",
        });
      },
    },
  });

  const publishWorkflowApp = handlers["aiwf:publishWorkflowApp"];
  const out = await publishWorkflowApp({}, {
    app_id: "finance_app",
    name: "Finance App",
    workflow_definition: {
      workflow_id: "wf_finance",
      version: "workflow.v1",
      nodes: [{ id: "n1", type: "ingest_files" }],
      edges: [],
    },
  });

  assert.equal(out.ok, false);
  assert.equal(out.error_code, "workflow_validation_unavailable");
  assert.match(String(out.error || ""), /workflow validation unavailable/i);
});
