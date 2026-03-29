const test = require("node:test");
const assert = require("node:assert/strict");

const {
  GLUE_PROVIDER,
  createWorkflowAppRegistryStore,
} = require("../workflow_app_registry_store");
const {
  jsonResponse,
  governanceBoundaryResponse,
} = require("./governance_test_support");

function makeGraph(name = "Finance App") {
  return {
    workflow_id: "wf_finance",
    version: "workflow.v1",
    name,
    nodes: [],
    edges: [],
  };
}

test("workflow app registry store honors glue env override in offline_local mode", () => {
  const store = createWorkflowAppRegistryStore({
    loadConfig: () => ({ mode: "offline_local" }),
    validateWorkflowGraph: () => {},
    env: {
      AIWF_WORKFLOW_APP_REGISTRY_PROVIDER: "glue_http",
    },
  });

  assert.equal(store.resolveProvider({ mode: "offline_local" }), GLUE_PROVIDER);
});

test("workflow app registry store uses glue provider for backend-owned registry", async () => {
  const remoteItems = new Map();
  const store = createWorkflowAppRegistryStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    nowIso: () => "2026-03-21T00:00:00Z",
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("workflow_apps", "/governance/workflow-apps");
      }
      const method = String(init.method || "GET").toUpperCase();
      const appId = decodeURIComponent(String(url).split("/").pop() || "");
      if (method === "GET" && url.includes("/governance/workflow-apps?limit=")) {
        return jsonResponse(200, { ok: true, items: Array.from(remoteItems.values()) });
      }
      if (method === "GET") {
        if (!remoteItems.has(appId)) return jsonResponse(404, { ok: false, error: `workflow app not found: ${appId}` });
        return jsonResponse(200, { ok: true, item: remoteItems.get(appId) });
      }
      if (method === "PUT") {
        const body = JSON.parse(String(init.body || "{}"));
        const item = {
          schema_version: "workflow_app_registry_entry.v1",
          owner: "glue-python",
          source_of_truth: "glue-python.governance.workflow_apps",
          app_id: appId,
          name: body.app.name,
          workflow_id: body.app.workflow_id,
          published_version_id: body.app.published_version_id,
          params_schema: body.app.params_schema,
          template_policy: body.app.template_policy,
          created_at: "2026-03-21T00:00:00Z",
          updated_at: "2026-03-21T00:00:00Z",
        };
        remoteItems.set(appId, item);
        return jsonResponse(200, { ok: true, item });
      }
      return jsonResponse(500, { ok: false, error: "unexpected request" });
    },
  });

  const published = await store.publishApp({
    app_id: "finance_app_remote",
    name: "Finance Remote",
    workflow_id: "wf_finance",
    published_version_id: "wf_finance_published_001",
    params_schema: { region: { type: "string" } },
    template_policy: { version: 1, governance: { mode: "strict" } },
  }, { mode: "base_api" });
  assert.equal(published.ok, true);
  assert.equal(published.provider, GLUE_PROVIDER);
  assert.equal(remoteItems.get("finance_app_remote").template_policy.version, 1);
  assert.equal(remoteItems.get("finance_app_remote").published_version_id, "wf_finance_published_001");
  assert.equal(remoteItems.get("finance_app_remote").graph, undefined);
  assert.equal(remoteItems.get("finance_app_remote").workflow_definition, undefined);

  const listed = await store.listApps(100, { mode: "base_api" });
  assert.equal(listed.provider, GLUE_PROVIDER);
  assert.deepEqual(listed.items.map((item) => item.app_id), ["finance_app_remote"]);
  assert.equal(listed.items[0].graph, undefined);
  assert.equal(listed.items[0].workflow_definition, undefined);

  const fetched = await store.getApp("finance_app_remote", { mode: "base_api" });
  assert.equal(fetched.provider, GLUE_PROVIDER);
  assert.equal(fetched.owner, "glue-python");
  assert.equal(fetched.graph, undefined);
  assert.equal(fetched.workflow_definition, undefined);
});

test("workflow app registry store rejects retired local legacy provider", async () => {
  const store = createWorkflowAppRegistryStore({
    loadConfig: () => ({ mode: "offline_local" }),
  });

  const published = await store.publishApp({
    app_id: "finance_app_local",
    name: "Finance Local",
    workflow_id: "wf_finance",
    published_version_id: "wf_finance_published_002",
    params_schema: { region: { type: "string" } },
    template_policy: { version: 1 },
  }, { mode: "offline_local", workflowAppRegistryProvider: "local_legacy" });

  assert.equal(published.ok, false);
  assert.match(String(published.error || ""), /retired/i);
});

test("workflow app registry store preserves structured remote publish failure details", async () => {
  const store = createWorkflowAppRegistryStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async (url) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("workflow_apps", "/governance/workflow-apps");
      }
      return jsonResponse(400, {
        ok: false,
        error: "workflow app published_version_id not found: ver_missing",
        error_code: "governance_validation_invalid",
        error_scope: "workflow_app",
        error_item_contract: "contracts/desktop/node_config_validation_errors.v1.json",
        error_items: [{
          path: "published_version_id",
          code: "validation_error",
          message: "workflow app published_version_id not found: ver_missing",
        }],
      });
    },
  });

  const published = await store.publishApp({
    app_id: "finance_app_remote",
    name: "Finance Remote",
    workflow_id: "wf_finance",
    published_version_id: "ver_missing",
    params_schema: {},
    template_policy: {},
  }, { mode: "base_api" });

  assert.equal(published.ok, false);
  assert.equal(published.error_code, "governance_validation_invalid");
  assert.equal(published.error_scope, "workflow_app");
  assert.ok(Array.isArray(published.error_items));
  assert.ok(published.error_items.some((item) => item.path === "published_version_id" && item.code === "validation_error"));
});
