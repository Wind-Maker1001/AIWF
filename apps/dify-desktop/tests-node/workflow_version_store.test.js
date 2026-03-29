const test = require("node:test");
const assert = require("node:assert/strict");

const {
  GLUE_PROVIDER,
  compareVersionItems,
  createWorkflowVersionStore,
} = require("../workflow_version_store");
const {
  jsonResponse,
  governanceBoundaryResponse,
} = require("./governance_test_support");

function makeVersion(versionId, workflowDefinition) {
  return {
    version_id: versionId,
    ts: "2026-03-21T00:00:00Z",
    workflow_name: "Finance Flow",
    workflow_id: "wf_finance",
    path: `D:/flows/${versionId}.json`,
    workflow_definition: workflowDefinition,
  };
}

function graphA() {
  return {
    workflow_id: "wf_finance",
    version: "workflow.v1",
    nodes: [{ id: "n1", type: "ingest_files", config: {} }],
    edges: [],
  };
}

function graphB() {
  return {
    workflow_id: "wf_finance",
    version: "workflow.v1",
    nodes: [
      { id: "n1", type: "ingest_files", config: {} },
      { id: "n2", type: "quality_check_v3", config: { strict: true } },
    ],
    edges: [{ from: "n1", to: "n2" }],
  };
}

test("workflow version store honors glue env override in offline_local mode", () => {
  const store = createWorkflowVersionStore({
    loadConfig: () => ({ mode: "offline_local" }),
    env: {
      AIWF_WORKFLOW_VERSION_PROVIDER: "glue_http",
    },
  });

  assert.equal(store.resolveProvider({ mode: "offline_local" }), GLUE_PROVIDER);
});

test("workflow version store rejects retired local legacy provider", async () => {
  const store = createWorkflowVersionStore({
    loadConfig: () => ({ mode: "offline_local" }),
  });

  assert.equal(store.resolveProvider({ mode: "offline_local" }), GLUE_PROVIDER);

  const saved = await store.recordVersion(makeVersion("ver_local", graphA()), {
    mode: "offline_local",
    workflowVersionProvider: "local_legacy",
  });
  assert.equal(saved.ok, false);
  assert.match(String(saved.error || ""), /retired/i);
});

test("workflow version store uses glue provider for backend-owned snapshots", async () => {
  const remote = new Map();
  const store = createWorkflowVersionStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("workflow_versions", "/governance/workflow-versions");
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "PUT") {
        const id = decodeURIComponent(String(url).split("/").pop() || "");
        const body = JSON.parse(String(init.body || "{}"));
        assert.equal(typeof body.version.workflow_definition, "object");
        assert.equal(body.version.graph, undefined);
        remote.set(id, {
          schema_version: "workflow_version_snapshot.v1",
          owner: "glue-python",
          source_of_truth: "glue-python.governance.workflow_versions",
          ...body.version,
        });
        return jsonResponse(200, { ok: true, item: remote.get(id) });
      }
      if (method === "GET" && url.includes("/governance/workflow-versions?")) {
        return jsonResponse(200, { ok: true, items: Array.from(remote.values()) });
      }
      if (method === "GET") {
        const id = decodeURIComponent(String(url).split("/").pop() || "");
        if (!remote.has(id)) return jsonResponse(404, { ok: false, error: `version not found: ${id}` });
        return jsonResponse(200, { ok: true, item: remote.get(id) });
      }
      if (method === "POST" && url.endsWith("/governance/workflow-versions/compare")) {
        return jsonResponse(200, {
          ok: true,
          provider: "glue-python",
          summary: { version_a: "ver_a", version_b: "ver_b", changed_nodes: 1, added_edges: 1 },
          node_diff: [{ id: "n2", change: "added", type_a: "", type_b: "quality_check_v3" }],
          added_edges: ["n1->n2:null"],
          removed_edges: [],
        });
      }
      return jsonResponse(500, { ok: false, error: `unexpected request: ${method} ${url}` });
    },
  });

  const saved = await store.recordVersion(makeVersion("ver_a", graphA()), { mode: "base_api" });
  assert.equal(saved.provider, GLUE_PROVIDER);
  assert.equal(saved.item.workflow_definition.workflow_id, "wf_finance");
  assert.equal(saved.item.graph, undefined);
  await store.recordVersion(makeVersion("ver_b", graphB()), { mode: "base_api" });

  const listed = await store.listVersions(100, "", { mode: "base_api" });
  assert.equal(listed.provider, GLUE_PROVIDER);
  assert.equal(listed.items.length, 2);
  assert.equal(listed.items[0].workflow_definition.workflow_id, "wf_finance");
  assert.equal(listed.items[0].graph, undefined);

  const restored = await store.getVersion("ver_b", { mode: "base_api" });
  assert.equal(restored.owner, "glue-python");
  assert.equal(restored.workflow_definition.workflow_id, "wf_finance");
  assert.equal(restored.graph, undefined);

  const compared = await store.compareVersions("ver_a", "ver_b", { mode: "base_api" });
  assert.equal(compared.summary.changed_nodes, 1);
});

test("workflow version store preserves structured remote record failure details", async () => {
  const store = createWorkflowVersionStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async (url) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("workflow_versions", "/governance/workflow-versions");
      }
      return jsonResponse(400, {
        ok: false,
        error: "workflow graph invalid: workflow contains unregistered node types: unknown_future_node",
        error_code: "workflow_graph_invalid",
        error_items: [{
          path: "workflow.nodes",
          code: "unknown_node_type",
          message: "workflow contains unregistered node types: unknown_future_node",
        }],
      });
    },
  });

  const saved = await store.recordVersion(makeVersion("ver_remote_fail", graphA()), { mode: "base_api" });

  assert.equal(saved.ok, false);
  assert.equal(saved.error_code, "workflow_graph_invalid");
  assert.ok(Array.isArray(saved.error_items));
  assert.ok(saved.error_items.some((item) => item.path === "workflow.nodes" && item.code === "unknown_node_type"));
});

test("workflow version compare helper detects nested config changes", () => {
  const compared = compareVersionItems(
    makeVersion("ver_a", {
      workflow_id: "wf_finance",
      version: "workflow.v1",
      nodes: [{
        id: "n1",
        type: "plugin_registry_v1",
        config: {
          op: "register",
          manifest: {
            command: "alpha",
            enabled: true,
          },
        },
      }],
      edges: [],
    }),
    makeVersion("ver_b", {
      workflow_id: "wf_finance",
      version: "workflow.v1",
      nodes: [{
        id: "n1",
        type: "plugin_registry_v1",
        config: {
          op: "register",
          manifest: {
            command: "beta",
            enabled: true,
          },
        },
      }],
      edges: [],
    }),
  );

  assert.equal(compared.summary.changed_nodes, 1);
  assert.equal(compared.node_diff.length, 1);
  assert.equal(compared.node_diff[0].change, "updated");
  assert.equal(compared.node_diff[0].config_changed, true);
});
