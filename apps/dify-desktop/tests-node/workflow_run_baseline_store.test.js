const test = require("node:test");
const assert = require("node:assert/strict");

const {
  GLUE_PROVIDER,
  createWorkflowRunBaselineStore,
} = require("../workflow_run_baseline_store");
const {
  jsonResponse,
  governanceBoundaryResponse,
} = require("./governance_test_support");

test("workflow run baseline store uses glue provider for backend-owned baselines", async () => {
  const remote = [];
  const store = createWorkflowRunBaselineStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("run_baselines", "/governance/run-baselines");
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.includes("/governance/run-baselines?")) {
        return jsonResponse(200, { ok: true, items: remote });
      }
      if (method === "PUT") {
        const body = JSON.parse(String(init.body || "{}")).baseline;
        const item = {
          schema_version: "run_baseline_entry.v1",
          owner: "glue-python",
          source_of_truth: "glue-python.governance.run_baselines",
          ...body,
        };
        remote.unshift(item);
        return jsonResponse(200, { ok: true, item });
      }
      return jsonResponse(500, { ok: false, error: `unexpected request: ${method} ${url}` });
    },
  });

  const saved = await store.save({
    baseline_id: "base_2",
    name: "Base Two",
    run_id: "run_2",
    workflow_id: "wf_finance",
  }, { mode: "base_api" });
  assert.equal(saved.provider, GLUE_PROVIDER);

  const listed = await store.list(100, { mode: "base_api" });
  assert.equal(listed.provider, GLUE_PROVIDER);
  assert.equal(listed.items.length, 1);
  assert.equal(listed.items[0].owner, "glue-python");
});

test("workflow run baseline store honors glue env override in offline_local mode", () => {
  const store = createWorkflowRunBaselineStore({
    loadConfig: () => ({ mode: "offline_local" }),
    env: {
      AIWF_RUN_BASELINE_PROVIDER: "glue_http",
    },
  });

  assert.equal(store.resolveProvider({ mode: "offline_local" }), GLUE_PROVIDER);
});

test("workflow run baseline store rejects retired local legacy provider", async () => {
  const store = createWorkflowRunBaselineStore({
    loadConfig: () => ({ mode: "offline_local" }),
  });

  const listed = await store.list(100, {
    mode: "offline_local",
    runBaselineProvider: "local_legacy",
  });
  assert.equal(listed.ok, false);
  assert.match(String(listed.error || ""), /retired/i);

  const out = await store.save({
    baseline_id: "base_1",
    run_id: "run_1",
  }, {
    mode: "offline_local",
    runBaselineProvider: "local_legacy",
  });

  assert.equal(out.ok, false);
  assert.match(String(out.error || ""), /retired/i);
});
