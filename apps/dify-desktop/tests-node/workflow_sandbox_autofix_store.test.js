const test = require("node:test");
const assert = require("node:assert/strict");

const {
  GLUE_PROVIDER,
  WORKFLOW_SANDBOX_AUTOFIX_ACTION_SCHEMA_VERSION,
  WORKFLOW_SANDBOX_AUTOFIX_STATE_SCHEMA_VERSION,
  createWorkflowSandboxAutoFixStore,
} = require("../workflow_sandbox_autofix_store");
const {
  jsonResponse,
  governanceBoundaryResponse,
} = require("./governance_test_support");

test("workflow sandbox autofix store mirrors glue state and actions", async () => {
  const remoteState = {
    violation_events: [{ run_id: "run_1" }],
    forced_isolation_mode: "process",
    forced_until: "2026-03-22T01:00:00Z",
    last_actions: [{ ts: "2026-03-22T00:10:00Z", actions: ["pause_queue"] }],
    green_streak: 1,
  };
  const sandboxSupport = {
    applySandboxAutoFixPayload: (payload, state) => ({
      ...(payload || {}),
      chiplet_isolation_mode: String(state?.forced_isolation_mode || ""),
      green_streak: Number(state?.green_streak || 0),
    }),
    maybeApplySandboxAutoFix: async (_run, _payload, options = null) => {
      const state = options?.state || {};
      const next = {
        ...state,
        green_streak: Number(state.green_streak || 0) + 1,
      };
      if (typeof options?.persistState === "function") {
        await options.persistState(next);
      }
      return { triggered: false, green_streak: next.green_streak };
    },
  };
  const store = createWorkflowSandboxAutoFixStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    sandboxSupport,
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse(
          "workflow_sandbox_autofix",
          "/governance/workflow-sandbox/autofix-state",
          ["/governance/workflow-sandbox/autofix-state", "/governance/workflow-sandbox/autofix-actions"],
        );
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.endsWith("/governance/workflow-sandbox/autofix-state")) {
        return jsonResponse(200, { ok: true, state: remoteState });
      }
      if (method === "GET" && url.includes("/governance/workflow-sandbox/autofix-actions?")) {
        return jsonResponse(200, {
          ok: true,
          forced_isolation_mode: remoteState.forced_isolation_mode,
          forced_until: remoteState.forced_until,
          items: remoteState.last_actions,
        });
      }
      if (method === "PUT" && url.endsWith("/governance/workflow-sandbox/autofix-state")) {
        const body = JSON.parse(String(init.body || "{}"));
        remoteState.violation_events = body.violation_events;
        remoteState.forced_isolation_mode = body.forced_isolation_mode;
        remoteState.forced_until = body.forced_until;
        remoteState.last_actions = body.last_actions;
        remoteState.green_streak = body.green_streak;
        return jsonResponse(200, { ok: true, state: remoteState });
      }
      return jsonResponse(500, { ok: false, error: `unexpected request: ${method} ${url}` });
    },
  });

  const actions = await store.listActions(20, { mode: "base_api" });
  assert.equal(actions.provider, GLUE_PROVIDER);
  assert.equal(actions.items.length, 1);
  assert.equal(actions.items[0].schema_version, WORKFLOW_SANDBOX_AUTOFIX_ACTION_SCHEMA_VERSION);

  const applied = await store.applyPayload({ workflow_id: "wf_remote" }, { mode: "base_api" });
  assert.equal(applied.chiplet_isolation_mode, "process");
  assert.equal(applied.green_streak, 1);

  const processed = await store.processRunAutoFix({ run_id: "run_2" }, { sandbox_autofix_enabled: true }, { mode: "base_api" });
  assert.equal(processed.green_streak, 2);
  assert.equal(remoteState.green_streak, 2);

  const mirrored = await store.persistStateMirror({
    violation_events: [],
    forced_isolation_mode: "",
    forced_until: "",
    last_actions: [],
    green_streak: 0,
  }, { mode: "base_api" });
  assert.equal(mirrored.provider, GLUE_PROVIDER);
  assert.equal(mirrored.state.schema_version, WORKFLOW_SANDBOX_AUTOFIX_STATE_SCHEMA_VERSION);
  assert.equal(remoteState.green_streak, 0);

  const state = await store.getState({ mode: "base_api" });
  assert.equal(state.state.schema_version, WORKFLOW_SANDBOX_AUTOFIX_STATE_SCHEMA_VERSION);
});

test("workflow sandbox autofix store rejects retired local legacy provider", async () => {
  const store = createWorkflowSandboxAutoFixStore({
    loadConfig: () => ({ mode: "offline_local" }),
    sandboxSupport: {
      applySandboxAutoFixPayload: (payload) => payload,
      maybeApplySandboxAutoFix: async () => ({ ok: true }),
    },
  });

  await assert.rejects(
    () => store.getState({
      mode: "offline_local",
      workflowSandboxAutoFixProvider: "local_legacy",
    }),
    /retired/i
  );
});
