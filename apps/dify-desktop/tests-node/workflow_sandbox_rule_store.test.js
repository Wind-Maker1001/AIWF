const test = require("node:test");
const assert = require("node:assert/strict");

const {
  GLUE_PROVIDER,
  WORKFLOW_SANDBOX_RULE_COMPARE_SCHEMA_VERSION,
  WORKFLOW_SANDBOX_RULE_VERSION_SCHEMA_VERSION,
  WORKFLOW_SANDBOX_RULES_SCHEMA_VERSION,
  createWorkflowSandboxRuleStore,
} = require("../workflow_sandbox_rule_store");
const {
  jsonResponse,
  governanceBoundaryResponse,
} = require("./governance_test_support");

function makeSandboxSupport() {
  return {
    normalizeSandboxAlertRules(input) {
      const src = input && typeof input === "object" ? input : {};
      const toList = (arr) => Array.from(new Set((Array.isArray(arr) ? arr : [])
        .map((item) => String(item || "").trim().toLowerCase())
        .filter(Boolean)));
      const muteSrc = src.mute_until_by_key && typeof src.mute_until_by_key === "object" ? src.mute_until_by_key : {};
      const mute = {};
      Object.keys(muteSrc).forEach((key) => {
        const normalizedKey = String(key || "").trim().toLowerCase();
        const value = String(muteSrc[key] || "").trim();
        if (normalizedKey && value) mute[normalizedKey] = value;
      });
      return {
        whitelist_codes: toList(src.whitelist_codes),
        whitelist_node_types: toList(src.whitelist_node_types),
        whitelist_keys: toList(src.whitelist_keys),
        mute_until_by_key: mute,
      };
    },
  };
}

test("workflow sandbox rule store uses glue provider as the only sandbox rule source", async () => {
  const sandboxSupport = makeSandboxSupport();
  const remoteState = {
    rules: {
      whitelist_codes: ["sandbox_limit_exceeded:output"],
      whitelist_node_types: ["ai_refine"],
      whitelist_keys: [],
      mute_until_by_key: {},
    },
    versions: [
      {
        version_id: "ver_a",
        ts: "2026-03-21T00:00:00Z",
        rules: {
          whitelist_codes: ["sandbox_limit_exceeded:output"],
          whitelist_node_types: [],
          whitelist_keys: [],
          mute_until_by_key: {},
        },
        meta: { reason: "set_rules" },
      },
      {
        version_id: "ver_b",
        ts: "2026-03-21T00:10:00Z",
        rules: {
          whitelist_codes: ["sandbox_limit_exceeded:output"],
          whitelist_node_types: ["ai_refine"],
          whitelist_keys: [],
          mute_until_by_key: {},
        },
        meta: { reason: "set_rules" },
      },
    ],
  };

  const store = createWorkflowSandboxRuleStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    sandboxSupport,
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse(
          "workflow_sandbox_rules",
          "/governance/workflow-sandbox/rules",
          ["/governance/workflow-sandbox/rules", "/governance/workflow-sandbox/rule-versions"],
        );
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.endsWith("/governance/workflow-sandbox/rules")) {
        return jsonResponse(200, { ok: true, rules: remoteState.rules });
      }
      if (method === "PUT" && url.endsWith("/governance/workflow-sandbox/rules")) {
        const body = JSON.parse(String(init.body || "{}"));
        remoteState.rules = body.rules;
        remoteState.versions = [{
          version_id: `ver_${remoteState.versions.length + 1}`,
          ts: "2026-03-21T01:00:00Z",
          rules: body.rules,
          meta: body.meta || {},
        }, ...remoteState.versions];
        return jsonResponse(200, { ok: true, rules: remoteState.rules, version_id: remoteState.versions[0].version_id });
      }
      if (method === "GET" && url.includes("/governance/workflow-sandbox/rule-versions?limit=")) {
        return jsonResponse(200, { ok: true, items: remoteState.versions });
      }
      if (method === "POST" && url.endsWith("/governance/workflow-sandbox/rule-versions/ver_a/rollback")) {
        remoteState.rules = remoteState.versions.find((item) => item.version_id === "ver_a").rules;
        remoteState.versions = [{
          version_id: "ver_rollback",
          ts: "2026-03-21T02:00:00Z",
          rules: remoteState.rules,
          meta: { reason: "rollback", from_version_id: "ver_a" },
        }, ...remoteState.versions];
        return jsonResponse(200, { ok: true, rules: remoteState.rules, version_id: "ver_rollback" });
      }
      return jsonResponse(500, { ok: false, error: `unexpected request: ${method} ${url}` });
    },
  });

  const fetched = await store.getRules({ mode: "base_api" });
  assert.equal(fetched.provider, GLUE_PROVIDER);
  assert.equal(fetched.schema_version, WORKFLOW_SANDBOX_RULES_SCHEMA_VERSION);
  assert.deepEqual(fetched.rules.whitelist_node_types, ["ai_refine"]);

  const listed = await store.listVersions(100, { mode: "base_api" });
  assert.equal(listed.provider, GLUE_PROVIDER);
  assert.equal(listed.items.length, 2);
  assert.equal(listed.items[0].schema_version, WORKFLOW_SANDBOX_RULE_VERSION_SCHEMA_VERSION);

  const compared = await store.compareVersions("ver_a", "ver_b", { mode: "base_api" });
  assert.equal(compared.provider, GLUE_PROVIDER);
  assert.equal(compared.schema_version, WORKFLOW_SANDBOX_RULE_COMPARE_SCHEMA_VERSION);
  assert.deepEqual(compared.whitelist_node_types.added, ["ai_refine"]);

  const muted = await store.muteAlert({
    node_type: "ai_refine",
    node_id: "*",
    code: "*",
    minutes: 30,
  }, { mode: "base_api" });
  assert.equal(muted.ok, true);
  assert.equal(muted.provider, GLUE_PROVIDER);
  assert.ok(remoteState.rules.mute_until_by_key["ai_refine::*::*"]);

  const rolledBack = await store.rollbackVersion("ver_a", { mode: "base_api" });
  assert.equal(rolledBack.ok, true);
  assert.equal(rolledBack.provider, GLUE_PROVIDER);
  assert.equal(rolledBack.schema_version, WORKFLOW_SANDBOX_RULES_SCHEMA_VERSION);
  assert.deepEqual(rolledBack.rules.whitelist_node_types, []);
});

test("workflow sandbox rule store rejects unsupported provider override", async () => {
  const sandboxSupport = makeSandboxSupport();
  const store = createWorkflowSandboxRuleStore({
    loadConfig: () => ({ mode: "offline_local" }),
    sandboxSupport,
  });

  const out = await store.getRules({
    mode: "offline_local",
    workflowSandboxRuleProvider: "unsupported_provider",
  });
  assert.equal(out.ok, false);
  assert.match(String(out.error || ""), /unsupported|provider/i);
});
