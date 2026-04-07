const test = require("node:test");
const assert = require("node:assert/strict");

const {
  GLUE_PROVIDER,
  QUALITY_RULE_SET_SCHEMA_VERSION,
  createWorkflowQualityRuleSetSupport,
} = require("../workflow_quality_rule_store");
const {
  jsonResponse,
  governanceBoundaryResponse,
} = require("./governance_test_support");

test("workflow quality rule store resolves provider from mode and explicit override", () => {
  const support = createWorkflowQualityRuleSetSupport({
    loadConfig: () => ({ mode: "offline_local" }),
  });

  assert.equal(support.resolveProvider(), GLUE_PROVIDER);
  assert.equal(support.resolveProvider({ mode: "base_api" }), GLUE_PROVIDER);
  assert.throws(
    () => support.resolveProvider({ mode: "base_api", qualityRuleSetProvider: "unsupported_provider" }),
    /unsupported|provider/i
  );
});

test("workflow quality rule store honors glue env override in offline_local mode", () => {
  const support = createWorkflowQualityRuleSetSupport({
    loadConfig: () => ({ mode: "offline_local" }),
    env: {
      AIWF_QUALITY_RULE_SET_PROVIDER: "glue_http",
    },
  });

  assert.equal(support.resolveProvider({ mode: "offline_local" }), GLUE_PROVIDER);
});

test("workflow quality rule store uses glue http provider for remote governance", async () => {
  const remoteItems = new Map();
  const calls = [];
  const support = createWorkflowQualityRuleSetSupport({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    nowIso: () => "2026-03-21T00:00:00Z",
    fetchImpl: async (url, init = {}) => {
      calls.push({ url, method: init.method || "GET" });
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("quality_rule_sets", "/governance/quality-rule-sets");
      }
      const method = String(init.method || "GET").toUpperCase();
      const key = decodeURIComponent(String(url).split("/").pop() || "");
      if (method === "GET" && url.endsWith("/governance/quality-rule-sets?limit=5000")) {
        return jsonResponse(200, { ok: true, sets: Array.from(remoteItems.values()) });
      }
      if (method === "GET") {
        if (!remoteItems.has(key)) return jsonResponse(404, { ok: false, error: `quality rule set not found: ${key}` });
        return jsonResponse(200, { ok: true, set: remoteItems.get(key) });
      }
      if (method === "PUT") {
        const body = JSON.parse(String(init.body || "{}"));
        const next = {
          owner: "glue-python",
          source_of_truth: "glue-python.governance.quality_rule_sets",
          id: key,
          name: body.set.name,
          version: body.set.version,
          scope: body.set.scope,
          rules: body.set.rules,
          created_at: "2026-03-21T00:00:00Z",
          updated_at: "2026-03-21T00:00:00Z",
        };
        remoteItems.set(key, next);
        return jsonResponse(200, { ok: true, set: next });
      }
      if (method === "DELETE") {
        remoteItems.delete(key);
        return jsonResponse(200, { ok: true, id: key });
      }
      return jsonResponse(500, { ok: false, error: "unexpected request" });
    },
  });

  const saved = await support.saveQualityRuleSet({
    set: {
      id: "finance_remote",
      name: "Finance Remote",
      version: "v1",
      scope: "workflow",
      rules: { required_columns: ["amount"] },
    },
  }, { mode: "base_api" });
  assert.equal(saved.ok, true);
  assert.equal(saved.provider, GLUE_PROVIDER);
  assert.equal(saved.set.schema_version, QUALITY_RULE_SET_SCHEMA_VERSION);

  const fetched = await support.getQualityRuleSet("finance_remote", { mode: "base_api" });
  assert.equal(fetched.provider, GLUE_PROVIDER);
  assert.equal(fetched.schema_version, QUALITY_RULE_SET_SCHEMA_VERSION);
  assert.equal(fetched.owner, "glue-python");

  const listed = await support.listQualityRuleSets({ mode: "base_api" });
  assert.equal(listed.sets[0].schema_version, QUALITY_RULE_SET_SCHEMA_VERSION);
  assert.deepEqual(listed.sets.map((item) => item.id), ["finance_remote"]);

  const removed = await support.removeQualityRuleSet({ id: "finance_remote" }, { mode: "base_api" });
  assert.equal(removed.ok, true);
  assert.equal(remoteItems.size, 0);
  assert.ok(calls.some((entry) => /governance\/quality-rule-sets\/finance_remote$/.test(entry.url)));
});

test("workflow quality rule store rejects unsupported provider override", async () => {
  const support = createWorkflowQualityRuleSetSupport({
    loadConfig: () => ({ mode: "offline_local" }),
  });

  const saved = await support.saveQualityRuleSet({
    set: {
      id: "finance_local",
      name: "Finance Local",
      version: "v1",
      scope: "workflow",
      rules: { required_columns: ["amount"] },
    },
  }, { mode: "offline_local", qualityRuleSetProvider: "unsupported_provider" });

  assert.equal(saved.ok, false);
  assert.match(String(saved.error || ""), /unsupported|provider/i);
});

test("workflow quality rule store preserves structured remote failure details", async () => {
  const support = createWorkflowQualityRuleSetSupport({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async (url) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("quality_rule_sets", "/governance/quality-rule-sets");
      }
      return jsonResponse(400, {
        ok: false,
        error: "quality rule set id must match ^[a-z0-9][a-z0-9_-]{1,79}$",
        error_code: "governance_validation_invalid",
        error_scope: "quality_rule_set",
        error_item_contract: "contracts/desktop/node_config_validation_errors.v1.json",
        error_items: [{
          path: "set.id",
          code: "validation_error",
          message: "set.id must match ^[a-z0-9][a-z0-9_-]{1,79}$",
        }],
      });
    },
  });

  const listed = await support.listQualityRuleSets({ mode: "base_api" });

  assert.equal(listed.ok, false);
  assert.equal(listed.error_code, "governance_validation_invalid");
  assert.equal(listed.error_scope, "quality_rule_set");
  assert.ok(Array.isArray(listed.error_items));
  assert.ok(listed.error_items.some((item) => item.path === "set.id"));
});
