const test = require("node:test");
const assert = require("node:assert/strict");

const { createWorkflowAppRegistryStore } = require("../workflow_app_registry_store");
const { createWorkflowManualReviewStore } = require("../workflow_manual_review_store");
const { createWorkflowQualityRuleSetSupport } = require("../workflow_quality_rule_store");
const { createWorkflowRunBaselineStore } = require("../workflow_run_baseline_store");
const { createWorkflowSandboxRuleStore } = require("../workflow_sandbox_rule_store");
const { createWorkflowSandboxAutoFixStore } = require("../workflow_sandbox_autofix_store");
const { createWorkflowVersionStore } = require("../workflow_version_store");

function restoreGlobalUnderscore(hadOwnUnderscore, originalUnderscore) {
  if (hadOwnUnderscore) {
    global._ = originalUnderscore;
    return;
  }
  delete global._;
}

async function assertNoImplicitGlobalUnderscore(run) {
  const hadOwnUnderscore = Object.prototype.hasOwnProperty.call(global, "_");
  const originalUnderscore = global._;
  delete global._;
  try {
    await run();
    assert.equal(Object.prototype.hasOwnProperty.call(global, "_"), false);
  } finally {
    restoreGlobalUnderscore(hadOwnUnderscore, originalUnderscore);
  }
}

const sandboxSupport = {
  normalizeSandboxAlertRules(rules = {}) {
    return {
      whitelist_codes: [],
      whitelist_node_types: [],
      whitelist_keys: [],
      mute_until_by_key: {},
      ...rules,
    };
  },
  applySandboxAutoFixPayload(payload = {}, state = {}) {
    return { payload, state };
  },
  async maybeApplySandboxAutoFix(run = {}, payload = {}, ctx = {}) {
    return { ok: true, run, payload, ctx };
  },
};

test("workflow governance stores do not leak resolveProvider into global underscore", async () => {
  await assertNoImplicitGlobalUnderscore(async () => {
    const store = createWorkflowAppRegistryStore({
      loadConfig: () => ({ mode: "offline_local" }),
    });
    await assert.rejects(
      store.getApp("app_1", { mode: "offline_local", workflowAppRegistryProvider: "unsupported_provider" }),
      /unsupported|provider/i
    );
  });

  await assertNoImplicitGlobalUnderscore(async () => {
    const store = createWorkflowManualReviewStore({
      loadConfig: () => ({ mode: "offline_local" }),
    });
    const out = await store.listQueue(1, { mode: "offline_local", manualReviewProvider: "unsupported_provider" });
    assert.equal(out.ok, false);
    assert.match(String(out.error || ""), /unsupported|provider/i);
  });

  await assertNoImplicitGlobalUnderscore(async () => {
    const support = createWorkflowQualityRuleSetSupport({
      loadConfig: () => ({ mode: "offline_local" }),
    });
    await assert.rejects(
      support.getQualityRuleSet("set_1", { mode: "offline_local", qualityRuleSetProvider: "unsupported_provider" }),
      /unsupported|provider/i
    );
  });

  await assertNoImplicitGlobalUnderscore(async () => {
    const store = createWorkflowRunBaselineStore({
      loadConfig: () => ({ mode: "offline_local" }),
    });
    const out = await store.list(1, { mode: "offline_local", runBaselineProvider: "unsupported_provider" });
    assert.equal(out.ok, false);
    assert.match(String(out.error || ""), /unsupported|provider/i);
  });

  await assertNoImplicitGlobalUnderscore(async () => {
    const store = createWorkflowSandboxRuleStore({
      loadConfig: () => ({ mode: "offline_local" }),
      sandboxSupport,
    });
    const out = await store.getRules({ mode: "offline_local", workflowSandboxRuleProvider: "unsupported_provider" });
    assert.equal(out.ok, false);
    assert.match(String(out.error || ""), /unsupported|provider/i);
  });

  await assertNoImplicitGlobalUnderscore(async () => {
    const store = createWorkflowSandboxAutoFixStore({
      loadConfig: () => ({ mode: "offline_local" }),
      sandboxSupport,
    });
    await assert.rejects(
      store.applyPayload({}, { mode: "offline_local", workflowSandboxAutoFixProvider: "unsupported_provider" }),
      /unsupported|provider/i
    );
  });

  await assertNoImplicitGlobalUnderscore(async () => {
    const store = createWorkflowVersionStore({
      loadConfig: () => ({ mode: "offline_local" }),
    });
    await assert.rejects(
      store.getVersion("ver_1", { mode: "offline_local", workflowVersionProvider: "unsupported_provider" }),
      /unsupported|provider/i
    );
  });
});
