const test = require("node:test");
const assert = require("node:assert/strict");
const { createAiGuardrailsHelpers } = require("../workflow_chiplets/domains/ai_guardrails_policy");

test("shouldBlockAiOnData blocks when data file is detected", () => {
  const helpers = createAiGuardrailsHelpers({
    collectFiles: () => ["D:/input/report.xlsx"],
  });
  const out = helpers.shouldBlockAiOnData({ payload: {} }, { config: {} });
  assert.equal(out.block, true);
  assert.match(out.reason, /data_file_detected/);
});

test("shouldBlockAiOnData allows explicit override on node config", () => {
  const helpers = createAiGuardrailsHelpers({
    collectFiles: () => ["D:/input/report.xlsx"],
  });
  const out = helpers.shouldBlockAiOnData({ payload: {} }, { config: { allow_ai_on_data: true } });
  assert.deepEqual(out, { block: false, reason: "" });
});

test("hasCitationMarkers detects bracket refs and source label", () => {
  const helpers = createAiGuardrailsHelpers({ collectFiles: () => [] });
  assert.equal(helpers.hasCitationMarkers("evidence [1]"), true);
  assert.equal(helpers.hasCitationMarkers("source: internal note"), true);
  assert.equal(helpers.hasCitationMarkers("plain text only"), false);
});

test("ai budget helpers enforce call limit and accumulate usage", () => {
  const helpers = createAiGuardrailsHelpers({ collectFiles: () => [] });
  const ctx = {
    aiBudget: {
      enabled: true,
      calls: 0,
      max_calls_per_run: 1,
      estimated_tokens: 0,
      max_estimated_tokens_per_run: 9999,
      token_price_usd_per_1k: 0.002,
      max_estimated_cost_usd_per_run: 0.8,
    },
  };

  helpers.enforceAiBudgetBeforeCall(ctx, "hello");
  helpers.recordAiBudgetAfterCall(ctx, "hello", "world");
  assert.equal(ctx.aiBudget.calls, 1);
  assert.ok(ctx.aiBudget.estimated_tokens > 0);
  assert.ok(ctx.aiBudget.estimated_cost_usd > 0);
  assert.throws(() => helpers.enforceAiBudgetBeforeCall(ctx, "next"), /ai_budget_exceeded:calls/);
});
