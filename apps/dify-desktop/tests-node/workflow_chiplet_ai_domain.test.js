const test = require("node:test");
const assert = require("node:assert/strict");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { registerAiDomainChiplets } = require("../workflow_chiplets/domains/ai_domain");

function register(registry) {
  registerAiDomainChiplets(
    registry,
    {
      callExternalAi: async () => ({ reason: "ok", text: "hello [1]" }),
      auditAiText: () => ({ passed: true, reasons: [] }),
      summarizeCorpus: () => ({ sections: 0, bullets: 0, chars: 0, cjk: 0, latin: 0, sha256: "x" }),
      computeViaRust: async () => ({ mode: "rust_http", started: false, metrics: { sections: 0, bullets: 0, chars: 0, cjk: 0, latin: 0 } }),
      runIsolatedTask: undefined,
    },
    {
      shouldBlockAiOnData: () => ({ block: false, reason: "" }),
      enforceAiBudgetBeforeCall: () => {},
      recordAiBudgetAfterCall: () => {},
      resolveIsolationLevel: () => "none",
      resolveSandboxLimits: () => ({}),
      extractNumericTokens: (s) => String(s || "").match(/\d+/g) || [],
      hasCitationMarkers: (s) => /\[[0-9]+\]/.test(String(s || "")),
      compareMetricCore: () => [],
    },
  );
}

test("ai domain registers expected chiplets", () => {
  const registry = new WorkflowChipletRegistry();
  register(registry);
  assert.equal(registry.has("ai_strategy_v1"), true);
  assert.equal(registry.has("ai_refine"), true);
  assert.equal(registry.has("ai_audit"), true);
});

test("ai_refine returns reuse_existing when aiText already exists", async () => {
  const registry = new WorkflowChipletRegistry();
  register(registry);
  const ctx = {
    payload: { ai: {} },
    corpusText: "corpus",
    aiText: "already exists",
  };
  const out = await registry.resolve("ai_refine").run(ctx, { config: { reuse_existing: true } });
  assert.equal(out.ai_mode, "reuse_existing");
  assert.equal(out.ai_text_chars, "already exists".length);
});
