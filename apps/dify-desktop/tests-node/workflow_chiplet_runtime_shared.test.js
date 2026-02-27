const test = require("node:test");
const assert = require("node:assert/strict");
const { createRuntimeSharedHelpers } = require("../workflow_chiplets/domains/runtime_shared");

test("resolveIsolationLevel returns none when isolation runtime is unavailable", () => {
  const helpers = createRuntimeSharedHelpers({ runIsolatedTask: undefined });
  const out = helpers.resolveIsolationLevel(
    { payload: { chiplet_isolation_mode: "all" } },
    "ai_refine",
    true,
    {},
  );
  assert.equal(out, "none");
});

test("resolveIsolationLevel honors node-level explicit sandbox setting", () => {
  const helpers = createRuntimeSharedHelpers({ runIsolatedTask: async () => ({}) });
  const out = helpers.resolveIsolationLevel({}, "ai_refine", false, { config: { isolation_level: "sandbox" } });
  assert.equal(out, "sandbox");
});

test("resolveIsolationLevel returns process for mode=all", () => {
  const helpers = createRuntimeSharedHelpers({ runIsolatedTask: async () => ({}) });
  const out = helpers.resolveIsolationLevel(
    { payload: { chiplet_isolation_mode: "all" } },
    "any_type",
    false,
    {},
  );
  assert.equal(out, "process");
});

test("resolveSandboxLimits applies node > payload > config > env precedence", () => {
  const prev = {
    AIWF_SANDBOX_MAX_DURATION_MS: process.env.AIWF_SANDBOX_MAX_DURATION_MS,
    AIWF_SANDBOX_MAX_CPU_MS: process.env.AIWF_SANDBOX_MAX_CPU_MS,
    AIWF_SANDBOX_MAX_RSS_MB: process.env.AIWF_SANDBOX_MAX_RSS_MB,
    AIWF_SANDBOX_MAX_OUTPUT_BYTES: process.env.AIWF_SANDBOX_MAX_OUTPUT_BYTES,
  };
  process.env.AIWF_SANDBOX_MAX_DURATION_MS = "111";
  process.env.AIWF_SANDBOX_MAX_CPU_MS = "222";
  process.env.AIWF_SANDBOX_MAX_RSS_MB = "333";
  process.env.AIWF_SANDBOX_MAX_OUTPUT_BYTES = "444";
  try {
    const helpers = createRuntimeSharedHelpers({ runIsolatedTask: async () => ({}) });
    const out = helpers.resolveSandboxLimits(
      {
        config: { sandbox_limits: { max_duration_ms: 1000, max_cpu_ms: 2000, max_rss_mb: 3000, max_output_bytes: 4000 } },
        payload: { sandbox_limits: { max_duration_ms: 1100, max_cpu_ms: 2100, max_rss_mb: 3100, max_output_bytes: 4100 } },
      },
      { config: { sandbox_limits: { max_duration_ms: 1200, max_cpu_ms: 2200, max_rss_mb: 3200, max_output_bytes: 4200 } } },
    );
    assert.deepEqual(out, {
      max_duration_ms: 1200,
      max_cpu_ms: 2200,
      max_rss_mb: 3200,
      max_output_bytes: 4200,
    });
  } finally {
    process.env.AIWF_SANDBOX_MAX_DURATION_MS = prev.AIWF_SANDBOX_MAX_DURATION_MS;
    process.env.AIWF_SANDBOX_MAX_CPU_MS = prev.AIWF_SANDBOX_MAX_CPU_MS;
    process.env.AIWF_SANDBOX_MAX_RSS_MB = prev.AIWF_SANDBOX_MAX_RSS_MB;
    process.env.AIWF_SANDBOX_MAX_OUTPUT_BYTES = prev.AIWF_SANDBOX_MAX_OUTPUT_BYTES;
  }
});
