const test = require("node:test");
const assert = require("node:assert/strict");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { registerRustOpsDomainChiplets } = require("../workflow_chiplets/domains/rust_ops_domain");

test("rust ops domain registers representative operators", () => {
  const registry = new WorkflowChipletRegistry();
  registerRustOpsDomainChiplets(
    registry,
    { runIsolatedTask: undefined },
    {
      rustBase: () => "http://127.0.0.1:18082",
      rustRequired: () => true,
      resolveIsolationLevel: () => "none",
      resolveSandboxLimits: () => ({}),
    },
  );
  assert.equal(registry.has("transform_rows_v3"), true);
  assert.equal(registry.has("plugin_operator_v1"), true);
  assert.equal(registry.has("explain_plan_v2"), true);
});

test("rust ops returns fallback result when rust is optional and operator call fails", async () => {
  const registry = new WorkflowChipletRegistry();
  registerRustOpsDomainChiplets(
    registry,
    { runIsolatedTask: undefined },
    {
      rustBase: () => "http://127.0.0.1:18082",
      rustRequired: () => false,
      resolveIsolationLevel: () => "none",
      resolveSandboxLimits: () => ({}),
    },
  );
  const prevFetch = global.fetch;
  global.fetch = async () => {
    throw new Error("network_down");
  };
  try {
    const out = await registry.resolve("transform_rows_v3").run({ runId: "r1", payload: {} }, { config: {} });
    assert.equal(out.ok, false);
    assert.equal(out.status, "fallback");
    assert.equal(out.operator, "transform_rows_v3");
  } finally {
    global.fetch = prevFetch;
  }
});
