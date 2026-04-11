const test = require("node:test");
const assert = require("node:assert/strict");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { registerRustOpsDomainChiplets } = require("../workflow_chiplets/domains/rust_ops_domain");
const {
  DESKTOP_RUST_OPERATOR_TYPES,
  assertDesktopRustOperator,
} = require("../workflow_chiplets/domains/rust_operator_manifest.generated");

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
  assert.equal(registry.has("postprocess_rows_v1"), true);
  assert.equal(registry.has("plugin_operator_v1"), true);
  assert.equal(registry.has("explain_plan_v2"), true);
  assert.deepEqual([...registry.list()].sort(), [...DESKTOP_RUST_OPERATOR_TYPES]);
});

test("desktop rust operator manifest excludes hidden workflow-only operators", () => {
  assert.throws(
    () => assertDesktopRustOperator("aggregate_pushdown_v1"),
    /desktop rust operator not manifest-authorized/i,
  );
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

test("rust ops returns fallback result when remote rust endpoint is blocked and rust is optional", async () => {
  const registry = new WorkflowChipletRegistry();
  const prevAllowEgress = process.env.AIWF_ALLOW_EGRESS;
  const prevAllowCloud = process.env.AIWF_ALLOW_CLOUD_LLM;
  delete process.env.AIWF_ALLOW_EGRESS;
  delete process.env.AIWF_ALLOW_CLOUD_LLM;
  registerRustOpsDomainChiplets(
    registry,
    { runIsolatedTask: undefined },
    {
      rustBase: () => "https://rust.example.com",
      rustRequired: () => false,
      resolveIsolationLevel: () => "none",
      resolveSandboxLimits: () => ({}),
    },
  );
  try {
    const out = await registry.resolve("transform_rows_v3").run({ runId: "r1", payload: {} }, { config: {} });
    assert.equal(out.ok, false);
    assert.equal(out.status, "fallback");
    assert.match(String(out.detail || ""), /rust_egress_blocked/i);
  } finally {
    if (prevAllowEgress === undefined) delete process.env.AIWF_ALLOW_EGRESS;
    else process.env.AIWF_ALLOW_EGRESS = prevAllowEgress;
    if (prevAllowCloud === undefined) delete process.env.AIWF_ALLOW_CLOUD_LLM;
    else process.env.AIWF_ALLOW_CLOUD_LLM = prevAllowCloud;
  }
});

test("rust ops rejects when remote rust endpoint is blocked and rust is required", async () => {
  const registry = new WorkflowChipletRegistry();
  const prevAllowEgress = process.env.AIWF_ALLOW_EGRESS;
  const prevAllowCloud = process.env.AIWF_ALLOW_CLOUD_LLM;
  delete process.env.AIWF_ALLOW_EGRESS;
  delete process.env.AIWF_ALLOW_CLOUD_LLM;
  registerRustOpsDomainChiplets(
    registry,
    { runIsolatedTask: undefined },
    {
      rustBase: () => "https://rust.example.com",
      rustRequired: () => true,
      resolveIsolationLevel: () => "none",
      resolveSandboxLimits: () => ({}),
    },
  );
  try {
    await assert.rejects(
      registry.resolve("transform_rows_v3").run({ runId: "r1", payload: {} }, { config: {} }),
      /rust_egress_blocked/i,
    );
  } finally {
    if (prevAllowEgress === undefined) delete process.env.AIWF_ALLOW_EGRESS;
    else process.env.AIWF_ALLOW_EGRESS = prevAllowEgress;
    if (prevAllowCloud === undefined) delete process.env.AIWF_ALLOW_CLOUD_LLM;
    else process.env.AIWF_ALLOW_CLOUD_LLM = prevAllowCloud;
  }
});
