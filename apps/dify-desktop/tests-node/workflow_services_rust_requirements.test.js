const test = require("node:test");
const assert = require("node:assert/strict");
const { computeViaRust } = require("../workflow_services");

test("computeViaRust falls back when rust is optional and remote egress is blocked", async () => {
  const prevAllowEgress = process.env.AIWF_ALLOW_EGRESS;
  const prevAllowCloud = process.env.AIWF_ALLOW_CLOUD_LLM;
  delete process.env.AIWF_ALLOW_EGRESS;
  delete process.env.AIWF_ALLOW_CLOUD_LLM;
  try {
    const out = await computeViaRust("hello world", {
      rust_endpoint: "https://rust.example.com",
      rust_required: false,
    });
    assert.equal(out.mode, "js_fallback_egress_blocked");
    assert.equal(out.started, false);
  } finally {
    if (prevAllowEgress === undefined) delete process.env.AIWF_ALLOW_EGRESS;
    else process.env.AIWF_ALLOW_EGRESS = prevAllowEgress;
    if (prevAllowCloud === undefined) delete process.env.AIWF_ALLOW_CLOUD_LLM;
    else process.env.AIWF_ALLOW_CLOUD_LLM = prevAllowCloud;
  }
});

test("computeViaRust rejects when rust is required and remote egress is blocked", async () => {
  const prevAllowEgress = process.env.AIWF_ALLOW_EGRESS;
  const prevAllowCloud = process.env.AIWF_ALLOW_CLOUD_LLM;
  delete process.env.AIWF_ALLOW_EGRESS;
  delete process.env.AIWF_ALLOW_CLOUD_LLM;
  try {
    await assert.rejects(
      computeViaRust("hello world", {
        rust_endpoint: "https://rust.example.com",
        rust_required: true,
      }),
      /rust_egress_blocked/i,
    );
  } finally {
    if (prevAllowEgress === undefined) delete process.env.AIWF_ALLOW_EGRESS;
    else process.env.AIWF_ALLOW_EGRESS = prevAllowEgress;
    if (prevAllowCloud === undefined) delete process.env.AIWF_ALLOW_CLOUD_LLM;
    else process.env.AIWF_ALLOW_CLOUD_LLM = prevAllowCloud;
  }
});
