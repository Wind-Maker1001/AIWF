const test = require("node:test");
const assert = require("node:assert/strict");
const {
  normalizeDifyRequest,
  normalizeDifyResponse,
  normalizeDifyError,
  normalizeDifyAiConfig,
  pickDifyEndpoint,
  buildDifyInvokePayload,
} = require("../dify_adapter");

test("normalizeDifyRequest provides stable defaults", () => {
  const out = normalizeDifyRequest({});
  assert.equal(out.owner, "dify");
  assert.equal(out.actor, "dify");
  assert.equal(out.ruleset_version, "v1");
  assert.deepEqual(out.params, {});
});

test("normalizeDifyResponse provides stable shape", () => {
  const out = normalizeDifyResponse({ ok: true, job_id: "j1", artifacts: [{ id: 1 }] });
  assert.equal(out.ok, true);
  assert.equal(out.job_id, "j1");
  assert.ok(Array.isArray(out.artifacts));
});

test("normalizeDifyError maps status code to error code", () => {
  assert.equal(normalizeDifyError({ error: "x" }, 401).code, "AUTH_FAILED");
  assert.equal(normalizeDifyError({ error: "x" }, 504).code, "TIMEOUT");
  assert.equal(normalizeDifyError({ error: "x" }, 500).code, "UPSTREAM_5XX");
  assert.equal(normalizeDifyError({ error: "x" }, 400).code, "UPSTREAM_4XX");
});

test("normalizeDifyAiConfig provides safe defaults", () => {
  const out = normalizeDifyAiConfig({});
  assert.equal(out.provider, "openai");
  assert.equal(out.timeout_ms, 120000);
  assert.equal(out.max_retries, 2);
  assert.deepEqual(out.router.endpoints, []);
});

test("pickDifyEndpoint prefers local endpoint when configured", () => {
  const ep = pickDifyEndpoint({
    router: {
      strategy: "prefer_local",
      endpoints: [
        { endpoint: "https://api.remote/v1" },
        { endpoint: "http://127.0.0.1:8000/v1" },
      ],
    },
  });
  assert.equal(ep, "http://127.0.0.1:8000/v1");
});

test("buildDifyInvokePayload injects ai and cooked payload", () => {
  const out = buildDifyInvokePayload(
    { owner: "u1", actor: "u1", params: { a: 1 } },
    { model: "gpt-x", endpoint: "http://localhost:1234/v1", timeout_ms: 9000 },
    { markdown_path: "x.md" },
  );
  assert.equal(out.owner, "u1");
  assert.equal(out.params.ai.model, "gpt-x");
  assert.equal(out.params.ai.endpoint, "http://localhost:1234/v1");
  assert.equal(out.params.ai.timeout_ms, 9000);
  assert.equal(out.params.cooked_payload.markdown_path, "x.md");
});
