const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPreflightRustHelpersModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/preflight-rust-helpers.js")).href;
  return import(file);
}

test("preflight rust helpers build io-contract input with fallback file and operator-specific fields", async () => {
  const { buildIoContractInput, IO_CONTRACT_COMPATIBLE_OPERATORS } = await loadPreflightRustHelpersModule();
  assert.equal(IO_CONTRACT_COMPATIBLE_OPERATORS.has("transform_rows_v3"), true);
  assert.deepEqual(
    buildIoContractInput("transform_rows_v3", {}, "D:/input.csv"),
    { input_uri: "D:/input.csv" }
  );
  assert.deepEqual(
    buildIoContractInput("anomaly_explain_v1", { rows: [{ a: 1 }], score_field: "risk_score" }),
    { rows: [{ a: 1 }], score_field: "risk_score" }
  );
  assert.deepEqual(
    buildIoContractInput("plugin_operator_v1", { plugin: "demo" }),
    { plugin: "demo" }
  );
});

test("preflight rust helpers normalize non-ok and ok rust responses", async () => {
  const { postRustOperator } = await loadPreflightRustHelpersModule();
  const fail = await postRustOperator("http://localhost", "/operators/test", { ok: false }, async () => ({
    ok: false,
    status: 503,
  }));
  assert.deepEqual(fail, { ok: false, status: 503, error: "HTTP 503" });

  const success = await postRustOperator("http://localhost", "/operators/test", { ok: true }, async (url, init) => {
    assert.equal(url, "http://localhost/operators/test");
    assert.equal(init.method, "POST");
    return {
      ok: true,
      async json() {
        return { valid: true };
      },
    };
  });
  assert.deepEqual(success, { ok: true, body: { valid: true } });
});
