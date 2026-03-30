const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("async benchmark scripts keep tenant-aware quota-respecting submission", () => {
  const bench = readText("ops/scripts/bench_rust_async_tasks.ps1");
  const trend = readText("ops/scripts/check_async_bench_trend.ps1");

  assert.match(bench, /TenantId/i);
  assert.match(bench, /MaxInFlight/i);
  assert.match(bench, /AIWF_ASYNC_BENCH_TENANT_ID/i);
  assert.match(bench, /AIWF_ASYNC_BENCH_MAX_IN_FLIGHT/i);
  assert.match(bench, /"tenant_id": tenant_id/);
  assert.match(bench, /"submission_mode": "quota_respecting"/);
  assert.match(bench, /len\(pending\) < max_in_flight/);
  assert.match(bench, /tenant concurrency exceeded/i);

  assert.match(trend, /TenantId/i);
  assert.match(trend, /MaxInFlight/i);
  assert.match(trend, /AIWF_ASYNC_BENCH_TENANT_ID/i);
  assert.match(trend, /AIWF_ASYNC_BENCH_MAX_IN_FLIGHT/i);
  assert.match(trend, /submission_mode/i);
  assert.match(trend, /max_in_flight/i);
  assert.match(trend, /legacy_unbounded/i);
});

test("async benchmark documentation keeps quota guidance explicit", () => {
  const verification = readText("docs/verification.md");
  const minimalDelivery = readText("docs/offline_delivery_minimal.md");
  const nativeDelivery = readText("docs/offline_delivery_native_winui.md");

  assert.match(verification, /check_async_bench_trend\.ps1/i);
  assert.match(verification, /bench_async/i);
  assert.match(verification, /AIWF_ASYNC_BENCH_MAX_IN_FLIGHT/i);
  assert.match(verification, /AIWF_TENANT_MAX_CONCURRENCY/i);
  assert.match(minimalDelivery, /bench_async/i);
  assert.match(minimalDelivery, /AIWF_ASYNC_BENCH_MAX_IN_FLIGHT/i);
  assert.match(nativeDelivery, /bench_async/i);
  assert.match(nativeDelivery, /AIWF_ASYNC_BENCH_MAX_IN_FLIGHT/i);
});
