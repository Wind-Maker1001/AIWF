const test = require("node:test");
const assert = require("node:assert/strict");

process.env.AIWF_CHIPLET_POOL_SIZE = "1";
process.env.AIWF_CHIPLET_AI_POOL_SIZE = "1";

const {
  runIsolatedTask,
  shutdownAllPools,
  getPoolStats,
} = require("../workflow_chiplets/isolated_worker_host");

test("isolated worker pool drains queued ai_refine tasks", async () => {
  const payload = {
    workflowPayload: {},
    corpusText: "a\nb\nc",
    metrics: {
      sections: 1,
      bullets: 0,
      chars: 5,
      cjk: 0,
      latin: 3,
      sha256: "x",
    },
  };
  const p1 = runIsolatedTask("ai_refine", payload, 15000);
  const p2 = runIsolatedTask("ai_refine", payload, 15000);
  const p3 = runIsolatedTask("ai_refine", payload, 15000);
  const out = await Promise.all([p1, p2, p3]);
  assert.equal(out.length, 3);
  assert.ok(String(out[0].text || "").length > 0);
  assert.ok(String(out[1].text || "").length > 0);
  assert.ok(String(out[2].text || "").length > 0);
});

test("isolated worker rejects unsupported task", async () => {
  await assert.rejects(
    runIsolatedTask("not_supported_task", {}, 5000),
    /unsupported isolated task/i
  );
});

test("isolated worker supports ai_call task", async () => {
  const out = await runIsolatedTask("ai_call", {
    workflowPayload: {},
    corpusText: "hello\nworld",
    metrics: { sections: 1, bullets: 0, chars: 11, cjk: 0, latin: 10, sha256: "x" },
  }, 15000);
  assert.ok(String(out?.text || "").length > 0);
});

test("sandbox mode blocks remote ai_call endpoint", async () => {
  await assert.rejects(
    runIsolatedTask("ai_call", {
      workflowPayload: {
        ai: {
          endpoint: "https://api.example.com/v1/chat/completions",
          api_key: "k",
          model: "m",
        },
      },
      corpusText: "hello",
      metrics: { sections: 1, bullets: 0, chars: 5, cjk: 0, latin: 5, sha256: "x" },
      isolation_level: "sandbox",
    }, 15000),
    /sandbox_egress_blocked/i
  );
});

test("sandbox mode enforces output byte limit", async () => {
  await assert.rejects(
    runIsolatedTask("ai_call", {
      workflowPayload: {},
      corpusText: "hello\nworld",
      metrics: { sections: 1, bullets: 0, chars: 11, cjk: 0, latin: 10, sha256: "x" },
      isolation_level: "sandbox",
      sandbox_limits: { max_output_bytes: 32 },
    }, 15000),
    /sandbox_limit_exceeded:output/i
  );
});

test("pool stats are observable", async () => {
  await runIsolatedTask("ai_refine", {
    workflowPayload: {},
    corpusText: "x",
    metrics: { sections: 1, bullets: 0, chars: 1, cjk: 0, latin: 1, sha256: "x" },
  }, 15000);
  const stat = getPoolStats();
  assert.ok(stat && stat.totals && Number.isFinite(Number(stat.totals.size)));
});

test.after(() => {
  shutdownAllPools();
});
