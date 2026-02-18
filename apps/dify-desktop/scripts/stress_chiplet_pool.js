const {
  runIsolatedTask,
  shutdownAllPools,
  getPoolStats,
} = require("../workflow_chiplets/isolated_worker_host");

async function main() {
  const seconds = Number(process.env.AIWF_CHIPLET_STRESS_SECONDS || "30");
  const concurrency = Number(process.env.AIWF_CHIPLET_STRESS_CONCURRENCY || "4");
  const timeoutMs = Number(process.env.AIWF_CHIPLET_STRESS_TASK_TIMEOUT_MS || "15000");
  const task = String(process.env.AIWF_CHIPLET_STRESS_TASK || "ai_refine").trim().toLowerCase();
  const endAt = Date.now() + Math.max(5, seconds) * 1000;

  let ok = 0;
  let failed = 0;
  let launched = 0;

  async function workerLoop() {
    while (Date.now() < endAt) {
      launched += 1;
      try {
        if (task === "compute_rust") {
          await runIsolatedTask("compute_rust", {
            corpusText: "stress test corpus\n- a\n- b\n- c\n",
            options: { run_id: `stress_${launched}`, rust_required: false },
          }, timeoutMs);
        } else {
          await runIsolatedTask("ai_refine", {
            workflowPayload: {},
            corpusText: "stress test corpus\n- a\n- b\n- c\n",
            metrics: { sections: 1, bullets: 3, chars: 28, cjk: 0, latin: 24, sha256: "stress" },
          }, timeoutMs);
        }
        ok += 1;
      } catch {
        failed += 1;
      }
    }
  }

  const workers = [];
  for (let i = 0; i < Math.max(1, concurrency); i += 1) workers.push(workerLoop());
  await Promise.all(workers);
  const stats = getPoolStats();
  shutdownAllPools();

  const out = {
    ok: failed === 0,
    seconds: Math.max(5, seconds),
    concurrency: Math.max(1, concurrency),
    task,
    launched,
    succeeded: ok,
    failed,
    pool: stats,
  };
  console.log(JSON.stringify(out, null, 2));
  if (failed > 0) process.exit(1);
}

main().catch((e) => {
  console.error(String(e && e.stack ? e.stack : e));
  try { shutdownAllPools(); } catch {}
  process.exit(1);
});
