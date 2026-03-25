function createWorkflowHistorySupport(deps) {
  const {
    fs,
    diagnosticsLogPath,
    runHistoryPath,
    listRunRecords = null,
  } = deps;

  function readDiagnostics(limit = 50) {
    const filePath = diagnosticsLogPath();
    if (!fs.existsSync(filePath)) return { ok: true, items: [], by_chiplet: {} };
    const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/).filter((item) => item.trim());
    const items = lines
      .slice(Math.max(0, lines.length - limit))
      .map((item) => {
        try {
          return JSON.parse(item);
        } catch {
          return null;
        }
      })
      .filter(Boolean);

    const byChiplet = {};
    for (const item of items) {
      const chiplets = item?.diagnostics?.chiplets || {};
      Object.keys(chiplets).forEach((key) => {
        const row = chiplets[key] || {};
        if (!byChiplet[key]) byChiplet[key] = { runs: 0, failed: 0, seconds_total: 0, attempts_total: 0 };
        byChiplet[key].runs += Number(row.runs || 0);
        byChiplet[key].failed += Number(row.failed || 0);
        byChiplet[key].seconds_total += Number(row.seconds_total || 0);
        byChiplet[key].attempts_total += Number(row.attempts_total || 0);
      });
    }

    Object.values(byChiplet).forEach((item) => {
      item.seconds_total = Number(item.seconds_total.toFixed(3));
      item.seconds_avg = item.runs > 0 ? Number((item.seconds_total / item.runs).toFixed(3)) : 0;
      item.failure_rate = item.runs > 0 ? Number((item.failed / item.runs).toFixed(4)) : 0;
    });

    return { ok: true, items, by_chiplet: byChiplet };
  }

  function percentile(values, q) {
    const array = (Array.isArray(values) ? values : [])
      .map((item) => Number(item))
      .filter((item) => Number.isFinite(item))
      .sort((a, b) => a - b);
    if (!array.length) return 0;
    const position = Math.max(0, Math.min(1, Number(q)));
    const index = Math.ceil(array.length * position) - 1;
    return Number(array[Math.max(0, Math.min(array.length - 1, index))].toFixed(3));
  }

  function isFallbackNodeRun(nodeRun) {
    const status = String(nodeRun?.output?.status || nodeRun?.status || "").toLowerCase();
    if (status === "fallback") return true;
    if (nodeRun?.output?.fallback === true || nodeRun?.output?.degraded === true) return true;
    const detail = String(nodeRun?.output?.detail || nodeRun?.error || "").toLowerCase();
    return /fallback|degrad|闄嶇骇/.test(detail);
  }

  function listRunHistory(limit = 50) {
    const filePath = runHistoryPath();
    if (!fs.existsSync(filePath)) return [];
    const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/).filter((item) => item.trim());
    return lines
      .slice(Math.max(0, lines.length - Math.max(1, Math.min(5000, Number(limit || 50)))))
      .map((item) => {
        try {
          return JSON.parse(item);
        } catch {
          return null;
        }
      })
      .filter(Boolean)
      .reverse();
  }

  async function loadRunRecords(limit = 200) {
    if (typeof listRunRecords === "function") {
      const out = await listRunRecords(limit);
      if (Array.isArray(out)) return { ok: true, items: out };
      if (out && typeof out === "object" && out.ok === false) return out;
      return { ok: true, items: Array.isArray(out?.items) ? out.items : [] };
    }
    return { ok: true, items: listRunHistory(limit) };
  }

  async function buildPerfDashboard(limit = 200) {
    const listed = await loadRunRecords(limit);
    if (!listed?.ok) return listed;
    const runs = Array.isArray(listed.items) ? listed.items : [];
    const byChiplet = {};
    runs.forEach((item) => {
      const nodes = Array.isArray(item?.result?.node_runs) ? item.result.node_runs : [];
      nodes.forEach((node) => {
        const key = String(node?.type || "").trim();
        if (!key) return;
        if (!byChiplet[key]) {
          byChiplet[key] = {
            chiplet: key,
            calls: 0,
            failed: 0,
            seconds: [],
            attempts_total: 0,
            fallback_count: 0,
          };
        }
        byChiplet[key].calls += 1;
        if (String(node?.status || "") !== "done") byChiplet[key].failed += 1;
        byChiplet[key].seconds.push(Number(node?.seconds || 0));
        byChiplet[key].attempts_total += Math.max(1, Number(node?.telemetry?.attempts || 1));
        if (isFallbackNodeRun(node)) byChiplet[key].fallback_count += 1;
      });
    });
    const items = Object.values(byChiplet).map((item) => {
      const calls = Number(item.calls || 0);
      const retries = Math.max(0, Number(item.attempts_total || 0) - calls);
      return {
        ok: true,
        chiplet: item.chiplet,
        calls,
        failed: Number(item.failed || 0),
        error_rate: calls > 0 ? Number((Number(item.failed || 0) / calls).toFixed(4)) : 0,
        retry_count: retries,
        retry_rate: calls > 0 ? Number((retries / calls).toFixed(4)) : 0,
        fallback_count: Number(item.fallback_count || 0),
        fallback_rate: calls > 0 ? Number((Number(item.fallback_count || 0) / calls).toFixed(4)) : 0,
        p50_seconds: percentile(item.seconds, 0.5),
        p95_seconds: percentile(item.seconds, 0.95),
        p99_seconds: percentile(item.seconds, 0.99),
        avg_seconds: item.seconds.length
          ? Number((item.seconds.reduce((sum, value) => sum + Number(value || 0), 0) / item.seconds.length).toFixed(3))
          : 0,
      };
    }).sort((a, b) => Number(b.calls || 0) - Number(a.calls || 0));
    return { ok: true, total_runs: runs.length, items };
  }

  function findRunById(runId) {
    const id = String(runId || "").trim();
    if (!id) return null;
    return listRunHistory(2000).find((item) => String(item?.run_id || "") === id) || null;
  }

  function runTimeline(runId) {
    const found = findRunById(runId);
    if (!found) return { ok: false, error: "run not found" };
    const rows = Array.isArray(found?.result?.node_runs) ? found.result.node_runs : [];
    const timeline = rows
      .map((node) => ({
        node_id: String(node?.id || ""),
        type: String(node?.type || ""),
        status: String(node?.status || ""),
        started_at: String(node?.started_at || ""),
        ended_at: String(node?.ended_at || ""),
        seconds: Number(node?.seconds || 0),
      }))
      .sort((a, b) => String(a.started_at || "").localeCompare(String(b.started_at || "")));
    return {
      ok: true,
      run_id: runId,
      status: String(found?.result?.status || ""),
      timeline,
    };
  }

  function failureSummary(limit = 400) {
    const runs = listRunHistory(limit);
    const failedRuns = runs.filter((item) => !item?.result?.ok);
    const byNode = {};
    failedRuns.forEach((run) => {
      const nodeRuns = Array.isArray(run?.result?.node_runs) ? run.result.node_runs : [];
      nodeRuns
        .filter((node) => String(node?.status || "") === "failed")
        .forEach((node) => {
          const key = String(node?.type || "unknown");
          if (!byNode[key]) byNode[key] = { failed: 0, samples: [] };
          byNode[key].failed += 1;
          if (byNode[key].samples.length < 3) byNode[key].samples.push(String(node?.error || "").slice(0, 200));
        });
    });
    return {
      ok: true,
      total_runs: runs.length,
      failed_runs: failedRuns.length,
      by_node: byNode,
    };
  }

  return {
    buildPerfDashboard,
    failureSummary,
    findRunById,
    listRunHistory,
    readDiagnostics,
    runTimeline,
  };
}

function registerWorkflowHistoryIpc(ctx, deps) {
  const { ipcMain } = ctx;
  const {
    readDiagnostics,
    buildPerfDashboard,
    listRunHistory,
    getRun,
    runTimeline,
    failureSummary,
  } = deps;

  ipcMain.handle("aiwf:getWorkflowDiagnostics", async (_evt, opts) => {
    const limit = Number(opts?.limit || 50);
    const safe = Number.isFinite(limit) ? Math.max(1, Math.min(500, Math.floor(limit))) : 50;
    return readDiagnostics(safe);
  });

  ipcMain.handle("aiwf:getWorkflowPerfDashboard", async (_evt, opts) => {
    const limit = Number(opts?.limit || 200);
    const safe = Number.isFinite(limit) ? Math.max(1, Math.min(2000, Math.floor(limit))) : 200;
    return await buildPerfDashboard(safe);
  });

  ipcMain.handle("aiwf:listWorkflowRuns", async (_evt, opts) => {
    const limit = Number(opts?.limit || 50);
    const safe = Number.isFinite(limit) ? Math.max(1, Math.min(500, Math.floor(limit))) : 50;
    return await listRunHistory(safe);
  });

  ipcMain.handle("aiwf:getWorkflowLineage", async (_evt, req) => {
    try {
      const runId = String(req?.run_id || "").trim();
      const hit = await getRun(runId);
      if (!hit) return { ok: false, error: "run not found" };
      const lineage = hit?.result?.lineage && typeof hit.result.lineage === "object" ? hit.result.lineage : {};
      return { ok: true, run_id: runId, lineage };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:getWorkflowRunTimeline", async (_evt, req) => {
    return await runTimeline(String(req?.run_id || "").trim());
  });

  ipcMain.handle("aiwf:getWorkflowFailureSummary", async (_evt, req) => {
    const limit = Number(req?.limit || 400);
    return await failureSummary(limit);
  });
}

module.exports = {
  createWorkflowHistorySupport,
  registerWorkflowHistoryIpc,
};
