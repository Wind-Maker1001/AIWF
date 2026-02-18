function registerMetricsIpc(ctx) {
  const {
    ipcMain,
    fs,
    path,
    routeMetricsLogPath,
    routeMetricsSummaryPath,
    rotateLogIfNeeded,
  } = ctx;

  ipcMain.handle("aiwf:logRouteMetrics", async (_evt, payload) => {
    try {
      const p = routeMetricsLogPath();
      const sp = routeMetricsSummaryPath();
      const recentWindowSize = 50;
      fs.mkdirSync(path.dirname(p), { recursive: true });
      rotateLogIfNeeded(p);
      const rec = {
        ts: new Date().toISOString(),
        ...((payload && typeof payload === "object") ? payload : {}),
      };
      fs.appendFileSync(p, JSON.stringify(rec) + "\n", "utf8");
      let summary = { schema_version: 2, updated_at: rec.ts, recent_window_size: recentWindowSize, workflows: {} };
      if (fs.existsSync(sp)) {
        try { summary = JSON.parse(fs.readFileSync(sp, "utf8")); } catch {}
      }
      if (!summary || typeof summary !== "object") summary = { schema_version: 2, updated_at: rec.ts, recent_window_size: recentWindowSize, workflows: {} };
      summary.schema_version = 2;
      summary.recent_window_size = Number(summary.recent_window_size || recentWindowSize) || recentWindowSize;
      const wf = String(rec.workflow_id || "custom");
      if (!summary.workflows || typeof summary.workflows !== "object") summary.workflows = {};
      if (!summary.workflows[wf]) {
        summary.workflows[wf] = {
          samples: 0,
          renders: 0,
          solved: 0,
          astar: 0,
          segmented: 0,
          fallback: 0,
          cache_hit: 0,
          cache_miss: 0,
          avg_fallback_ratio: 0,
          recent: [],
          recent_avg_fallback_ratio: 0,
          recent_avg_cache_hit_rate: 0,
          last_sample: {},
        };
      }
      const s = summary.workflows[wf];
      s.samples += 1;
      s.renders += Number(rec.renders || 0);
      s.solved += Number(rec.solved || 0);
      s.astar += Number(rec.astar || 0);
      s.segmented += Number(rec.segmented || 0);
      s.fallback += Number(rec.fallback || 0);
      s.cache_hit += Number(rec.cache_hit || 0);
      s.cache_miss += Number(rec.cache_miss || 0);
      const cur = Number(rec.fallback_ratio || 0);
      s.avg_fallback_ratio = Number((((Number(s.avg_fallback_ratio || 0) * (s.samples - 1)) + cur) / s.samples).toFixed(4));
      if (!Array.isArray(s.recent)) s.recent = [];
      s.recent.push({
        ts: rec.ts,
        fallback_ratio: cur,
        cache_hit: Number(rec.cache_hit || 0),
        cache_miss: Number(rec.cache_miss || 0),
        solved: Number(rec.solved || 0),
      });
      while (s.recent.length > summary.recent_window_size) s.recent.shift();
      const recent = s.recent;
      const recentAvgFb = recent.length ? recent.reduce((a, x) => a + Number(x.fallback_ratio || 0), 0) / recent.length : 0;
      const hit = recent.reduce((a, x) => a + Number(x.cache_hit || 0), 0);
      const miss = recent.reduce((a, x) => a + Number(x.cache_miss || 0), 0);
      const recentHitRate = (hit + miss) > 0 ? hit / (hit + miss) : 0;
      s.recent_avg_fallback_ratio = Number(recentAvgFb.toFixed(4));
      s.recent_avg_cache_hit_rate = Number(recentHitRate.toFixed(4));
      s.last_sample = {
        ts: rec.ts,
        fallback_ratio: cur,
        cache_hit: Number(rec.cache_hit || 0),
        cache_miss: Number(rec.cache_miss || 0),
        solved: Number(rec.solved || 0),
      };
      summary.updated_at = rec.ts;
      fs.writeFileSync(sp, JSON.stringify(summary, null, 2), "utf8");
      return { ok: true, path: p, summary_path: sp };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:getRouteMetricsSummary", async () => {
    try {
      const sp = routeMetricsSummaryPath();
      const lp = routeMetricsLogPath();
      const dir = path.dirname(sp);
      if (!fs.existsSync(sp)) {
        return { ok: true, exists: false, path: sp, log_path: lp, log_dir: dir, summary: {} };
      }
      const summary = JSON.parse(fs.readFileSync(sp, "utf8"));
      return { ok: true, exists: true, path: sp, log_path: lp, log_dir: dir, summary };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });
}

module.exports = {
  registerMetricsIpc,
};
