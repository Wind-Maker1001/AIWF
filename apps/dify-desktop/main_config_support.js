const CONFIG_NAME = "config.json";

function createConfigSupport({ app, fs, path }) {
  function configPath() {
    return path.join(app.getPath("userData"), CONFIG_NAME);
  }

  function routeMetricsLogPath() {
    return path.join(app.getPath("userData"), "logs", "route_metrics.jsonl");
  }

  function routeMetricsSummaryPath() {
    return path.join(app.getPath("userData"), "logs", "route_metrics_summary.json");
  }

  function rotateLogIfNeeded(filePath, maxBytes = 5 * 1024 * 1024, keep = 5) {
    try {
      if (!fs.existsSync(filePath)) return;
      const st = fs.statSync(filePath);
      if (!st || st.size < maxBytes) return;
      const dir = path.dirname(filePath);
      const base = path.basename(filePath, ".jsonl");
      const ts = new Date().toISOString().replace(/[:.]/g, "-");
      const dst = path.join(dir, `${base}.${ts}.jsonl`);
      fs.renameSync(filePath, dst);
      const olds = fs.readdirSync(dir)
        .filter((n) => n.startsWith(`${base}.`) && n.endsWith(".jsonl"))
        .map((n) => ({ n, p: path.join(dir, n), t: fs.statSync(path.join(dir, n)).mtimeMs }))
        .sort((a, b) => b.t - a.t);
      for (let i = keep; i < olds.length; i += 1) {
        try { fs.unlinkSync(olds[i].p); } catch {}
      }
    } catch {}
  }

  function loadConfig() {
    try {
      const p = configPath();
      if (!fs.existsSync(p)) {
        return { mode: "offline_local", baseUrl: "http://127.0.0.1:18080", apiKey: "" };
      }
      return JSON.parse(fs.readFileSync(p, "utf8"));
    } catch {
      return { mode: "offline_local", baseUrl: "http://127.0.0.1:18080", apiKey: "" };
    }
  }

  function saveConfig(cfg) {
    fs.mkdirSync(path.dirname(configPath()), { recursive: true });
    fs.writeFileSync(configPath(), JSON.stringify(cfg, null, 2), "utf8");
  }

  return {
    configPath,
    routeMetricsLogPath,
    routeMetricsSummaryPath,
    rotateLogIfNeeded,
    loadConfig,
    saveConfig,
  };
}

module.exports = { createConfigSupport };
