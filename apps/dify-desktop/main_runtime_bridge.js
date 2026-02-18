function createBridgeSupport({ path, fork }) {
  function headers(apiKey) {
    const h = { "Content-Type": "application/json" };
    if (apiKey && String(apiKey).trim()) h["X-API-Key"] = String(apiKey).trim();
    return h;
  }

  function runOfflineCleaningInWorker(payload, outRoot) {
    return new Promise((resolve, reject) => {
      const workerPath = path.join(__dirname, "offline_worker.js");
      const child = fork(workerPath, [], {
        stdio: ["ignore", "ignore", "ignore", "ipc"],
        windowsHide: true,
      });

      let settled = false;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        try { child.kill("SIGKILL"); } catch {}
        reject(new Error("offline cleaning timeout after 30 minutes"));
      }, 30 * 60 * 1000);

      child.on("message", (msg) => {
        if (settled) return;
        if (msg && msg.type === "result") {
          settled = true;
          clearTimeout(timer);
          resolve(msg.data || { ok: false, error: "empty worker result" });
        } else if (msg && msg.type === "error") {
          settled = true;
          clearTimeout(timer);
          reject(new Error(String(msg.error || "offline worker failed")));
        }
      });

      child.on("error", (e) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(e);
      });

      child.on("exit", (code) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(new Error(`offline worker exited unexpectedly: ${code}`));
      });

      child.send({ payload: { ...(payload || {}), output_root: outRoot } });
    });
  }

  async function runViaBaseApi(payload, cfg) {
    const base = String(cfg.baseUrl || "http://127.0.0.1:18080").replace(/\/$/, "");
    const resp = await fetch(`${base}/api/v1/integrations/dify/run_cleaning`, {
      method: "POST",
      headers: headers(cfg.apiKey),
      body: JSON.stringify(payload || {}),
    });
    const txt = await resp.text();
    let obj = {};
    try { obj = JSON.parse(txt || "{}"); } catch { obj = { ok: false, error: txt }; }
    if (!resp.ok) throw new Error(obj?.error || txt || `HTTP ${resp.status}`);
    return obj;
  }

  async function baseHealth(cfg) {
    const base = String(cfg.baseUrl || "http://127.0.0.1:18080").replace(/\/$/, "");
    const h = headers(cfg.apiKey);
    const a = await fetch(`${base}/actuator/health`, { headers: h });
    const b = await fetch(`${base}/api/v1/integrations/dify/health`, { headers: h });
    return {
      ok: a.ok && b.ok,
      actuator: a.ok ? await a.json() : { ok: false, status: a.status },
      dify_bridge: b.ok ? await b.json() : { ok: false, status: b.status },
    };
  }

  function parsePrometheusMetrics(text) {
    const out = {};
    String(text || "")
      .split(/\r?\n/)
      .forEach((line) => {
        const s = line.trim();
        if (!s || s.startsWith("#")) return;
        const idx = s.lastIndexOf(" ");
        if (idx <= 0) return;
        const k = s.slice(0, idx).trim();
        const v = Number(s.slice(idx + 1).trim());
        if (!Number.isNaN(v)) out[k] = v;
      });
    return out;
  }

  async function getTaskStoreStatus(cfg) {
    const accel = String(
      (cfg && cfg.accelUrl) || process.env.AIWF_ACCEL_URL || "http://127.0.0.1:18082"
    ).replace(/\/$/, "");
    const resp = await fetch(`${accel}/metrics`, { headers: headers(cfg && cfg.apiKey) });
    if (!resp.ok) {
      return { ok: false, enabled: false, healthy: false, error: `metrics http ${resp.status}` };
    }
    const txt = await resp.text();
    const m = parsePrometheusMetrics(txt);
    const enabled = Number(m.aiwf_task_store_remote_enabled || 0) === 1;
    const healthy = Number(m.aiwf_task_store_remote_ok || 0) === 1;
    const probeFailures = Number(m.aiwf_task_store_remote_probe_failures_total || 0);
    const lastProbeEpoch = Number(m.aiwf_task_store_remote_last_probe_epoch || 0);
    return { ok: true, enabled, healthy, probeFailures, lastProbeEpoch, accelUrl: accel };
  }

  return {
    runOfflineCleaningInWorker,
    runViaBaseApi,
    baseHealth,
    getTaskStoreStatus,
  };
}

module.exports = { createBridgeSupport };
