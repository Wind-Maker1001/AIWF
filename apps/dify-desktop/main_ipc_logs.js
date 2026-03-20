function createMainIpcLogSupport(ctx) {
  const {
    app,
    fs,
    path,
    runModeAuditLogPath,
  } = ctx;

  function appendRunModeAudit(entry) {
    try {
      if (!fs || typeof fs.mkdirSync !== "function" || typeof fs.appendFileSync !== "function") return;
      const filePath = typeof runModeAuditLogPath === "function" ? runModeAuditLogPath() : "";
      if (!filePath) return;
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.appendFileSync(filePath, `${JSON.stringify(entry)}\n`, "utf8");
    } catch {}
  }

  function localGateAuditLogPath() {
    return path.join(app.getPath("userData"), "logs", "local_gate_checks.jsonl");
  }

  function pruneJsonlLog(filePath, opts = {}) {
    try {
      if (!fs.existsSync(filePath)) return;
      const maxLines = Math.max(200, Number(opts.maxLines || process.env.AIWF_LOCAL_GATE_LOG_MAX_LINES || 2000));
      const keepDays = Math.max(1, Number(opts.keepDays || process.env.AIWF_LOCAL_GATE_LOG_KEEP_DAYS || 14));
      const cutoff = Date.now() - keepDays * 24 * 3600 * 1000;
      const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/).filter(Boolean);
      const kept = [];
      for (const line of lines) {
        try {
          const obj = JSON.parse(line);
          const ts = Date.parse(String(obj?.ts || ""));
          if (Number.isFinite(ts) && ts < cutoff) continue;
          kept.push(line);
        } catch {
          kept.push(line);
        }
      }
      const tail = kept.slice(-maxLines);
      fs.writeFileSync(filePath, `${tail.join("\n")}${tail.length ? "\n" : ""}`, "utf8");
    } catch {}
  }

  function appendLocalGateAudit(entry) {
    try {
      const filePath = localGateAuditLogPath();
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.appendFileSync(filePath, `${JSON.stringify(entry)}\n`, "utf8");
      pruneJsonlLog(filePath, {});
    } catch {}
  }

  function readJsonlTail(filePath, limit = 50) {
    try {
      if (!fs.existsSync(filePath)) return [];
      const txt = fs.readFileSync(filePath, "utf8");
      const lines = txt.split(/\r?\n/).filter(Boolean);
      return lines
        .slice(-Math.max(1, Number(limit || 50)))
        .map((line) => {
          try { return JSON.parse(line); } catch { return null; }
        })
        .filter(Boolean);
    } catch {
      return [];
    }
  }

  function hasRequiredGatePasses(limit = 200) {
    const required = ["test:unit", "smoke", "test:regression", "test:regression:dirty", "test:office-gate"];
    const items = readJsonlTail(localGateAuditLogPath(), Math.max(50, Number(limit || 200)));
    const latestBy = {};
    items.forEach((item) => {
      const script = String(item?.script || "");
      if (!required.includes(script)) return;
      latestBy[script] = item;
    });
    const missing = required.filter((script) => !latestBy[script] || !latestBy[script].ok);
    return { ok: missing.length === 0, missing, latestBy };
  }

  function localBuildAuditLogPath() {
    return path.join(app.getPath("userData"), "logs", "local_build_runs.jsonl");
  }

  function appendLocalBuildAudit(entry) {
    try {
      const filePath = localBuildAuditLogPath();
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.appendFileSync(filePath, `${JSON.stringify(entry)}\n`, "utf8");
      pruneJsonlLog(filePath, { keepDays: 30, maxLines: 1000 });
    } catch {}
  }

  function classifyRemoteFailure(err, remoteResult) {
    if (remoteResult && remoteResult.ok === false) return "remote_not_ok";
    const msg = String(err && err.message ? err.message : err || "").toLowerCase();
    if (!msg) return "unknown";
    if (msg.includes("timeout")) return "timeout";
    if (msg.includes("fetch failed") || msg.includes("unable to connect") || msg.includes("network")) return "network";
    if (msg.includes("http_5") || msg.includes("5xx") || msg.includes("gateway") || msg.includes("service unavailable")) return "server_5xx";
    if (msg.includes("http_4") || msg.includes("input_invalid") || msg.includes("policy_blocked") || msg.includes("unauthorized") || msg.includes("forbidden")) return "client_4xx";
    return "unknown";
  }

  function shouldFallbackByPolicy(policyRaw, failureClass) {
    const policy = String(policyRaw || "smart").trim().toLowerCase();
    if (policy === "always") return true;
    if (policy === "never") return false;
    if (policy === "smart_strict") {
      return failureClass === "timeout" || failureClass === "network" || failureClass === "server_5xx";
    }
    if (failureClass === "client_4xx") return false;
    return true;
  }

  return {
    appendRunModeAudit,
    appendLocalGateAudit,
    appendLocalBuildAudit,
    classifyRemoteFailure,
    hasRequiredGatePasses,
    localBuildAuditLogPath,
    localGateAuditLogPath,
    pruneJsonlLog,
    readJsonlTail,
    shouldFallbackByPolicy,
  };
}

module.exports = {
  createMainIpcLogSupport,
};
