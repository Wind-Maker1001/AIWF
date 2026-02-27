const { registerMetricsIpc } = require("./main_ipc_metrics");
const { registerWorkflowIpc } = require("./main_ipc_workflow");
const { registerRuntimeIpc } = require("./main_ipc_runtime");
const { spawn } = require("child_process");
const os = require("os");

function registerIpcHandlers(ctx) {
  const {
    app,
    ipcMain,
    shell,
    loadConfig,
    saveConfig,
    baseHealth,
    runOfflineCleaningInWorker,
    runOfflinePrecheckInWorker,
    runOfflinePreviewInWorker,
    runViaBaseApi,
    listCleaningTemplates,
    path,
    fs,
    runModeAuditLogPath,
  } = ctx;

  function appendRunModeAudit(entry) {
    try {
      if (!fs || typeof fs.mkdirSync !== "function" || typeof fs.appendFileSync !== "function") return;
      const p = typeof runModeAuditLogPath === "function" ? runModeAuditLogPath() : "";
      if (!p) return;
      fs.mkdirSync(path.dirname(p), { recursive: true });
      fs.appendFileSync(p, `${JSON.stringify(entry)}\n`, "utf8");
    } catch {}
  }

  function localGateAuditLogPath() {
    return path.join(app.getPath("userData"), "logs", "local_gate_checks.jsonl");
  }

  function appendLocalGateAudit(entry) {
    try {
      const p = localGateAuditLogPath();
      fs.mkdirSync(path.dirname(p), { recursive: true });
      fs.appendFileSync(p, `${JSON.stringify(entry)}\n`, "utf8");
      pruneJsonlLog(p, {});
    } catch {}
  }

  function readJsonlTail(filePath, limit = 50) {
    try {
      if (!fs.existsSync(filePath)) return [];
      const txt = fs.readFileSync(filePath, "utf8");
      const lines = txt.split(/\r?\n/).filter(Boolean);
      return lines.slice(-Math.max(1, Number(limit || 50))).map((ln) => {
        try { return JSON.parse(ln); } catch { return null; }
      }).filter(Boolean);
    } catch {
      return [];
    }
  }
  function pruneJsonlLog(filePath, opts = {}) {
    try {
      if (!fs.existsSync(filePath)) return;
      const maxLines = Math.max(200, Number(opts.maxLines || process.env.AIWF_LOCAL_GATE_LOG_MAX_LINES || 2000));
      const keepDays = Math.max(1, Number(opts.keepDays || process.env.AIWF_LOCAL_GATE_LOG_KEEP_DAYS || 14));
      const cutoff = Date.now() - keepDays * 24 * 3600 * 1000;
      const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/).filter(Boolean);
      const kept = [];
      for (const ln of lines) {
        try {
          const obj = JSON.parse(ln);
          const ts = Date.parse(String(obj?.ts || ""));
          if (Number.isFinite(ts) && ts < cutoff) continue;
          kept.push(ln);
        } catch {
          kept.push(ln);
        }
      }
      const tail = kept.slice(-maxLines);
      fs.writeFileSync(filePath, `${tail.join("\n")}${tail.length ? "\n" : ""}`, "utf8");
    } catch {}
  }
  function hasRequiredGatePasses(limit = 200) {
    const required = ["test:unit", "smoke", "test:regression", "test:regression:dirty", "test:office-gate"];
    const items = readJsonlTail(localGateAuditLogPath(), Math.max(50, Number(limit || 200)));
    const latestBy = {};
    items.forEach((it) => {
      const s = String(it?.script || "");
      if (!required.includes(s)) return;
      latestBy[s] = latestBy[s] || it;
    });
    const missing = required.filter((s) => !latestBy[s] || !latestBy[s].ok);
    return { ok: missing.length === 0, missing, latestBy };
  }
  function localBuildAuditLogPath() {
    return path.join(app.getPath("userData"), "logs", "local_build_runs.jsonl");
  }
  function appendLocalBuildAudit(entry) {
    try {
      const p = localBuildAuditLogPath();
      fs.mkdirSync(path.dirname(p), { recursive: true });
      fs.appendFileSync(p, `${JSON.stringify(entry)}\n`, "utf8");
      pruneJsonlLog(p, { keepDays: 30, maxLines: 1000 });
    } catch {}
  }
  function resolveOutputRoot(cfg = null) {
    const c = cfg && typeof cfg === "object" ? cfg : loadConfig();
    const fromCfg = String(c?.outputRoot || "").trim();
    if (fromCfg) return fromCfg;
    const eDesktop = "E:\\Desktop_Real";
    if (fs.existsSync(eDesktop)) return path.join(eDesktop, "AIWF");
    return path.join(app.getPath("desktop"), "AIWF_Builds");
  }
  function normalizeAbsPath(p) {
    return path.resolve(String(p || "").trim());
  }
  function isPathWithin(childPath, rootPath) {
    try {
      const child = normalizeAbsPath(childPath).toLowerCase();
      const root = normalizeAbsPath(rootPath).toLowerCase();
      return child === root || child.startsWith(`${root}${path.sep.toLowerCase()}`);
    } catch {
      return false;
    }
  }
  function trustedRoots(cfg = null) {
    const merged = cfg && typeof cfg === "object" ? cfg : loadConfig();
    return [
      resolveOutputRoot(merged),
      app.getPath("documents"),
      app.getPath("desktop"),
      app.getPath("userData"),
      os.tmpdir(),
    ];
  }
  function isTrustedPath(p, cfg = null) {
    const raw = String(p || "").trim();
    if (!raw) return false;
    if (!path.isAbsolute(raw)) return false;
    const roots = trustedRoots(cfg);
    return roots.some((r) => isPathWithin(raw, r));
  }
  function buildDesktopOutputDir(cfg = null) {
    return resolveOutputRoot(cfg);
  }
  function buildSamplePoolDir(cfg = null) {
    const c = cfg && typeof cfg === "object" ? cfg : loadConfig();
    const fromCfg = String(c?.samplePoolDir || "").trim();
    if (fromCfg) return fromCfg;
    return path.join(resolveOutputRoot(c), "sample_pool");
  }
  function listSamplePoolFiles(cfg = null) {
    const dir = buildSamplePoolDir(cfg);
    const exts = new Set([".pdf", ".docx", ".txt", ".png", ".jpg", ".jpeg", ".xlsx", ".csv"]);
    try {
      if (!fs.existsSync(dir)) return { ok: true, root: dir, files: [] };
      const files = fs.readdirSync(dir, { withFileTypes: true })
        .filter((it) => it.isFile() && exts.has(path.extname(String(it.name || "")).toLowerCase()))
        .map((it) => {
          const p = path.join(dir, it.name);
          let size = 0;
          let mtime = 0;
          try {
            const st = fs.statSync(p);
            size = Number(st.size || 0);
            mtime = Number(st.mtimeMs || 0);
          } catch {}
          return { name: String(it.name || ""), path: p, size, mtime };
        })
        .sort((a, b) => b.mtime - a.mtime);
      return { ok: true, root: dir, files };
    } catch (e) {
      return { ok: false, error: String(e), root: dir, files: [] };
    }
  }
  function addSamplePoolFiles(paths = [], cfg = null) {
    const dir = buildSamplePoolDir(cfg);
    fs.mkdirSync(dir, { recursive: true });
    const copied = [];
    const skipped = [];
    const bad = [];
    const arr = Array.isArray(paths) ? paths : [];
    const exts = new Set([".pdf", ".docx", ".txt", ".png", ".jpg", ".jpeg", ".xlsx", ".csv"]);
    arr.forEach((p0) => {
      try {
        const p = String(p0 || "").trim();
        if (!p || !fs.existsSync(p) || !fs.statSync(p).isFile()) { bad.push(p); return; }
        const ext = path.extname(p).toLowerCase();
        if (!exts.has(ext)) { skipped.push(p); return; }
        const base = path.basename(p);
        let dst = path.join(dir, base);
        if (fs.existsSync(dst)) {
          const srcSize = Number(fs.statSync(p).size || 0);
          const dstSize = Number(fs.statSync(dst).size || -1);
          if (srcSize === dstSize) { skipped.push(p); return; }
          const stem = path.basename(base, ext);
          dst = path.join(dir, `${stem}_${Date.now()}${ext}`);
        }
        fs.copyFileSync(p, dst);
        copied.push(dst);
      } catch {
        bad.push(String(p0 || ""));
      }
    });
    return { ok: true, root: dir, copied, skipped, bad };
  }
  function clearSamplePool(cfg = null) {
    const dir = buildSamplePoolDir(cfg);
    let removed = 0;
    try {
      if (!fs.existsSync(dir)) return { ok: true, root: dir, removed };
      fs.readdirSync(dir, { withFileTypes: true }).forEach((it) => {
        if (!it.isFile()) return;
        const p = path.join(dir, it.name);
        try { fs.unlinkSync(p); removed += 1; } catch {}
      });
      return { ok: true, root: dir, removed };
    } catch (e) {
      return { ok: false, error: String(e), root: dir, removed };
    }
  }
  function getLatestArtifactsDir(cfg = null) {
    const root = buildDesktopOutputDir(cfg);
    try {
      if (!fs.existsSync(root)) return { ok: false, error: "desktop_build_dir_not_found", root };
      const dirs = fs.readdirSync(root, { withFileTypes: true })
        .filter((it) => it.isDirectory() && /^[a-f0-9]{32}$/i.test(String(it.name || "")))
        .map((it) => {
          const p = path.join(root, it.name, "artifacts");
          let m = 0;
          try {
            const st = fs.statSync(p);
            m = Number(st.mtimeMs || 0);
          } catch {}
          return { id: String(it.name || ""), artifacts_dir: p, mtime: m };
        })
        .filter((x) => x.mtime > 0)
        .sort((a, b) => b.mtime - a.mtime);
      if (!dirs.length) return { ok: false, error: "no_artifacts_job_found", root };
      return { ok: true, root, ...dirs[0] };
    } catch (e) {
      return { ok: false, error: String(e), root };
    }
  }
  function checkDesktopBuildArtifacts(options = {}, cfg = null) {
    const recentMs = Number(options.recentMs || process.env.AIWF_BUILD_ARTIFACT_RECENT_MS || 7_200_000);
    const now = Date.now();
    const buildDir = buildDesktopOutputDir(cfg);
    const reportsDir = path.join(buildDir, "reports");
    const files = collectExeFiles(buildDir, []);
    const exes = files
      .map((p) => ({ path: p, mtime: Number(fs.statSync(p).mtimeMs || 0) }))
      .sort((a, b) => b.mtime - a.mtime);
    const reportFiles = [];
    try {
      if (fs.existsSync(reportsDir)) {
        fs.readdirSync(reportsDir, { withFileTypes: true }).forEach((it) => {
          if (!it.isFile()) return;
          const p = path.join(reportsDir, it.name);
          reportFiles.push({ path: p, name: String(it.name || ""), mtime: Number(fs.statSync(p).mtimeMs || 0) });
        });
      }
    } catch {}
    const mdReports = reportFiles.filter((x) => /^release_report_.*\.md$/i.test(x.name)).sort((a, b) => b.mtime - a.mtime);
    const jsonReports = reportFiles.filter((x) => /^release_report_.*\.json$/i.test(x.name)).sort((a, b) => b.mtime - a.mtime);
    const recent = (x) => !!x && (now - Number(x.mtime || 0) <= recentMs);
    const missing = [];
    if (!recent(exes[0])) missing.push("desktop_exe_recent");
    if (!recent(mdReports[0])) missing.push("release_report_md_recent");
    if (!recent(jsonReports[0])) missing.push("release_report_json_recent");
    return {
      ok: missing.length === 0,
      missing,
      recent_ms: recentMs,
      desktop_dir: buildDir,
      reports_dir: reportsDir,
      latest_exe: exes[0] ? String(exes[0].path || "") : "",
      latest_md_report: mdReports[0] ? String(mdReports[0].path || "") : "",
      latest_json_report: jsonReports[0] ? String(jsonReports[0].path || "") : "",
    };
  }
  function collectExeFiles(rootDir, out = []) {
    try {
      if (!fs.existsSync(rootDir)) return out;
      const items = fs.readdirSync(rootDir, { withFileTypes: true });
      items.forEach((it) => {
        const p = path.join(rootDir, it.name);
        if (it.isDirectory()) collectExeFiles(p, out);
        else if (/\.exe$/i.test(it.name)) out.push(p);
      });
    } catch {}
    return out;
  }
  function copyRecentBuildArtifactsToDesktop(sinceMs = 0, cfg = null) {
    const desktopDir = buildDesktopOutputDir(cfg);
    fs.mkdirSync(desktopDir, { recursive: true });
    const distDir = path.join(__dirname, "dist");
    const files = collectExeFiles(distDir, []);
    const picked = files
      .map((p) => ({ p, mtime: Number(fs.statSync(p).mtimeMs || 0) }))
      .filter((x) => x.mtime >= Math.max(0, Number(sinceMs || 0) - 10_000))
      .sort((a, b) => b.mtime - a.mtime)
      .slice(0, 8);
    const copied = [];
    picked.forEach((x) => {
      try {
        const bn = path.basename(x.p);
        const dst = path.join(desktopDir, bn);
        fs.copyFileSync(x.p, dst);
        copied.push(dst);
      } catch {}
    });
    return { desktop_dir: desktopDir, copied_files: copied };
  }
  function buildReleaseReportMarkdown(data) {
    const lines = [];
    lines.push("# AIWF 发布报告");
    lines.push("");
    lines.push(`- 生成时间: ${new Date().toISOString()}`);
    lines.push(`- 门禁状态: ${data?.gate_ok ? "通过" : "未通过"}`);
    const miss = Array.isArray(data?.missing) ? data.missing : [];
    lines.push(`- 缺失项: ${miss.length ? miss.join(", ") : "无"}`);
    lines.push("");
    lines.push("## 最近门禁记录");
    (Array.isArray(data?.gate_items) ? data.gate_items : []).slice(0, 20).forEach((it, i) => {
      lines.push(`${i + 1}. ${String(it.ts || "")} | ${String(it.script || "")} | ${it.ok ? "通过" : (it.canceled ? "已取消" : "失败")} | ${Number(it.seconds || 0).toFixed(2)}s`);
    });
    lines.push("");
    lines.push("## 最近打包记录");
    (Array.isArray(data?.build_runs) ? data.build_runs : []).slice(0, 20).forEach((it, i) => {
      lines.push(`${i + 1}. ${String(it.ts || "")} | ${String(it.script || "")} | ${it.ok ? "成功" : "失败"} | ${Number(it.seconds || 0).toFixed(2)}s`);
    });
    lines.push("");
    return `\uFEFF${lines.join("\n")}\n`;
  }
  const gateRunningByWc = new Map();
  const buildRunningByWc = new Map();
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
    const p = String(policyRaw || "smart").trim().toLowerCase();
    if (p === "always") return true;
    if (p === "never") return false;
    if (p === "smart_strict") {
      return failureClass === "timeout" || failureClass === "network" || failureClass === "server_5xx";
    }
    // smart(default): fallback for transport/server issues and remote not-ok.
    if (failureClass === "client_4xx") return false;
    return true;
  }

  ipcMain.handle("aiwf:getConfig", async () => loadConfig());
  ipcMain.handle("aiwf:saveConfig", async (_evt, cfg) => {
    saveConfig(cfg || {});
    return { ok: true };
  });

  ipcMain.handle("aiwf:health", async (_evt, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    if ((merged.mode || "offline_local") === "offline_local") {
      return { ok: true, mode: "offline_local", message: "离线本地模式可用" };
    }
    return await baseHealth(merged);
  });

  ipcMain.handle("aiwf:runCleaning", async (_evt, payload, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    const outRoot = resolveOutputRoot(merged);
    const startedAt = Date.now();
    if ((merged.mode || "offline_local") === "offline_local") {
      const local = await runOfflineCleaningInWorker(payload, outRoot);
      appendRunModeAudit({
        ts: new Date().toISOString(),
        mode: "offline_local",
        fallback_applied: false,
        ok: !!(local && local.ok),
        job_id: local && local.job_id ? String(local.job_id) : "",
        duration_ms: Date.now() - startedAt,
      });
      return local;
    }
    const allowFallback = merged.enableOfflineFallback !== false;
    const fallbackPolicy = String(merged.fallbackPolicy || "smart").trim().toLowerCase() || "smart";
    try {
      const remote = await runViaBaseApi(payload, merged);
      if (remote && remote.ok) {
        appendRunModeAudit({
          ts: new Date().toISOString(),
          mode: "base_api",
          fallback_applied: false,
          ok: true,
          job_id: remote && remote.job_id ? String(remote.job_id) : "",
          duration_ms: Date.now() - startedAt,
        });
        return remote;
      }
      const fclass = classifyRemoteFailure(null, remote);
      if (!allowFallback || !shouldFallbackByPolicy(fallbackPolicy, fclass)) {
        appendRunModeAudit({
          ts: new Date().toISOString(),
          mode: "base_api",
          fallback_applied: false,
          ok: !!(remote && remote.ok),
          reason: "remote_returned_not_ok",
          failure_class: fclass,
          fallback_policy: fallbackPolicy,
          job_id: remote && remote.job_id ? String(remote.job_id) : "",
          duration_ms: Date.now() - startedAt,
        });
        return remote;
      }
      const local = await runOfflineCleaningInWorker(payload, outRoot);
      const out = {
        ...(local || {}),
        fallback_applied: true,
        fallback_reason: "remote_returned_not_ok",
        fallback_policy: fallbackPolicy,
        failure_class: fclass,
        fallback_message: "后端返回失败，已自动切换到离线本地模式完成清洗。",
        remote_result: remote || null,
      };
      appendRunModeAudit({
        ts: new Date().toISOString(),
        mode: "base_api",
        fallback_applied: true,
        ok: !!(out && out.ok),
        reason: "remote_returned_not_ok",
        failure_class: fclass,
        fallback_policy: fallbackPolicy,
        job_id: out && out.job_id ? String(out.job_id) : "",
        duration_ms: Date.now() - startedAt,
      });
      return out;
    } catch (e) {
      const fclass = classifyRemoteFailure(e, null);
      if (!allowFallback || !shouldFallbackByPolicy(fallbackPolicy, fclass)) throw e;
      const local = await runOfflineCleaningInWorker(payload, outRoot);
      const out = {
        ...(local || {}),
        fallback_applied: true,
        fallback_reason: "remote_request_failed",
        fallback_policy: fallbackPolicy,
        failure_class: fclass,
        fallback_message: "后端不可用，已自动切换到离线本地模式完成清洗。",
        remote_error: String(e && e.message ? e.message : e),
      };
      appendRunModeAudit({
        ts: new Date().toISOString(),
        mode: "base_api",
        fallback_applied: true,
        ok: !!(out && out.ok),
        reason: "remote_request_failed",
        failure_class: fclass,
        fallback_policy: fallbackPolicy,
        remote_error: String(e && e.message ? e.message : e),
        job_id: out && out.job_id ? String(out.job_id) : "",
        duration_ms: Date.now() - startedAt,
      });
      return out;
    }
  });

  ipcMain.handle("aiwf:precheckCleaning", async (_evt, payload, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    if ((merged.mode || "offline_local") === "offline_local") {
      const outRoot = resolveOutputRoot(merged);
      return await runOfflinePrecheckInWorker(payload, outRoot);
    }
    return { ok: false, error: "当前仅离线本地模式支持模板预检" };
  });

  ipcMain.handle("aiwf:previewDebateStyle", async (_evt, payload, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    if ((merged.mode || "offline_local") === "offline_local") {
      const outRoot = resolveOutputRoot(merged);
      return await runOfflinePreviewInWorker(payload, outRoot);
    }
    return { ok: false, error: "当前仅离线本地模式支持样式预览" };
  });

  ipcMain.handle("aiwf:listCleaningTemplates", async () => {
    try {
      return listCleaningTemplates();
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:openPath", async (_evt, p) => {
    const candidate = String(p || "").trim();
    if (!candidate) return { ok: false, error: "path_required" };
    if (!isTrustedPath(candidate)) return { ok: false, error: "path_not_allowed" };
    await shell.openPath(candidate);
    return { ok: true };
  });
  ipcMain.handle("aiwf:getLatestArtifactsDir", async () => {
    return getLatestArtifactsDir();
  });
  ipcMain.handle("aiwf:getSamplePoolInfo", async (_evt, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    return listSamplePoolFiles(merged);
  });
  ipcMain.handle("aiwf:samplePoolAddFiles", async (_evt, paths, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    return addSamplePoolFiles(paths, merged);
  });
  ipcMain.handle("aiwf:samplePoolClear", async (_evt, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    return clearSamplePool(merged);
  });

  ipcMain.handle("aiwf:getLocalGateSummary", async (_evt, req) => {
    const limit = Number(req?.limit || 80);
    const items = readJsonlTail(localGateAuditLogPath(), Math.max(1, Math.min(500, limit)));
    const total = items.length;
    const passed = items.filter((x) => !!x?.ok).length;
    const failed = total - passed;
    const passRate = total > 0 ? Number((passed / total).toFixed(4)) : 0;
    return { ok: true, total, passed, failed, pass_rate: passRate, items: items.reverse() };
  });

  ipcMain.handle("aiwf:getLocalGateRuntime", async (evt) => {
    const wcId = Number(evt?.sender?.id || 0);
    const running = gateRunningByWc.get(wcId);
    if (!running) return { ok: true, running: false };
    return {
      ok: true,
      running: true,
      run_id: String(running.run_id || ""),
      script: String(running.script || ""),
      started_at: String(running.started_at || ""),
    };
  });

  ipcMain.handle("aiwf:cancelLocalGateCheck", async (evt) => {
    const wcId = Number(evt?.sender?.id || 0);
    const running = gateRunningByWc.get(wcId);
    if (!running || !running.child) return { ok: false, error: "no_running_gate_check" };
    try {
      running.canceled = true;
      running.child.kill();
      return { ok: true, canceled: true, run_id: String(running.run_id || "") };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:runLocalGateCheck", async (evt, req) => {
    const script = String(req?.script || "").trim();
    const allowed = new Set(["test:unit", "smoke", "test:regression", "test:regression:dirty", "test:office-gate", "acceptance:real"]);
    if (!allowed.has(script)) return { ok: false, error: `unsupported script: ${script}` };
    const wcId = Number(evt?.sender?.id || 0);
    if (gateRunningByWc.get(wcId)) {
      return { ok: false, error: "gate_check_already_running" };
    }
    const cwd = path.join(__dirname);
    const npmBin = process.platform === "win32" ? "npm.cmd" : "npm";
    const startedAt = Date.now();
    const runId = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const channel = "aiwf:localGateLog";
    const childEnv = { ...process.env };
    if (script === "acceptance:real") {
      const cfg = loadConfig();
      childEnv.AIWF_ACCEPTANCE_INPUT_DIR = buildSamplePoolDir(cfg);
      childEnv.AIWF_ACCEPTANCE_OUTPUT_ROOT = resolveOutputRoot(cfg);
    }

    return await new Promise((resolve) => {
      let out = "";
      let err = "";
      const child = spawn(npmBin, ["run", script], { cwd, windowsHide: true, env: childEnv });
      gateRunningByWc.set(wcId, { child, run_id: runId, script, started_at: new Date().toISOString(), canceled: false });
      const send = (type, text) => {
        try { evt.sender.send(channel, { run_id: runId, script, type, text: String(text || ""), ts: new Date().toISOString() }); } catch {}
      };
      send("start", `开始执行: npm run ${script}`);
      child.stdout.on("data", (buf) => {
        const s = String(buf || "");
        out += s;
        send("stdout", s);
      });
      child.stderr.on("data", (buf) => {
        const s = String(buf || "");
        err += s;
        send("stderr", s);
      });
      child.on("error", (e) => {
        const msg = String(e && e.message ? e.message : e);
        send("error", msg);
      });
      child.on("close", (code) => {
        const ok = Number(code || 0) === 0;
        const runtime = gateRunningByWc.get(wcId);
        const canceled = !!runtime?.canceled;
        gateRunningByWc.delete(wcId);
        const mix = `${out}\n${err}`;
        const m = mix.match(/(?:regression report|office gate report):\s*([A-Za-z]:\\[^\r\n]+\.json)/i);
        const reportPath = m ? String(m[1] || "").trim() : "";
        const item = {
          ts: new Date().toISOString(),
          run_id: runId,
          script,
          ok: canceled ? false : ok,
          canceled,
          exit_code: Number(code || 0),
          seconds: Number(((Date.now() - startedAt) / 1000).toFixed(3)),
          stdout_tail: out.slice(-1200),
          stderr_tail: err.slice(-1200),
          report_path: reportPath,
          report_dir: reportPath ? path.dirname(reportPath) : "",
        };
        appendLocalGateAudit(item);
        send("done", canceled ? `已取消: npm run ${script}` : `完成: npm run ${script} (exit=${item.exit_code})`);
        resolve(item);
      });
    });
  });

  ipcMain.handle("aiwf:getBuildGuardStatus", async () => {
    const gate = hasRequiredGatePasses(400);
    const runs = readJsonlTail(localBuildAuditLogPath(), 80);
    const artifact = checkDesktopBuildArtifacts();
    return {
      ok: true,
      gate_ok: !!gate.ok,
      missing: gate.missing || [],
      latest_gate: gate.latestBy || {},
      artifact_check: artifact,
      build_runs: runs.reverse(),
    };
  });

  ipcMain.handle("aiwf:getLocalBuildRuntime", async (evt) => {
    const wcId = Number(evt?.sender?.id || 0);
    const running = buildRunningByWc.get(wcId);
    if (!running) return { ok: true, running: false };
    return {
      ok: true,
      running: true,
      run_id: String(running.run_id || ""),
      script: String(running.script || ""),
      started_at: String(running.started_at || ""),
    };
  });

  ipcMain.handle("aiwf:cancelLocalBuildScript", async (evt) => {
    const wcId = Number(evt?.sender?.id || 0);
    const running = buildRunningByWc.get(wcId);
    if (!running || !running.child) return { ok: false, error: "no_running_build" };
    try {
      running.canceled = true;
      running.child.kill();
      return { ok: true, canceled: true, run_id: String(running.run_id || "") };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:runLocalBuildScript", async (evt, req) => {
    const script = String(req?.script || "").trim();
    const allowed = new Set(["build:win:release:gated", "build:win:installer:release:gated"]);
    if (!allowed.has(script)) return { ok: false, error: `unsupported build script: ${script}` };
    const wcId = Number(evt?.sender?.id || 0);
    if (buildRunningByWc.get(wcId)) return { ok: false, error: "build_already_running" };
    const gate = hasRequiredGatePasses(400);
    if (!gate.ok) return { ok: false, error: "release_gate_not_passed", missing: gate.missing };
    const cwd = path.join(__dirname);
    const npmBin = process.platform === "win32" ? "npm.cmd" : "npm";
    const startedAt = Date.now();
    const runId = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const channel = "aiwf:localBuildLog";
    return await new Promise((resolve) => {
      let out = "";
      let err = "";
      const child = spawn(npmBin, ["run", script], { cwd, windowsHide: true, env: process.env });
      buildRunningByWc.set(wcId, { child, run_id: runId, script, started_at: new Date().toISOString(), canceled: false });
      const send = (type, text) => {
        try { evt.sender.send(channel, { run_id: runId, script, type, text: String(text || ""), ts: new Date().toISOString() }); } catch {}
      };
      send("start", `开始执行: npm run ${script}`);
      child.stdout.on("data", (buf) => {
        const s = String(buf || "");
        out += s;
        send("stdout", s);
      });
      child.stderr.on("data", (buf) => {
        const s = String(buf || "");
        err += s;
        send("stderr", s);
      });
      child.on("error", (e) => send("error", String(e && e.message ? e.message : e)));
      child.on("close", (code) => {
        const ok = Number(code || 0) === 0;
        const runtime = buildRunningByWc.get(wcId);
        const canceled = !!runtime?.canceled;
        buildRunningByWc.delete(wcId);
        const copied = ok && !canceled ? copyRecentBuildArtifactsToDesktop(startedAt) : { desktop_dir: buildDesktopOutputDir(), copied_files: [] };
        const item = {
          ts: new Date().toISOString(),
          run_id: runId,
          script,
          ok: canceled ? false : ok,
          canceled,
          exit_code: Number(code || 0),
          seconds: Number(((Date.now() - startedAt) / 1000).toFixed(3)),
          stdout_tail: out.slice(-1200),
          stderr_tail: err.slice(-1200),
          desktop_dir: String(copied.desktop_dir || ""),
          copied_files: Array.isArray(copied.copied_files) ? copied.copied_files : [],
        };
        appendLocalBuildAudit(item);
        send("done", canceled ? `已取消: npm run ${script}` : `完成: npm run ${script} (exit=${item.exit_code})`);
        resolve(item);
      });
    });
  });

  ipcMain.handle("aiwf:exportReleaseReport", async (_evt, req) => {
    const format = String(req?.format || "md").trim().toLowerCase() === "json" ? "json" : "md";
    const gateItems = readJsonlTail(localGateAuditLogPath(), 200).reverse();
    const buildRuns = readJsonlTail(localBuildAuditLogPath(), 120).reverse();
    const gate = hasRequiredGatePasses(400);
    const payload = {
      generated_at: new Date().toISOString(),
      gate_ok: !!gate.ok,
      missing: gate.missing || [],
      latest_gate: gate.latestBy || {},
      gate_items: gateItems,
      build_runs: buildRuns,
    };
    const outDir = path.join(buildDesktopOutputDir(), "reports");
    fs.mkdirSync(outDir, { recursive: true });
    const fp = path.join(outDir, `release_report_${Date.now()}.${format}`);
    if (format === "json") {
      fs.writeFileSync(fp, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
    } else {
      fs.writeFileSync(fp, buildReleaseReportMarkdown(payload), "utf8");
    }
    return { ok: true, path: fp, format };
  });
  registerMetricsIpc(ctx);
  registerWorkflowIpc(ctx);
  registerRuntimeIpc(ctx);
}

module.exports = {
  registerIpcHandlers,
};







