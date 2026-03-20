const { spawn } = require("child_process");

function registerBuildGuardIpc(ctx, deps) {
  const {
    ipcMain,
    path,
    loadConfig,
  } = ctx;
  const {
    localGateAuditLogPath,
    readJsonlTail,
    hasRequiredGatePasses,
    buildSamplePoolDir,
    resolveOutputRoot,
    appendLocalGateAudit,
    localBuildAuditLogPath,
    appendLocalBuildAudit,
    checkDesktopBuildArtifacts,
    copyRecentBuildArtifactsToDesktop,
    buildDesktopOutputDir,
  } = deps;

  const gateRunningByWc = new Map();
  const buildRunningByWc = new Map();

  ipcMain.handle("aiwf:getLocalGateSummary", async (_evt, req) => {
    const limit = Number(req?.limit || 80);
    const items = readJsonlTail(localGateAuditLogPath(), Math.max(1, Math.min(500, limit)));
    const total = items.length;
    const passed = items.filter((item) => !!item?.ok).length;
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
    } catch (error) {
      return { ok: false, error: String(error) };
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
        const chunk = String(buf || "");
        out += chunk;
        send("stdout", chunk);
      });
      child.stderr.on("data", (buf) => {
        const chunk = String(buf || "");
        err += chunk;
        send("stderr", chunk);
      });
      child.on("error", (error) => {
        const msg = String(error && error.message ? error.message : error);
        send("error", msg);
      });
      child.on("close", (code) => {
        const ok = Number(code || 0) === 0;
        const runtime = gateRunningByWc.get(wcId);
        const canceled = !!runtime?.canceled;
        gateRunningByWc.delete(wcId);
        const mix = `${out}\n${err}`;
        const match = mix.match(/(?:regression report|office gate report):\s*([A-Za-z]:\\[^\r\n]+\.json)/i);
        const reportPath = match ? String(match[1] || "").trim() : "";
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
    } catch (error) {
      return { ok: false, error: String(error) };
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
        const chunk = String(buf || "");
        out += chunk;
        send("stdout", chunk);
      });
      child.stderr.on("data", (buf) => {
        const chunk = String(buf || "");
        err += chunk;
        send("stderr", chunk);
      });
      child.on("error", (error) => send("error", String(error && error.message ? error.message : error)));
      child.on("close", (code) => {
        const ok = Number(code || 0) === 0;
        const runtime = buildRunningByWc.get(wcId);
        const canceled = !!runtime?.canceled;
        buildRunningByWc.delete(wcId);
        const copied = ok && !canceled
          ? copyRecentBuildArtifactsToDesktop(startedAt)
          : { desktop_dir: buildDesktopOutputDir(), copied_files: [] };
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
    const filePath = path.join(outDir, `release_report_${Date.now()}.${format}`);
    if (format === "json") {
      fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
    } else {
      const lines = [];
      lines.push("# AIWF 发布报告");
      lines.push("");
      lines.push(`- 生成时间: ${new Date().toISOString()}`);
      lines.push(`- 门禁状态: ${payload.gate_ok ? "通过" : "未通过"}`);
      const missing = Array.isArray(payload.missing) ? payload.missing : [];
      lines.push(`- 缺失项: ${missing.length ? missing.join(", ") : "无"}`);
      lines.push("");
      lines.push("## 最近门禁记录");
      payload.gate_items.slice(0, 20).forEach((item, index) => {
        lines.push(`${index + 1}. ${String(item.ts || "")} | ${String(item.script || "")} | ${item.ok ? "通过" : (item.canceled ? "已取消" : "失败")} | ${Number(item.seconds || 0).toFixed(2)}s`);
      });
      lines.push("");
      lines.push("## 最近打包记录");
      payload.build_runs.slice(0, 20).forEach((item, index) => {
        lines.push(`${index + 1}. ${String(item.ts || "")} | ${String(item.script || "")} | ${item.ok ? "成功" : "失败"} | ${Number(item.seconds || 0).toFixed(2)}s`);
      });
      lines.push("");
      fs.writeFileSync(filePath, `\uFEFF${lines.join("\n")}\n`, "utf8");
    }
    return { ok: true, path: filePath, format };
  });
}

module.exports = {
  registerBuildGuardIpc,
};
