function registerWorkflowIpc(ctx) {
  const {
    app,
    ipcMain,
    dialog,
    fs,
    path,
    createWorkflowWindow,
    loadConfig,
    runMinimalWorkflow,
  } = ctx;

  function diagnosticsLogPath() {
    const p = path.join(app.getPath("userData"), "logs");
    fs.mkdirSync(p, { recursive: true });
    return path.join(p, "workflow_diagnostics.jsonl");
  }

  function appendDiagnostics(run) {
    try {
      const payload = {
        ts: new Date().toISOString(),
        run_id: run?.run_id || "",
        workflow_id: run?.workflow_id || "",
        ok: !!run?.ok,
        status: run?.status || "",
        diagnostics: run?.diagnostics || {},
      };
      fs.appendFileSync(diagnosticsLogPath(), `${JSON.stringify(payload)}\n`, "utf8");
    } catch {}
  }

  function readDiagnostics(limit = 50) {
    const fp = diagnosticsLogPath();
    if (!fs.existsSync(fp)) return { ok: true, items: [], by_chiplet: {} };
    const lines = fs.readFileSync(fp, "utf8").split(/\r?\n/).filter((x) => x.trim());
    const items = lines
      .slice(Math.max(0, lines.length - limit))
      .map((x) => {
        try {
          return JSON.parse(x);
        } catch {
          return null;
        }
      })
      .filter(Boolean);

    const by = {};
    for (const it of items) {
      const chiplets = it?.diagnostics?.chiplets || {};
      Object.keys(chiplets).forEach((k) => {
        const c = chiplets[k] || {};
        if (!by[k]) by[k] = { runs: 0, failed: 0, seconds_total: 0, attempts_total: 0 };
        by[k].runs += Number(c.runs || 0);
        by[k].failed += Number(c.failed || 0);
        by[k].seconds_total += Number(c.seconds_total || 0);
        by[k].attempts_total += Number(c.attempts_total || 0);
      });
    }

    Object.values(by).forEach((v) => {
      v.seconds_total = Number(v.seconds_total.toFixed(3));
      v.seconds_avg = v.runs > 0 ? Number((v.seconds_total / v.runs).toFixed(3)) : 0;
      v.failure_rate = v.runs > 0 ? Number((v.failed / v.runs).toFixed(4)) : 0;
    });

    return { ok: true, items, by_chiplet: by };
  }

  ipcMain.handle("aiwf:openWorkflowStudio", async () => {
    createWorkflowWindow();
    return { ok: true };
  });

  ipcMain.handle("aiwf:runWorkflow", async (_evt, payload, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    const out = await runMinimalWorkflow({
      payload: payload || {},
      config: merged,
      outputRoot: path.join(app.getPath("documents"), "AIWF-Offline"),
    });
    appendDiagnostics(out);
    return out;
  });

  ipcMain.handle("aiwf:getWorkflowDiagnostics", async (_evt, opts) => {
    const limit = Number(opts?.limit || 50);
    const safe = Number.isFinite(limit) ? Math.max(1, Math.min(500, Math.floor(limit))) : 50;
    return readDiagnostics(safe);
  });

  ipcMain.handle("aiwf:saveWorkflow", async (_evt, graph, name, opts) => {
    try {
      const options = opts && typeof opts === "object" ? opts : {};
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      const safeName = String(name || "workflow")
        .replace(/[\\/:*?"<>|]/g, "_")
        .replace(/\s+/g, "_")
        .slice(0, 80) || "workflow";
      let canceled = false;
      let filePath = "";
      if (options.mock && options.path && allowMockIo) {
        filePath = String(options.path);
      } else {
        const out = await dialog.showSaveDialog({
          title: "保存流程",
          defaultPath: path.join(app.getPath("documents"), `${safeName}.json`),
          filters: [{ name: "Workflow JSON", extensions: ["json"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        canceled = !!out.canceled;
        filePath = out.filePath || "";
      }
      if (canceled || !filePath) return { ok: false, canceled: true };
      const payload = graph && typeof graph === "object" ? graph : {};
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      return { ok: true, path: filePath };
    } catch (e) {
      return { ok: false, canceled: false, error: String(e) };
    }
  });

  ipcMain.handle("aiwf:loadWorkflow", async (_evt, opts) => {
    try {
      const options = opts && typeof opts === "object" ? opts : {};
      const allowMockIo = (!app.isPackaged) || String(process.env.AIWF_ENABLE_MOCK_IO || "").trim() === "1";
      let canceled = false;
      let filePaths = [];
      if (options.mock && options.path && allowMockIo) {
        filePaths = [String(options.path)];
      } else {
        const out = await dialog.showOpenDialog({
          title: "加载流程",
          filters: [{ name: "Workflow JSON", extensions: ["json"] }],
          properties: ["openFile"],
        });
        canceled = !!out.canceled;
        filePaths = out.filePaths || [];
      }
      if (canceled || !filePaths || !filePaths.length) return { ok: false, canceled: true };
      const p = filePaths[0];
      const obj = JSON.parse(fs.readFileSync(p, "utf8"));
      return { ok: true, canceled: false, path: p, graph: obj };
    } catch (e) {
      return { ok: false, canceled: false, error: String(e) };
    }
  });
}

module.exports = {
  registerWorkflowIpc,
};
