function registerWorkflowSandboxIoIpc(ctx, deps) {
  const {
    ipcMain,
    dialog,
    app,
    fs,
    path,
  } = ctx;
  const {
    isMockIoAllowed,
    resolveMockFilePath,
    sandboxAlertDedupWindowSec,
    sandboxAlerts,
    nowIso,
    appendAudit,
  } = deps;

  ipcMain.handle("aiwf:exportWorkflowSandboxAuditReport", async (_evt, req) => {
    try {
      const limit = Number(req?.limit || 500);
      const thresholds = req?.thresholds && typeof req.thresholds === "object" ? req.thresholds : {};
      const dedupWindowSec = Number.isFinite(Number(req?.dedup_window_sec))
        ? Math.max(0, Math.floor(Number(req?.dedup_window_sec)))
        : sandboxAlertDedupWindowSec(req || {});
      const format = String(req?.format || "md").trim().toLowerCase() === "json" ? "json" : "md";
      const data = sandboxAlerts(limit, thresholds, dedupWindowSec);
      const allowMockIo = isMockIoAllowed();
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        const safe = resolveMockFilePath(req.path);
        if (!safe.ok) return safe;
        filePath = safe.path;
      } else {
        const defaultName = `aiwf_sandbox_audit_${Date.now()}.${format}`;
        const pick = await dialog.showSaveDialog({
          title: "导出Sandbox审计报告",
          defaultPath: path.join(app.getPath("documents"), defaultName),
          filters: format === "json" ? [{ name: "JSON", extensions: ["json"] }] : [{ name: "Markdown", extensions: ["md"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      if (format === "json") {
        fs.writeFileSync(filePath, `${JSON.stringify({ exported_at: nowIso(), ...data }, null, 2)}\n`, "utf8");
      } else {
        const lines = [];
        lines.push("# AIWF Sandbox 审计报告");
        lines.push("");
        lines.push(`- 导出时间: ${nowIso()}`);
        lines.push(`- 告警总数: ${Number(data.total || 0)}`);
        lines.push(`- 健康等级: ${String(data?.health?.level || "green")}`);
        lines.push(`- 阈值: yellow=${Number(data?.health?.thresholds?.yellow || 1)}, red=${Number(data?.health?.thresholds?.red || 3)}`);
        lines.push("");
        lines.push("| 节点 | 次数 | 最近Run |");
        lines.push("|---|---:|---|");
        (Array.isArray(data.by_node) ? data.by_node : []).forEach((item) => {
          lines.push(`| ${String(item.node_type || "")}(${String(item.node_id || "")}) | ${Number(item.count || 0)} | ${String(item.last_run_id || "").slice(0, 12)} |`);
        });
        lines.push("");
        fs.writeFileSync(filePath, `${lines.join("\n")}\n`, "utf8");
      }
      appendAudit("sandbox_audit_export", { path: filePath, format, total: Number(data.total || 0) });
      return { ok: true, path: filePath, format, total: Number(data.total || 0), health: data.health };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:exportWorkflowSandboxPreset", async (_evt, req) => {
    try {
      const preset = req?.preset && typeof req.preset === "object" ? req.preset : {};
      const allowMockIo = isMockIoAllowed();
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        const safe = resolveMockFilePath(req.path);
        if (!safe.ok) return safe;
        filePath = safe.path;
      } else {
        const pick = await dialog.showSaveDialog({
          title: "导出Sandbox预设",
          defaultPath: path.join(app.getPath("documents"), `aiwf_sandbox_preset_${Date.now()}.json`),
          filters: [{ name: "JSON", extensions: ["json"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      const payload = { exported_at: nowIso(), preset };
      fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      appendAudit("sandbox_preset_export", { path: filePath });
      return { ok: true, path: filePath };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:importWorkflowSandboxPreset", async (_evt, req) => {
    try {
      const allowMockIo = isMockIoAllowed();
      let filePaths = [];
      if (req?.mock && req?.path && allowMockIo) {
        const safe = resolveMockFilePath(req.path);
        if (!safe.ok) return safe;
        filePaths = [safe.path];
      } else {
        const out = await dialog.showOpenDialog({
          title: "导入Sandbox预设",
          filters: [{ name: "JSON", extensions: ["json"] }],
          properties: ["openFile"],
        });
        if (out.canceled || !out.filePaths || !out.filePaths.length) return { ok: false, canceled: true };
        filePaths = out.filePaths;
      }
      const filePath = filePaths[0];
      const obj = JSON.parse(fs.readFileSync(filePath, "utf8"));
      const preset = obj?.preset && typeof obj.preset === "object" ? obj.preset : {};
      appendAudit("sandbox_preset_import", { path: filePath });
      return { ok: true, path: filePath, preset };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });
}

module.exports = {
  registerWorkflowSandboxIoIpc,
};
