function registerWorkflowSandboxManagementIpc(ctx, deps) {
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
    nowIso,
    appendAudit,
    sandboxSupport,
    sandboxRuleStore,
    sandboxAutoFixStore,
    workflowRunAuditStore,
  } = deps;

  ipcMain.handle("aiwf:getWorkflowSandboxAlerts", async (_evt, req) => {
    try {
      const rulesOut = await sandboxRuleStore.getRules();
      if (!rulesOut?.ok) return rulesOut;
      const limit = Number(req?.limit || 400);
      const dedupWindowSec = Number.isFinite(Number(req?.dedup_window_sec))
        ? Math.max(0, Math.floor(Number(req?.dedup_window_sec)))
        : sandboxSupport.sandboxAlertDedupWindowSec(req || {});
      return await sandboxSupport.sandboxAlerts(limit, req?.thresholds || null, dedupWindowSec, rulesOut.rules || {});
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:getWorkflowSandboxAlertRules", async () => {
    try {
      return await sandboxRuleStore.getRules();
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:setWorkflowSandboxAlertRules", async (_evt, req) => {
    try {
      const out = await sandboxRuleStore.saveRules(req);
      if (!out?.ok) return out;
      const next = sandboxSupport.normalizeSandboxAlertRules(out.rules || {});
      appendAudit("sandbox_alert_rules_set", {
        provider: String(out?.provider || ""),
        version_id: String(out?.version_id || ""),
        whitelist_codes: next.whitelist_codes.length,
        whitelist_node_types: next.whitelist_node_types.length,
        whitelist_keys: next.whitelist_keys.length,
        mute_keys: Object.keys(next.mute_until_by_key || {}).length,
      });
      return { ...out, rules: next };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:muteWorkflowSandboxAlert", async (_evt, req) => {
    try {
      const out = await sandboxRuleStore.muteAlert(req);
      if (!out?.ok) return out;
      appendAudit("sandbox_alert_muted", {
        key: String(out?.key || ""),
        minutes: Number(req?.minutes || 60),
        provider: String(out?.provider || ""),
      });
      return {
        ...out,
        rules: sandboxSupport.normalizeSandboxAlertRules(out.rules || {}),
      };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:listWorkflowSandboxRuleVersions", async (_evt, req) => {
    try {
      const limit = Number(req?.limit || 100);
      return await sandboxRuleStore.listVersions(limit);
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:compareWorkflowSandboxRuleVersions", async (_evt, req) => {
    try {
      return await sandboxRuleStore.compareVersions(String(req?.version_a || ""), String(req?.version_b || ""));
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:rollbackWorkflowSandboxRuleVersion", async (_evt, req) => {
    try {
      const versionId = String(req?.version_id || "").trim();
      if (!versionId) return { ok: false, error: "version_id required" };
      const out = await sandboxRuleStore.rollbackVersion(versionId);
      if (!out?.ok) return out;
      appendAudit("sandbox_alert_rules_rollback", {
        from_version_id: versionId,
        new_version_id: String(out?.version_id || ""),
        provider: String(out?.provider || ""),
      });
      return {
        ...out,
        rules: sandboxSupport.normalizeSandboxAlertRules(out.rules || {}),
      };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:getWorkflowSandboxAutoFixState", async () => {
    return await sandboxAutoFixStore.getState();
  });

  ipcMain.handle("aiwf:listWorkflowSandboxAutoFixActions", async (_evt, req) => {
    const limit = Number(req?.limit || 100);
    return await sandboxAutoFixStore.listActions(limit);
  });

  ipcMain.handle("aiwf:listWorkflowAuditLogs", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    const action = String(req?.action || "");
    return await workflowRunAuditStore.listAuditLogs(limit, action);
  });

  ipcMain.handle("aiwf:listWorkflowQualityGateReports", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    const filter = req?.filter && typeof req.filter === "object" ? req.filter : {};
    return await sandboxSupport.listQualityGateReports(limit, filter);
  });

  ipcMain.handle("aiwf:exportWorkflowQualityGateReports", async (_evt, req) => {
    try {
      const limit = Number(req?.limit || 300);
      const format = String(req?.format || "md").trim().toLowerCase() === "json" ? "json" : "md";
      const filter = req?.filter && typeof req.filter === "object" ? req.filter : {};
      const listed = await sandboxSupport.listQualityGateReports(limit, filter);
      if (!listed?.ok) return listed;
      const items = Array.isArray(listed.items) ? listed.items : [];
      const allowMockIo = isMockIoAllowed();
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        const safe = resolveMockFilePath(req.path);
        if (!safe.ok) return safe;
        filePath = safe.path;
      } else {
        const defaultName = `aiwf_quality_gate_${Date.now()}.${format}`;
        const pick = await dialog.showSaveDialog({
          title: "导出质量门禁报告",
          defaultPath: path.join(app.getPath("documents"), defaultName),
          filters: format === "json" ? [{ name: "JSON", extensions: ["json"] }] : [{ name: "Markdown", extensions: ["md"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      if (format === "json") {
        fs.writeFileSync(filePath, `${JSON.stringify({ exported_at: nowIso(), filter, total: items.length, items }, null, 2)}\n`, "utf8");
      } else {
        const lines = [];
        lines.push("# AIWF 质量门禁报告");
        lines.push("");
        lines.push(`- 导出时间: ${nowIso()}`);
        lines.push(`- 数量: ${items.length}`);
        lines.push(`- 筛选 run_id: ${String(filter?.run_id || "(none)")}`);
        lines.push(`- 筛选 status: ${String(filter?.status || "all")}`);
        lines.push("");
        lines.push("| Run | 状态 | 问题 | 时间 |");
        lines.push("|---|---|---|---|");
        items.forEach((item) => {
          const gate = item?.quality_gate && typeof item.quality_gate === "object" ? item.quality_gate : {};
          const status = gate.blocked ? "blocked" : (gate.passed ? "pass" : "unknown");
          const issues = Array.isArray(gate.issues) ? gate.issues.join(";") : "";
          lines.push(`| ${String(item.run_id || "").slice(0, 12)} | ${status} | ${issues} | ${String(item.ts || "")} |`);
        });
        fs.writeFileSync(filePath, `${lines.join("\n")}\n`, "utf8");
      }
      appendAudit("quality_gate_export", { path: filePath, format, total: items.length, filter });
      return { ok: true, path: filePath, total: items.length, format };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });
}

module.exports = {
  registerWorkflowSandboxManagementIpc,
};
