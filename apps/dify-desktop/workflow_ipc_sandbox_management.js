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
  } = deps;

  ipcMain.handle("aiwf:getWorkflowSandboxAlerts", async (_evt, req) => {
    const limit = Number(req?.limit || 400);
    const dedupWindowSec = Number.isFinite(Number(req?.dedup_window_sec))
      ? Math.max(0, Math.floor(Number(req?.dedup_window_sec)))
      : sandboxSupport.sandboxAlertDedupWindowSec(req || {});
    return sandboxSupport.sandboxAlerts(limit, req?.thresholds || null, dedupWindowSec);
  });

  ipcMain.handle("aiwf:getWorkflowSandboxAlertRules", async () => {
    return { ok: true, rules: sandboxSupport.loadSandboxAlertRules() };
  });

  ipcMain.handle("aiwf:setWorkflowSandboxAlertRules", async (_evt, req) => {
    try {
      const incoming = req?.rules && typeof req.rules === "object" ? req.rules : {};
      const next = sandboxSupport.normalizeSandboxAlertRules(incoming);
      sandboxSupport.saveSandboxAlertRules(next);
      const ver = sandboxSupport.appendSandboxRuleVersion(next, { reason: "set_rules" });
      appendAudit("sandbox_alert_rules_set", {
        version_id: String(ver?.version_id || ""),
        whitelist_codes: next.whitelist_codes.length,
        whitelist_node_types: next.whitelist_node_types.length,
        whitelist_keys: next.whitelist_keys.length,
        mute_keys: Object.keys(next.mute_until_by_key || {}).length,
      });
      return { ok: true, rules: next };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:muteWorkflowSandboxAlert", async (_evt, req) => {
    try {
      const nodeType = String(req?.node_type || "*").trim().toLowerCase() || "*";
      const nodeId = String(req?.node_id || "*").trim().toLowerCase() || "*";
      const code = String(req?.code || "*").trim().toLowerCase() || "*";
      const minutes = Number(req?.minutes || 60);
      const mins = Number.isFinite(minutes) ? Math.max(1, Math.floor(minutes)) : 60;
      const key = `${nodeType}::${nodeId}::${code}`;
      const rules = sandboxSupport.loadSandboxAlertRules();
      rules.mute_until_by_key[key] = new Date(Date.now() + mins * 60000).toISOString();
      const next = sandboxSupport.normalizeSandboxAlertRules(rules);
      sandboxSupport.saveSandboxAlertRules(next);
      const ver = sandboxSupport.appendSandboxRuleVersion(next, { reason: "mute", key, minutes: mins });
      appendAudit("sandbox_alert_muted", { key, minutes: mins });
      return { ok: true, key, mute_until: next.mute_until_by_key[key], version_id: String(ver?.version_id || ""), rules: next };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:listWorkflowSandboxRuleVersions", async (_evt, req) => {
    const limit = Number(req?.limit || 100);
    return { ok: true, items: sandboxSupport.listSandboxRuleVersions(limit) };
  });

  ipcMain.handle("aiwf:compareWorkflowSandboxRuleVersions", async (_evt, req) => {
    return sandboxSupport.compareSandboxRuleVersions(String(req?.version_a || ""), String(req?.version_b || ""));
  });

  ipcMain.handle("aiwf:rollbackWorkflowSandboxRuleVersion", async (_evt, req) => {
    try {
      const versionId = String(req?.version_id || "").trim();
      if (!versionId) return { ok: false, error: "version_id required" };
      const hit = sandboxSupport.listSandboxRuleVersions(5000).find((item) => String(item.version_id || "") === versionId);
      if (!hit) return { ok: false, error: "rule version not found" };
      const rules = sandboxSupport.normalizeSandboxAlertRules(hit.rules || {});
      sandboxSupport.saveSandboxAlertRules(rules);
      const ver = sandboxSupport.appendSandboxRuleVersion(rules, { reason: "rollback", from_version_id: versionId });
      appendAudit("sandbox_alert_rules_rollback", { from_version_id: versionId, new_version_id: String(ver?.version_id || "") });
      return { ok: true, rules, version_id: String(ver?.version_id || "") };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:getWorkflowSandboxAutoFixState", async () => {
    return { ok: true, state: sandboxSupport.loadSandboxAutoFixState() };
  });

  ipcMain.handle("aiwf:listWorkflowSandboxAutoFixActions", async (_evt, req) => {
    const limit = Number(req?.limit || 100);
    const safe = Number.isFinite(limit) ? Math.max(1, Math.min(1000, Math.floor(limit))) : 100;
    const state = sandboxSupport.loadSandboxAutoFixState();
    const items = Array.isArray(state.last_actions) ? state.last_actions.slice().reverse().slice(0, safe) : [];
    return {
      ok: true,
      items,
      forced_isolation_mode: String(state.forced_isolation_mode || ""),
      forced_until: String(state.forced_until || ""),
    };
  });

  ipcMain.handle("aiwf:listWorkflowAuditLogs", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    const action = String(req?.action || "");
    return { ok: true, items: sandboxSupport.listAudit(limit, action) };
  });

  ipcMain.handle("aiwf:listWorkflowQualityGateReports", async (_evt, req) => {
    const limit = Number(req?.limit || 200);
    const filter = req?.filter && typeof req.filter === "object" ? req.filter : {};
    return { ok: true, items: sandboxSupport.listQualityGateReports(limit, filter) };
  });

  ipcMain.handle("aiwf:exportWorkflowQualityGateReports", async (_evt, req) => {
    try {
      const limit = Number(req?.limit || 300);
      const format = String(req?.format || "md").trim().toLowerCase() === "json" ? "json" : "md";
      const filter = req?.filter && typeof req.filter === "object" ? req.filter : {};
      const items = sandboxSupport.listQualityGateReports(limit, filter);
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
