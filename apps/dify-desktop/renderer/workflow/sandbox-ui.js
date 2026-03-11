function createWorkflowSandboxUi(els, deps = {}) {
  const {
    setStatus = () => {},
    sandboxThresholdsPayload = () => ({}),
    sandboxDedupWindowSec = () => 600,
    sandboxRulesPayloadFromUi = () => ({}),
    applySandboxRulesToUi = () => {},
    applySandboxPresetToUi = () => {},
    currentSandboxPresetPayload = () => ({}),
    applySandboxPresetPayload = () => {},
    renderSandboxRows = () => {},
    renderSandboxRuleVersionRows = () => {},
    renderSandboxAutoFixRows = () => {},
  } = deps;

  async function refreshSandboxAlerts() {
    const out = await window.aiwfDesktop.getWorkflowSandboxAlerts({
      limit: 500,
      thresholds: sandboxThresholdsPayload(),
      dedup_window_sec: sandboxDedupWindowSec(),
    });
    if (out?.rules) applySandboxRulesToUi(out.rules);
    renderSandboxRows(out || {});
  }

  async function exportSandboxAudit() {
    const format = String(els.sandboxExportFormat?.value || "md").trim() || "md";
    const out = await window.aiwfDesktop.exportWorkflowSandboxAuditReport({
      limit: 500,
      thresholds: sandboxThresholdsPayload(),
      dedup_window_sec: sandboxDedupWindowSec(),
      format,
    });
    if (!out?.ok) {
      if (!out?.canceled) setStatus(`导出 Sandbox 报告失败: ${out?.error || "unknown"}`, false);
      return;
    }
    setStatus(`Sandbox 报告已导出: ${out.path}`, true);
  }

  async function loadSandboxRules() {
    const out = await window.aiwfDesktop.getWorkflowSandboxAlertRules();
    if (!out?.ok) {
      setStatus(`加载 Sandbox 规则失败: ${out?.error || "unknown"}`, false);
      return;
    }
    applySandboxRulesToUi(out.rules || {});
    setStatus("Sandbox 规则已加载", true);
  }

  async function refreshSandboxRuleVersions() {
    const out = await window.aiwfDesktop.listWorkflowSandboxRuleVersions({ limit: 80 });
    renderSandboxRuleVersionRows(out?.items || []);
  }

  async function saveSandboxRules() {
    const out = await window.aiwfDesktop.setWorkflowSandboxAlertRules({
      rules: sandboxRulesPayloadFromUi(),
    });
    if (!out?.ok) {
      setStatus(`保存 Sandbox 规则失败: ${out?.error || "unknown"}`, false);
      return;
    }
    applySandboxRulesToUi(out.rules || {});
    setStatus("Sandbox 规则已保存", true);
    await refreshSandboxRuleVersions();
    await refreshSandboxAlerts();
  }

  async function applySandboxPreset() {
    const preset = String(els.sandboxPreset?.value || "balanced").trim().toLowerCase();
    applySandboxPresetToUi(preset);
    const out = await window.aiwfDesktop.setWorkflowSandboxAlertRules({
      rules: sandboxRulesPayloadFromUi(),
    });
    if (!out?.ok) {
      setStatus(`应用预设失败: ${out?.error || "unknown"}`, false);
      return;
    }
    applySandboxRulesToUi(out.rules || {});
    setStatus(`已应用 Sandbox 预设: ${preset}`, true);
    await refreshSandboxRuleVersions();
    await refreshSandboxAlerts();
  }

  async function applySandboxMute() {
    const out = await window.aiwfDesktop.muteWorkflowSandboxAlert({
      node_type: String(els.sandboxMuteNodeType?.value || "*").trim() || "*",
      node_id: String(els.sandboxMuteNodeId?.value || "*").trim() || "*",
      code: String(els.sandboxMuteCode?.value || "*").trim() || "*",
      minutes: Number(els.sandboxMuteMinutes?.value || 60),
    });
    if (!out?.ok) {
      setStatus(`应用静默失败: ${out?.error || "unknown"}`, false);
      return;
    }
    setStatus(`已静默: ${out.key} 到 ${out.mute_until}`, true);
    await refreshSandboxRuleVersions();
    await refreshSandboxAlerts();
  }

  async function exportSandboxPreset() {
    const out = await window.aiwfDesktop.exportWorkflowSandboxPreset({
      preset: currentSandboxPresetPayload(),
    });
    if (!out?.ok) {
      if (!out?.canceled) setStatus(`导出预设失败: ${out?.error || "unknown"}`, false);
      return;
    }
    setStatus(`已导出预设: ${out.path}`, true);
  }

  async function importSandboxPreset() {
    const out = await window.aiwfDesktop.importWorkflowSandboxPreset({});
    if (!out?.ok) {
      if (!out?.canceled) setStatus(`导入预设失败: ${out?.error || "unknown"}`, false);
      return;
    }
    applySandboxPresetPayload(out.preset || {});
    await saveSandboxRules();
    await refreshSandboxAlerts();
    setStatus(`已导入预设: ${out.path}`, true);
  }

  async function refreshSandboxAutoFixLog() {
    const out = await window.aiwfDesktop.listWorkflowSandboxAutoFixActions({ limit: 120 });
    renderSandboxAutoFixRows(out?.items || []);
  }

  return {
    refreshSandboxAlerts,
    exportSandboxAudit,
    loadSandboxRules,
    refreshSandboxRuleVersions,
    saveSandboxRules,
    applySandboxPreset,
    applySandboxMute,
    exportSandboxPreset,
    importSandboxPreset,
    refreshSandboxAutoFixLog,
  };
}

export { createWorkflowSandboxUi };
