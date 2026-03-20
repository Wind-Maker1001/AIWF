function createWorkflowSupportSandbox(els, deps = {}) {
  function sandboxThresholdsPayload() {
    const yellowRaw = Number(els.sandboxThresholdYellow?.value || 1);
    const redRaw = Number(els.sandboxThresholdRed?.value || 3);
    const yellow = Number.isFinite(yellowRaw) ? Math.max(1, Math.floor(yellowRaw)) : 1;
    const red = Number.isFinite(redRaw) ? Math.max(yellow + 1, Math.floor(redRaw)) : Math.max(3, yellow + 1);
    return { yellow, red };
  }

  function sandboxDedupWindowSec() {
    const value = Number(els.sandboxDedupWindowSec?.value || 600);
    return Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 600;
  }

  function parseCsvList(text) {
    return String(text || "")
      .split(/[;,]/)
      .map((item) => String(item || "").trim().toLowerCase())
      .filter(Boolean);
  }

  const SANDBOX_RULE_PRESETS = {
    strict: {
      yellow: 1,
      red: 2,
      dedup_window_sec: 60,
      whitelist_codes: [],
      whitelist_node_types: [],
    },
    balanced: {
      yellow: 1,
      red: 3,
      dedup_window_sec: 600,
      whitelist_codes: [],
      whitelist_node_types: [],
    },
    loose: {
      yellow: 3,
      red: 8,
      dedup_window_sec: 1800,
      whitelist_codes: ["sandbox_limit_exceeded:output"],
      whitelist_node_types: [],
    },
  };

  function sandboxRulesPayloadFromUi() {
    return {
      whitelist_codes: parseCsvList(els.sandboxWhitelistCodes?.value || ""),
      whitelist_node_types: parseCsvList(els.sandboxWhitelistNodeTypes?.value || ""),
      whitelist_keys: [],
      mute_until_by_key: {},
    };
  }

  function applySandboxRulesToUi(rules) {
    const safeRules = rules && typeof rules === "object" ? rules : {};
    if (els.sandboxWhitelistCodes) {
      const codes = Array.isArray(safeRules.whitelist_codes) ? safeRules.whitelist_codes : [];
      els.sandboxWhitelistCodes.value = codes.join(",");
    }
    if (els.sandboxWhitelistNodeTypes) {
      const types = Array.isArray(safeRules.whitelist_node_types) ? safeRules.whitelist_node_types : [];
      els.sandboxWhitelistNodeTypes.value = types.join(",");
    }
  }

  function applySandboxPresetToUi(name) {
    const key = String(name || "balanced").trim().toLowerCase();
    const preset = SANDBOX_RULE_PRESETS[key] || SANDBOX_RULE_PRESETS.balanced;
    if (els.sandboxThresholdYellow) els.sandboxThresholdYellow.value = String(preset.yellow);
    if (els.sandboxThresholdRed) els.sandboxThresholdRed.value = String(preset.red);
    if (els.sandboxDedupWindowSec) els.sandboxDedupWindowSec.value = String(preset.dedup_window_sec);
    if (els.sandboxWhitelistCodes) els.sandboxWhitelistCodes.value = (preset.whitelist_codes || []).join(",");
    if (els.sandboxWhitelistNodeTypes) els.sandboxWhitelistNodeTypes.value = (preset.whitelist_node_types || []).join(",");
  }

  function renderSandboxHealth(health) {
    if (!els.sandboxHealthText) return;
    const level = String(health?.level || "green");
    const total = Number(health?.total || 0);
    const yellow = Number(health?.thresholds?.yellow || sandboxThresholdsPayload().yellow);
    const red = Number(health?.thresholds?.red || sandboxThresholdsPayload().red);
    const dedup = Number(health?.dedup_window_sec || sandboxDedupWindowSec());
    const suppressed = Number(health?.suppressed || 0);
    els.sandboxHealthText.textContent = `Sandbox状态: ${level.toUpperCase()} | 告警:${total} | 抑制:${suppressed} | 阈值:y=${yellow}, r=${red} | 去重窗:${dedup}s`;
    els.sandboxHealthText.style.color = level === "red" ? "#b42318" : (level === "yellow" ? "#b54708" : "#087443");
  }

  function currentSandboxPresetPayload() {
    return {
      thresholds: sandboxThresholdsPayload(),
      dedup_window_sec: sandboxDedupWindowSec(),
      rules: sandboxRulesPayloadFromUi(),
      autofix: {
        enabled: !!els.sandboxAutoFixEnabled?.checked,
        pause_queue: !!els.sandboxAutoFixPauseQueue?.checked,
        require_review: !!els.sandboxAutoFixRequireReview?.checked,
        force_isolation: !!els.sandboxAutoFixForceIsolation?.checked,
        red_threshold: Number(els.sandboxAutoFixRedThreshold?.value || 3),
        window_sec: Number(els.sandboxAutoFixWindowSec?.value || 900),
        force_minutes: Number(els.sandboxAutoFixForceMinutes?.value || 60),
        force_mode: String(els.sandboxAutoFixForceMode?.value || "process").trim() || "process",
      },
    };
  }

  function applySandboxPresetPayload(preset) {
    const safePreset = preset && typeof preset === "object" ? preset : {};
    const thresholds = safePreset.thresholds && typeof safePreset.thresholds === "object" ? safePreset.thresholds : {};
    if (els.sandboxThresholdYellow && Number.isFinite(Number(thresholds.yellow))) els.sandboxThresholdYellow.value = String(Math.floor(Number(thresholds.yellow)));
    if (els.sandboxThresholdRed && Number.isFinite(Number(thresholds.red))) els.sandboxThresholdRed.value = String(Math.floor(Number(thresholds.red)));
    if (els.sandboxDedupWindowSec && Number.isFinite(Number(safePreset.dedup_window_sec))) els.sandboxDedupWindowSec.value = String(Math.floor(Number(safePreset.dedup_window_sec)));
    if (safePreset.rules) applySandboxRulesToUi(safePreset.rules);
    const autofix = safePreset.autofix && typeof safePreset.autofix === "object" ? safePreset.autofix : {};
    if (els.sandboxAutoFixEnabled) els.sandboxAutoFixEnabled.checked = autofix.enabled !== false;
    if (els.sandboxAutoFixPauseQueue) els.sandboxAutoFixPauseQueue.checked = autofix.pause_queue !== false;
    if (els.sandboxAutoFixRequireReview) els.sandboxAutoFixRequireReview.checked = autofix.require_review !== false;
    if (els.sandboxAutoFixForceIsolation) els.sandboxAutoFixForceIsolation.checked = autofix.force_isolation !== false;
    if (els.sandboxAutoFixRedThreshold && Number.isFinite(Number(autofix.red_threshold))) els.sandboxAutoFixRedThreshold.value = String(Math.floor(Number(autofix.red_threshold)));
    if (els.sandboxAutoFixWindowSec && Number.isFinite(Number(autofix.window_sec))) els.sandboxAutoFixWindowSec.value = String(Math.floor(Number(autofix.window_sec)));
    if (els.sandboxAutoFixForceMinutes && Number.isFinite(Number(autofix.force_minutes))) els.sandboxAutoFixForceMinutes.value = String(Math.floor(Number(autofix.force_minutes)));
    if (els.sandboxAutoFixForceMode && autofix.force_mode) els.sandboxAutoFixForceMode.value = String(autofix.force_mode);
  }

  return {
    applySandboxPresetPayload,
    applySandboxPresetToUi,
    applySandboxRulesToUi,
    currentSandboxPresetPayload,
    parseCsvList,
    renderSandboxHealth,
    sandboxDedupWindowSec,
    sandboxRulesPayloadFromUi,
    sandboxThresholdsPayload,
  };
}

export { createWorkflowSupportSandbox };
