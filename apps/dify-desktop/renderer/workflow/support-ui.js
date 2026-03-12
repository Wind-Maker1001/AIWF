function createWorkflowSupportUi(els, deps = {}) {
  const {
    setStatus = () => {},
    getLastCompareResult = () => null,
    setLastCompareResult = () => {},
  } = deps;
  function sandboxThresholdsPayload() {
    const y = Number(els.sandboxThresholdYellow?.value || 1);
    const r = Number(els.sandboxThresholdRed?.value || 3);
    const yellow = Number.isFinite(y) ? Math.max(1, Math.floor(y)) : 1;
    const red = Number.isFinite(r) ? Math.max(yellow + 1, Math.floor(r)) : Math.max(3, yellow + 1);
    return { yellow, red };
  }

  function sandboxDedupWindowSec() {
    const n = Number(els.sandboxDedupWindowSec?.value || 600);
    return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 600;
  }

  function parseCsvList(text) {
    return String(text || "")
      .split(/[;,]/)
      .map((x) => String(x || "").trim().toLowerCase())
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
    const r = rules && typeof rules === "object" ? rules : {};
    if (els.sandboxWhitelistCodes) {
      const codes = Array.isArray(r.whitelist_codes) ? r.whitelist_codes : [];
      els.sandboxWhitelistCodes.value = codes.join(",");
    }
    if (els.sandboxWhitelistNodeTypes) {
      const types = Array.isArray(r.whitelist_node_types) ? r.whitelist_node_types : [];
      els.sandboxWhitelistNodeTypes.value = types.join(",");
    }
  }

  function applySandboxPresetToUi(name) {
    const key = String(name || "balanced").trim().toLowerCase();
    const p = SANDBOX_RULE_PRESETS[key] || SANDBOX_RULE_PRESETS.balanced;
    if (els.sandboxThresholdYellow) els.sandboxThresholdYellow.value = String(p.yellow);
    if (els.sandboxThresholdRed) els.sandboxThresholdRed.value = String(p.red);
    if (els.sandboxDedupWindowSec) els.sandboxDedupWindowSec.value = String(p.dedup_window_sec);
    if (els.sandboxWhitelistCodes) els.sandboxWhitelistCodes.value = (p.whitelist_codes || []).join(",");
    if (els.sandboxWhitelistNodeTypes) els.sandboxWhitelistNodeTypes.value = (p.whitelist_node_types || []).join(",");
  }

  function renderSandboxHealth(health) {
    if (!els.sandboxHealthText) return;
    const level = String(health?.level || "green");
    const total = Number(health?.total || 0);
    const y = Number(health?.thresholds?.yellow || sandboxThresholdsPayload().yellow);
    const r = Number(health?.thresholds?.red || sandboxThresholdsPayload().red);
    const dedup = Number(health?.dedup_window_sec || sandboxDedupWindowSec());
    const suppressed = Number(health?.suppressed || 0);
    els.sandboxHealthText.textContent = `Sandbox状态: ${level.toUpperCase()} | 告警:${total} | 抑制:${suppressed} | 阈值 y=${y}, r=${r} | 去重窗=${dedup}s`;
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
    const p = preset && typeof preset === "object" ? preset : {};
    const t = p.thresholds && typeof p.thresholds === "object" ? p.thresholds : {};
    if (els.sandboxThresholdYellow && Number.isFinite(Number(t.yellow))) els.sandboxThresholdYellow.value = String(Math.floor(Number(t.yellow)));
    if (els.sandboxThresholdRed && Number.isFinite(Number(t.red))) els.sandboxThresholdRed.value = String(Math.floor(Number(t.red)));
    if (els.sandboxDedupWindowSec && Number.isFinite(Number(p.dedup_window_sec))) els.sandboxDedupWindowSec.value = String(Math.floor(Number(p.dedup_window_sec)));
    if (p.rules) applySandboxRulesToUi(p.rules);
    const af = p.autofix && typeof p.autofix === "object" ? p.autofix : {};
    if (els.sandboxAutoFixEnabled) els.sandboxAutoFixEnabled.checked = af.enabled !== false;
    if (els.sandboxAutoFixPauseQueue) els.sandboxAutoFixPauseQueue.checked = af.pause_queue !== false;
    if (els.sandboxAutoFixRequireReview) els.sandboxAutoFixRequireReview.checked = af.require_review !== false;
    if (els.sandboxAutoFixForceIsolation) els.sandboxAutoFixForceIsolation.checked = af.force_isolation !== false;
    if (els.sandboxAutoFixRedThreshold && Number.isFinite(Number(af.red_threshold))) els.sandboxAutoFixRedThreshold.value = String(Math.floor(Number(af.red_threshold)));
    if (els.sandboxAutoFixWindowSec && Number.isFinite(Number(af.window_sec))) els.sandboxAutoFixWindowSec.value = String(Math.floor(Number(af.window_sec)));
    if (els.sandboxAutoFixForceMinutes && Number.isFinite(Number(af.force_minutes))) els.sandboxAutoFixForceMinutes.value = String(Math.floor(Number(af.force_minutes)));
    if (els.sandboxAutoFixForceMode && af.force_mode) els.sandboxAutoFixForceMode.value = String(af.force_mode);
  }



  function reviewHistoryFilterPayload() {
    return {
      run_id: String(els.reviewHistoryRunId?.value || "").trim(),
      reviewer: String(els.reviewHistoryReviewer?.value || "").trim(),
      status: String(els.reviewHistoryStatus?.value || "").trim(),
      date_from: String(els.reviewHistoryDateFrom?.value || "").trim(),
      date_to: String(els.reviewHistoryDateTo?.value || "").trim(),
    };
  }

  function renderReviewHistoryRows(items = []) {
    if (!els.reviewHistoryRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.reviewHistoryRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.reviewHistoryRows.innerHTML = "";
    items.slice(0, 100).forEach((it) => {
      const tr = document.createElement("tr");
      const tdKey = document.createElement("td");
      const tdStatus = document.createElement("td");
      const tdReviewer = document.createElement("td");
      const tdTime = document.createElement("td");
      tdKey.textContent = String(it.review_key || "");
      const status = String(it.status || "");
      tdStatus.textContent = status;
      if (status === "approved") tdStatus.style.color = "#087443";
      if (status === "rejected") tdStatus.style.color = "#b42318";
      tdReviewer.textContent = String(it.reviewer || "");
      tdTime.textContent = String(it.decided_at || "");
      tr.append(tdKey, tdStatus, tdReviewer, tdTime);
      els.reviewHistoryRows.appendChild(tr);
    });
  }

  async function refreshReviewHistory() {
    try {
      const out = await window.aiwfDesktop.listManualReviewHistory({
        limit: 300,
        filter: reviewHistoryFilterPayload(),
      });
      renderReviewHistoryRows(out?.items || []);
    } catch {
      renderReviewHistoryRows([]);
    }
  }

  async function exportReviewHistory() {
    try {
      const out = await window.aiwfDesktop.exportManualReviewHistory({
        filter: reviewHistoryFilterPayload(),
      });
      if (!out?.ok) {
        if (!out?.canceled) setStatus(`导出审核历史失败: ${out?.error || "unknown"}`, false);
        return;
      }
      setStatus(`审核历史已导出: ${out.path} (${out.total} 条)`, true);
    } catch (e) {
      setStatus(`导出审核历史失败: ${e}`, false);
    }
  }

  async function compareRuns() {
    const runA = String(els.compareRunA?.value || "").trim();
    const runB = String(els.compareRunB?.value || "").trim();
    if (!runA || !runB) {
      setStatus("请填写 Run A / Run B", false);
      return;
    }
    const out = await window.aiwfDesktop.compareWorkflowRuns({ run_a: runA, run_b: runB });
    setLastCompareResult(out);
    els.log.textContent = JSON.stringify(out, null, 2);
    renderCompareResult(out);
    setStatus(out?.ok ? "运行对比完成" : `运行对比失败: ${out?.error || "unknown"}`, !!out?.ok);
  }

  async function exportCompareReport() {
    const runA = String(els.compareRunA?.value || "").trim();
    const runB = String(els.compareRunB?.value || "").trim();
    if (!runA || !runB) {
      setStatus("请先填写 Run A / Run B", false);
      return;
    }
    try {
      const format = String(els.compareReportFormat?.value || "md").trim() || "md";
      const out = await window.aiwfDesktop.exportCompareReport({
        run_a: runA,
        run_b: runB,
        format,
      });
      if (!out?.ok) {
        if (!out?.canceled) setStatus(`导出对比报告失败: ${out?.error || "unknown"}`, false);
        return;
      }
      setStatus(`对比报告已导出: ${out.path}`, true);
    } catch (e) {
      setStatus(`导出对比报告失败: ${e}`, false);
    }
  }

  async function saveCurrentRunAsBaseline() {
    const runA = String(els.compareRunA?.value || "").trim();
    if (!runA) {
      setStatus("请先在“运行对比”里选择 Run A 作为基线", false);
      return;
    }
    const out = await window.aiwfDesktop.saveRunBaseline({
      run_id: runA,
      name: `baseline_${runA.slice(0, 8)}`,
    });
    if (out?.ok) setStatus(`基线已保存: ${out?.item?.baseline_id}`, true);
    else setStatus(`保存基线失败: ${out?.error || "unknown"}`, false);
  }

  async function compareWithLatestBaseline() {
    const runB = String(els.compareRunB?.value || "").trim();
    if (!runB) {
      setStatus("请先在“运行对比”里选择 Run B", false);
      return;
    }
    const baselines = await window.aiwfDesktop.listRunBaselines();
    const first = Array.isArray(baselines?.items) ? baselines.items[0] : null;
    if (!first) {
      setStatus("未找到基线，请先保存基线", false);
      return;
    }
    const out = await window.aiwfDesktop.compareRunWithBaseline({
      run_id: runB,
      baseline_id: first.baseline_id,
    });
    if (!out?.ok) {
      setStatus(`基线对比失败: ${out?.error || "unknown"}`, false);
      return;
    }
    const reg = out.regression || {};
    setStatus(
      `基线对比完成: changed=${reg.changed_nodes || 0}, status_flip=${reg.status_flip_nodes || 0}, perf_hot=${reg.perf_hot_nodes || 0}`,
      true
    );
  }

  async function loadLineageForRunA() {
    const runId = String(els.compareRunA?.value || "").trim();
    if (!runId) {
      setStatus("请先在“运行对比”里选择 Run A", false);
      return;
    }
    const out = await window.aiwfDesktop.getWorkflowLineage({ run_id: runId });
    if (!out?.ok) {
      setStatus(`加载血缘失败: ${out?.error || "unknown"}`, false);
      return;
    }
    const lineage = out.lineage || {};
    const nodes = Array.isArray(lineage?.nodes) ? lineage.nodes.length : Number(lineage?.node_count || 0);
    const edges = Array.isArray(lineage?.edges) ? lineage.edges.length : Number(lineage?.edge_count || 0);
    if (els.log) els.log.textContent = JSON.stringify({ run_id: runId, lineage }, null, 2);
    setStatus(`血缘已加载: nodes=${nodes}, edges=${edges}`, true);
  }

  function renderCompareResult(out) {
    if (!els.compareSummary || !els.compareRows) return;
    if (!out?.ok) {
      els.compareSummary.textContent = `对比失败: ${out?.error || "unknown"}`;
      els.compareRows.innerHTML = '<tr><td colspan="5" style="color:#74879b">暂无</td></tr>';
      return;
    }
    const s = out.summary || {};
    const onlyChanged = !!els.compareOnlyChanged?.checked;
    const onlyStatusChanged = !!els.compareOnlyStatusChanged?.checked;
    const minDelta = Number(els.compareMinDelta?.value || 0.001);
    const safeDelta = Number.isFinite(minDelta) ? Math.max(0, minDelta) : 0.001;
    const srcRows = Array.isArray(out.node_diff) ? out.node_diff : [];
    const rows = srcRows.filter((r) => {
      const delta = Math.abs(Number(r.seconds_delta || 0));
      if (onlyStatusChanged && !r.status_changed) return false;
      if (onlyChanged && !(r.status_changed || delta >= safeDelta)) return false;
      return true;
    });
    els.compareSummary.textContent = `A:${s.run_a || "-"} | B:${s.run_b || "-"} | changed_nodes:${s.changed_nodes || 0} | visible:${rows.length}/${srcRows.length}`;
    if (!rows.length) {
      els.compareRows.innerHTML = '<tr><td colspan="5" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.compareRows.innerHTML = "";
    rows.forEach((r) => {
      const tr = document.createElement("tr");
      const tdNode = document.createElement("td");
      const tdStatus = document.createElement("td");
      const tdA = document.createElement("td");
      const tdB = document.createElement("td");
      const tdD = document.createElement("td");
      tdNode.textContent = `${String(r.id || "")}(${String(r.type || "")})`;
      tdStatus.textContent = `${String(r.status_a || "")} / ${String(r.status_b || "")}`;
      tdA.textContent = Number(r.seconds_a || 0).toFixed(3);
      tdB.textContent = Number(r.seconds_b || 0).toFixed(3);
      const delta = Number(r.seconds_delta || 0);
      tdD.textContent = delta.toFixed(3);
      if (r.status_changed) tdStatus.style.color = "#b42318";
      if (delta > 0.001) tdD.style.color = "#b42318";
      if (delta < -0.001) tdD.style.color = "#087443";
      if (r.status_changed || Math.abs(delta) >= safeDelta) tr.style.background = "#fff8f2";
      tr.append(tdNode, tdStatus, tdA, tdB, tdD);
      els.compareRows.appendChild(tr);
    });
  }

  return {
    sandboxThresholdsPayload,
    sandboxDedupWindowSec,
    parseCsvList,
    sandboxRulesPayloadFromUi,
    applySandboxRulesToUi,
    applySandboxPresetToUi,
    renderSandboxHealth,
    reviewHistoryFilterPayload,
    renderReviewHistoryRows,
    refreshReviewHistory,
    exportReviewHistory,
    compareRuns,
    exportCompareReport,
    saveCurrentRunAsBaseline,
    compareWithLatestBaseline,
    loadLineageForRunA,
    renderCompareResult,
    currentSandboxPresetPayload,
    applySandboxPresetPayload,
  };
}

export { createWorkflowSupportUi };
