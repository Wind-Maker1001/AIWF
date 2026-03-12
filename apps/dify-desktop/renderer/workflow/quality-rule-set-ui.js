function createWorkflowQualityRuleSetUi(els, deps = {}) {
  const {
    setStatus = () => {},
    exportGraph = () => ({}),
    createOptionElement = () => document.createElement("option"),
  } = deps;

  function handleQualityRuleSetSelectChange() {
    if (els.qualityRuleSetId) {
      els.qualityRuleSetId.value = String(els.qualityRuleSetSelect?.value || "");
    }
  }

  async function refreshQualityRuleSets() {
    if (!els.qualityRuleSetSelect) return;
    try {
      const out = await window.aiwfDesktop.listQualityRuleSets();
      const sets = Array.isArray(out?.sets) ? out.sets : [];
      const cur = String(els.qualityRuleSetId?.value || "").trim();
      els.qualityRuleSetSelect.innerHTML = '<option value="">选择规则集...</option>';
      sets.forEach((s) => {
        const id = String(s?.id || "");
        if (!id) return;
        const op = createOptionElement();
        op.value = id;
        op.textContent = `${String(s?.name || id)} (${String(s?.version || "v1")})`;
        els.qualityRuleSetSelect.appendChild(op);
      });
      if (cur) els.qualityRuleSetSelect.value = cur;
    } catch {}
  }

  function collectRulesFromGraph() {
    const g = exportGraph();
    const nodes = Array.isArray(g?.nodes) ? g.nodes : [];
    const target = nodes.find((n) => ["quality_check_v2", "quality_check_v3", "quality_check_v4"].includes(String(n?.type || "")));
    if (!target) return {};
    const cfg = target?.config && typeof target.config === "object" ? target.config : {};
    return cfg.rules && typeof cfg.rules === "object" ? cfg.rules : {};
  }

  async function saveQualityRuleSetFromGraph() {
    const id = String(els.qualityRuleSetId?.value || "").trim();
    if (!id) {
      setStatus("请先填写质量规则集ID", false);
      return;
    }
    const rules = collectRulesFromGraph();
    const out = await window.aiwfDesktop.saveQualityRuleSet({
      set: {
        id,
        name: id,
        version: "v1",
        scope: "workflow",
        rules,
      },
    });
    if (out?.ok) {
      await refreshQualityRuleSets();
      setStatus(`质量规则集已保存: ${id}`, true);
    } else {
      setStatus(`保存规则集失败: ${out?.error || "unknown"}`, false);
    }
  }

  async function removeQualityRuleSetCurrent() {
    const id = String(els.qualityRuleSetId?.value || els.qualityRuleSetSelect?.value || "").trim();
    if (!id) {
      setStatus("请先选择质量规则集", false);
      return;
    }
    const out = await window.aiwfDesktop.removeQualityRuleSet({ id });
    if (out?.ok) {
      if (els.qualityRuleSetId) els.qualityRuleSetId.value = "";
      await refreshQualityRuleSets();
      setStatus(`质量规则集已删除: ${id}`, true);
    } else {
      setStatus(`删除规则集失败: ${out?.error || "unknown"}`, false);
    }
  }

  return {
    handleQualityRuleSetSelectChange,
    refreshQualityRuleSets,
    saveQualityRuleSetFromGraph,
    removeQualityRuleSetCurrent,
  };
}

export { createWorkflowQualityRuleSetUi };
