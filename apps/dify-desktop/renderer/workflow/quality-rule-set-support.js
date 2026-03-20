function currentQualityRuleSetId(els = {}) {
  return String(els.qualityRuleSetId?.value || els.qualityRuleSetSelect?.value || "").trim();
}

function normalizeQualityRuleSets(out) {
  return Array.isArray(out?.sets) ? out.sets : [];
}

function buildQualityRuleSetOptions(sets = []) {
  return sets
    .map((s) => {
      const id = String(s?.id || "");
      if (!id) return null;
      return {
        value: id,
        textContent: `${String(s?.name || id)} (${String(s?.version || "v1")})`,
      };
    })
    .filter(Boolean);
}

function collectQualityRulesFromGraph(graph) {
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const target = nodes.find((n) => ["quality_check_v2", "quality_check_v3", "quality_check_v4"].includes(String(n?.type || "")));
  if (!target) return {};
  const cfg = target?.config && typeof target.config === "object" ? target.config : {};
  return cfg.rules && typeof cfg.rules === "object" ? cfg.rules : {};
}

export {
  buildQualityRuleSetOptions,
  collectQualityRulesFromGraph,
  currentQualityRuleSetId,
  normalizeQualityRuleSets,
};
