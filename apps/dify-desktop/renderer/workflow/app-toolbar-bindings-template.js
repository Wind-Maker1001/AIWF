import { bindIfPresent } from "./app-toolbar-bindings-core.js";

function compareFallback(getLastCompareResult = () => null) {
  return getLastCompareResult() || { ok: false, error: "请先执行对比" };
}

function bindWorkflowTemplateToolbarActions(ctx = {}) {
  const {
    els,
    renderCompareResult = () => {},
    getLastCompareResult = () => null,
    applySelectedTemplate = () => {},
    saveCurrentAsTemplate = () => {},
    installTemplatePack = () => {},
    removeTemplatePackByCurrentTemplate = () => {},
    exportTemplatePackByCurrentTemplate = () => {},
    runTemplateAcceptance = () => {},
    exportTemplateAcceptanceReport = () => {},
    renderTemplateParamsForm = () => {},
    handleQualityRuleSetSelectChange = () => {},
    refreshQualityRuleSets = () => {},
    saveQualityRuleSetFromGraph = () => {},
    removeQualityRuleSetCurrent = () => {},
    renderPalette = () => {},
    renderNodeTypePolicyHint = () => {},
    applyDeepSeekDefaults = () => {},
    refreshOfflineBoundaryHint = () => {},
  } = ctx;

  const rerenderCompare = () => renderCompareResult(compareFallback(getLastCompareResult));

  bindIfPresent(els.compareOnlyChanged, "change", rerenderCompare);
  bindIfPresent(els.compareOnlyStatusChanged, "change", rerenderCompare);
  bindIfPresent(els.compareMinDelta, "change", rerenderCompare);
  bindIfPresent(els.btnApplyTemplate, "click", applySelectedTemplate);
  bindIfPresent(els.btnSaveTemplate, "click", saveCurrentAsTemplate);
  bindIfPresent(els.btnTemplatePackInstall, "click", installTemplatePack);
  bindIfPresent(els.btnTemplatePackRemove, "click", removeTemplatePackByCurrentTemplate);
  bindIfPresent(els.btnTemplatePackExport, "click", exportTemplatePackByCurrentTemplate);
  bindIfPresent(els.btnTemplateAcceptance, "click", runTemplateAcceptance);
  bindIfPresent(els.btnTemplateAcceptanceExport, "click", exportTemplateAcceptanceReport);
  bindIfPresent(els.templateSelect, "change", renderTemplateParamsForm);
  bindIfPresent(els.qualityRuleSetSelect, "change", handleQualityRuleSetSelectChange);
  bindIfPresent(els.btnQualityRuleSetsRefresh, "click", refreshQualityRuleSets);
  bindIfPresent(els.btnQualityRuleSetSave, "click", saveQualityRuleSetFromGraph);
  bindIfPresent(els.btnQualityRuleSetRemove, "click", removeQualityRuleSetCurrent);
  bindIfPresent(els.paletteMode, "change", renderPalette);
  bindIfPresent(els.paletteSearch, "input", renderPalette);
  bindIfPresent(els.nodeType, "input", renderNodeTypePolicyHint);
  bindIfPresent(els.nodeType, "change", renderNodeTypePolicyHint);
  bindIfPresent(els.btnUseDeepSeek, "click", applyDeepSeekDefaults);
  bindIfPresent(els.aiEndpoint, "input", refreshOfflineBoundaryHint);
}

export {
  bindWorkflowTemplateToolbarActions,
  compareFallback,
};
