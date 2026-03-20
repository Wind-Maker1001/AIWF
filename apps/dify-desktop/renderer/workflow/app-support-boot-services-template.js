function buildWorkflowBootTemplateServices(ctx = {}) {
  const {
    appServices = {},
    uiServices = {},
  } = ctx;

  return {
    applySelectedTemplate: appServices.applySelectedTemplate,
    saveCurrentAsTemplate: appServices.saveCurrentAsTemplate,
    installTemplatePack: appServices.installTemplatePack,
    removeTemplatePackByCurrentTemplate: appServices.removeTemplatePackByCurrentTemplate,
    exportTemplatePackByCurrentTemplate: appServices.exportTemplatePackByCurrentTemplate,
    runTemplateAcceptance: appServices.runTemplateAcceptance,
    exportTemplateAcceptanceReport: appServices.exportTemplateAcceptanceReport,
    renderTemplateParamsForm: appServices.renderTemplateParamsForm,
    handleQualityRuleSetSelectChange: appServices.handleQualityRuleSetSelectChange,
    refreshQualityRuleSets: appServices.refreshQualityRuleSets,
    saveQualityRuleSetFromGraph: appServices.saveQualityRuleSetFromGraph,
    removeQualityRuleSetCurrent: appServices.removeQualityRuleSetCurrent,
    renderPalette: appServices.renderPalette,
    applyDeepSeekDefaults: uiServices.applyDeepSeekDefaults,
    renderTemplateSelect: appServices.renderTemplateSelect,
    refreshTemplateMarketplace: appServices.refreshTemplateMarketplace,
  };
}

export { buildWorkflowBootTemplateServices };
