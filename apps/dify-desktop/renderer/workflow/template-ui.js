import { createWorkflowTemplateMarketplaceSupport } from "./template-ui-marketplace.js";
import { createWorkflowTemplateParamSupport } from "./template-ui-params.js";

function createWorkflowTemplateUi(els, deps = {}) {
  const {
    templateStorageKey = "aiwf.workflow.templates.v1",
    builtinTemplates = [],
    store,
    setStatus = () => {},
    renderAll = () => {},
    renderMigrationReport = () => {},
    graphPayload = () => ({}),
    syncRunParamsFormFromJson = () => {},
  } = deps;

  let marketplaceTemplates = [];

  function parseRunParamsLoose() {
    try {
      const raw = String(els.appRunParams?.value || "").trim();
      if (!raw) return {};
      const obj = JSON.parse(raw);
      return obj && typeof obj === "object" && !Array.isArray(obj) ? obj : {};
    } catch {
      return {};
    }
  }

  function currentTemplateGovernance() {
    return {
      preflight_gate_required: els.publishRequirePreflight ? !!els.publishRequirePreflight.checked : true,
      auto_fix_enabled: true,
      acceptance_mode: "preflight_autofix_recheck_v1",
    };
  }

  const marketplaceSupport = createWorkflowTemplateMarketplaceSupport(els, {
    templateStorageKey,
    builtinTemplates,
    graphPayload,
    setStatus,
    renderTemplateSelect: () => renderTemplateSelect(),
    parseRunParamsLoose: () => parseRunParamsLoose(),
    currentTemplateGovernance: () => currentTemplateGovernance(),
    getMarketplaceTemplates: () => marketplaceTemplates,
    setMarketplaceTemplates: (items) => { marketplaceTemplates = Array.isArray(items) ? items : []; },
  });

  function allTemplates() {
    return marketplaceSupport.allTemplates();
  }

  const paramSupport = createWorkflowTemplateParamSupport(els, {
    allTemplates: () => allTemplates(),
    setStatus,
    renderAll,
    renderMigrationReport,
    store,
    syncRunParamsFormFromJson,
    clearSelectedEdge: () => {},
  });

  function renderTemplateSelect() {
    if (!els.templateSelect) return;
    const items = allTemplates();
    els.templateSelect.innerHTML = '<option value="">选择模板...</option>';
    items.forEach((template) => {
      const option = document.createElement("option");
      option.value = String(template.id || "");
      option.textContent = String(template.name || template.id || "template");
      els.templateSelect.appendChild(option);
    });
    renderTemplateParamsForm();
  }

  async function refreshTemplateMarketplace() {
    await marketplaceSupport.refreshTemplateMarketplace();
  }

  function renderTemplateParamsForm() {
    return paramSupport.renderTemplateParamsForm();
  }

  function collectTemplateParamsFromForm() {
    return paramSupport.collectTemplateParamsFromForm();
  }

  function syncTemplateParamsJsonFromForm() {
    return paramSupport.syncTemplateParamsJsonFromForm();
  }

  function applyTemplateDependencyState() {
    return paramSupport.applyTemplateDependencyState();
  }

  function applySelectedTemplate() {
    return paramSupport.applySelectedTemplate();
  }

  function saveCurrentAsTemplate() {
    return marketplaceSupport.saveCurrentAsTemplate();
  }

  async function installTemplatePack() {
    return marketplaceSupport.installTemplatePack();
  }

  async function removeTemplatePackByCurrentTemplate() {
    return marketplaceSupport.removeTemplatePackByCurrentTemplate();
  }

  async function exportTemplatePackByCurrentTemplate() {
    return marketplaceSupport.exportTemplatePackByCurrentTemplate();
  }

  function parseTemplateParams() {
    return paramSupport.parseTemplateParams();
  }

  return {
    parseRunParamsLoose,
    currentTemplateGovernance,
    allTemplates,
    renderTemplateSelect,
    refreshTemplateMarketplace,
    renderTemplateParamsForm,
    collectTemplateParamsFromForm,
    syncTemplateParamsJsonFromForm,
    applyTemplateDependencyState,
    applySelectedTemplate,
    saveCurrentAsTemplate,
    installTemplatePack,
    removeTemplatePackByCurrentTemplate,
    exportTemplatePackByCurrentTemplate,
    parseTemplateParams,
  };
}

export { createWorkflowTemplateUi };
