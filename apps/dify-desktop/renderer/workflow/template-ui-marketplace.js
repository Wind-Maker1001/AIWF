import {
  LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
  TEMPLATE_WORKFLOW_DEFINITION_FIELD,
  parseLocalTemplateStorageText,
} from "./template-storage-contract.js";
import { formatAiwfError } from "./workflow-contract.js";

function createWorkflowTemplateMarketplaceSupport(els, deps = {}) {
  const {
    templateStorageKey = "aiwf.workflow.templates.v1",
    builtinTemplates = [],
    graphPayload = () => ({}),
    setStatus = () => {},
    renderTemplateSelect = () => {},
    parseRunParamsLoose = () => ({}),
    currentTemplateGovernance = () => ({}),
    getMarketplaceTemplates = () => [],
    setMarketplaceTemplates = () => {},
  } = deps;

  let localTemplates = [];

  function readLegacyLocalTemplates() {
    try {
      const raw = window.localStorage.getItem(templateStorageKey);
      if (!raw) return [];
      const normalized = parseLocalTemplateStorageText(raw, {
        allowStorageSchemaMigration: true,
        allowEntrySchemaMigration: true,
        allowLegacyGraphAlias: true,
      });
      if (normalized.migrated) {
        window.localStorage.setItem(templateStorageKey, JSON.stringify(normalized));
      }
      return Array.isArray(normalized.items) ? normalized.items : [];
    } catch {
      return [];
    }
  }

  function loadLocalTemplates() {
    return Array.isArray(localTemplates) && localTemplates.length ? localTemplates : readLegacyLocalTemplates();
  }

  async function refreshLocalTemplates() {
    let sharedItems = [];
    try {
      const out = await window.aiwfDesktop.listLocalTemplates({ limit: 500 });
      sharedItems = Array.isArray(out?.items) ? out.items : [];
    } catch {}

    if (!sharedItems.length) {
      const legacyItems = readLegacyLocalTemplates();
      if (legacyItems.length && window?.aiwfDesktop?.saveLocalTemplate) {
        for (const item of legacyItems) {
          const ret = await window.aiwfDesktop.saveLocalTemplate({ template: item });
          if (!ret?.ok) {
            setStatus(`Template migration failed: ${formatAiwfError(ret)}`, false);
            localTemplates = legacyItems;
            return;
          }
        }
        try {
          const out = await window.aiwfDesktop.listLocalTemplates({ limit: 500 });
          sharedItems = Array.isArray(out?.items) ? out.items : legacyItems;
        } catch {
          sharedItems = legacyItems;
        }
      } else {
        sharedItems = legacyItems;
      }
    }

    localTemplates = Array.isArray(sharedItems) ? sharedItems : [];
  }

  async function refreshTemplateMarketplace() {
    await refreshLocalTemplates();
    try {
      const out = await window.aiwfDesktop.listTemplateMarketplace({ limit: 500 });
      const packs = Array.isArray(out?.items) ? out.items : [];
      const merged = [];
      packs.forEach((pack) => {
        const templates = Array.isArray(pack?.templates) ? pack.templates : [];
        templates.forEach((template) => {
          if (!template || typeof template !== "object") return;
          merged.push({ ...template, __pack_id: String(pack?.id || ""), __pack_name: String(pack?.name || "") });
        });
      });
      setMarketplaceTemplates(merged);
    } catch {
      setMarketplaceTemplates([]);
    }
  }

  function allTemplates() {
    return [...builtinTemplates, ...loadLocalTemplates(), ...getMarketplaceTemplates()];
  }

  async function saveCurrentAsTemplate() {
    const name = String(prompt("Enter template name", String(els.workflowName?.value || "Custom Template")) || "").trim();
    if (!name) return;
    const id = `custom_${Date.now()}`;
    const out = await window.aiwfDesktop.saveLocalTemplate({
      template: {
        schema_version: LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
        id,
        name,
        [TEMPLATE_WORKFLOW_DEFINITION_FIELD]: graphPayload(),
        template_spec_version: 1,
        governance: currentTemplateGovernance(),
        runtime_defaults: parseRunParamsLoose(),
        created_at: new Date().toISOString(),
      },
    });
    if (!out?.ok) {
      setStatus(`Save template failed: ${formatAiwfError(out)}`, false);
      return;
    }
    await refreshLocalTemplates();
    renderTemplateSelect();
    if (els.templateSelect) els.templateSelect.value = id;
    setStatus(`Template saved: ${name}`, true);
  }

  async function installTemplatePack() {
    const out = await window.aiwfDesktop.loadWorkflow({ validateGraphContract: false });
    if (!out?.ok || !out?.path) {
      if (!out?.canceled) setStatus(`Load template pack failed: ${formatAiwfError(out)}`, false);
      return;
    }
    const ret = await window.aiwfDesktop.installTemplatePack({
      path: out.path,
      allowLegacyGraphAlias: true,
    });
    if (!ret?.ok) {
      setStatus(`Install template pack failed: ${formatAiwfError(ret)}`, false);
      return;
    }
    await refreshTemplateMarketplace();
    renderTemplateSelect();
    setStatus(`Template pack installed: ${ret?.item?.name || ret?.item?.id || ""}`, true);
  }

  async function removeTemplatePackByCurrentTemplate() {
    const id = String(els.templateSelect?.value || "").trim();
    const template = allTemplates().find((item) => String(item?.id || "") === id);
    const packId = String(template?.__pack_id || "").trim();
    if (!packId) {
      setStatus("Current template is not backed by a template pack.", false);
      return;
    }
    const out = await window.aiwfDesktop.removeTemplatePack({ id: packId });
    if (!out?.ok) {
      setStatus(`Remove template pack failed: ${formatAiwfError(out)}`, false);
      return;
    }
    await refreshTemplateMarketplace();
    renderTemplateSelect();
    setStatus(`Template pack removed: ${packId}`, true);
  }

  async function exportTemplatePackByCurrentTemplate() {
    const id = String(els.templateSelect?.value || "").trim();
    const template = allTemplates().find((item) => String(item?.id || "") === id);
    const packId = String(template?.__pack_id || "").trim();
    if (!packId) {
      setStatus("Current template is not backed by a template pack.", false);
      return;
    }
    const out = await window.aiwfDesktop.exportTemplatePack({ id: packId });
    if (out?.ok) setStatus(`Template pack exported: ${out.path}`, true);
    else if (!out?.canceled) setStatus(`Export template pack failed: ${formatAiwfError(out)}`, false);
  }

  return {
    allTemplates,
    exportTemplatePackByCurrentTemplate,
    installTemplatePack,
    loadLocalTemplates,
    refreshLocalTemplates,
    refreshTemplateMarketplace,
    removeTemplatePackByCurrentTemplate,
    saveCurrentAsTemplate,
  };
}

export { createWorkflowTemplateMarketplaceSupport };
