import {
  LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
  TEMPLATE_WORKFLOW_DEFINITION_FIELD,
  parseLocalTemplateStorageText,
  stringifyLocalTemplateStorage,
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

  function loadLocalTemplates() {
    try {
      const raw = window.localStorage.getItem(templateStorageKey);
      if (!raw) return [];
      const normalized = parseLocalTemplateStorageText(raw, {
        allowStorageSchemaMigration: true,
        allowEntrySchemaMigration: true,
      });
      if (normalized.migrated) {
        window.localStorage.setItem(templateStorageKey, JSON.stringify(normalized));
      }
      return Array.isArray(normalized.items) ? normalized.items : [];
    } catch {
      return [];
    }
  }

  function saveLocalTemplates(items) {
    try {
      window.localStorage.setItem(templateStorageKey, stringifyLocalTemplateStorage({
        schema_version: "local_template_storage.v1",
        items: Array.isArray(items) ? items : [],
      }, {
        allowStorageSchemaMigration: false,
        allowEntrySchemaMigration: true,
      }));
    } catch {}
  }

  async function refreshTemplateMarketplace() {
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

  function saveCurrentAsTemplate() {
    const name = String(prompt("请输入模板名称", String(els.workflowName?.value || "自定义模板")) || "").trim();
    if (!name) return;
    const custom = loadLocalTemplates();
    const id = `custom_${Date.now()}`;
    custom.push({
      schema_version: LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
      id,
      name,
      [TEMPLATE_WORKFLOW_DEFINITION_FIELD]: graphPayload(),
      template_spec_version: 1,
      governance: currentTemplateGovernance(),
      runtime_defaults: parseRunParamsLoose(),
      created_at: new Date().toISOString(),
    });
    saveLocalTemplates(custom);
    renderTemplateSelect();
    if (els.templateSelect) els.templateSelect.value = id;
    setStatus(`模板已保存: ${name}`, true);
  }

  async function installTemplatePack() {
    const out = await window.aiwfDesktop.loadWorkflow({ validateGraphContract: false });
    if (!out?.ok || !out?.path) {
      if (!out?.canceled) setStatus(`读取模板包失败: ${formatAiwfError(out)}`, false);
      return;
    }
    const ret = await window.aiwfDesktop.installTemplatePack({ path: out.path });
    if (!ret?.ok) {
      setStatus(`安装模板包失败: ${formatAiwfError(ret)}`, false);
      return;
    }
    await refreshTemplateMarketplace();
    renderTemplateSelect();
    setStatus(`模板包已安装: ${ret?.item?.name || ret?.item?.id || ""}`, true);
  }

  async function removeTemplatePackByCurrentTemplate() {
    const id = String(els.templateSelect?.value || "").trim();
    const template = allTemplates().find((item) => String(item?.id || "") === id);
    const packId = String(template?.__pack_id || "").trim();
    if (!packId) {
      setStatus("当前模板不是模板包来源，无法移除", false);
      return;
    }
    const out = await window.aiwfDesktop.removeTemplatePack({ id: packId });
    if (!out?.ok) {
      setStatus(`移除模板包失败: ${formatAiwfError(out)}`, false);
      return;
    }
    await refreshTemplateMarketplace();
    renderTemplateSelect();
    setStatus(`模板包已移除: ${packId}`, true);
  }

  async function exportTemplatePackByCurrentTemplate() {
    const id = String(els.templateSelect?.value || "").trim();
    const template = allTemplates().find((item) => String(item?.id || "") === id);
    const packId = String(template?.__pack_id || "").trim();
    if (!packId) {
      setStatus("当前模板不是模板包来源，无法导出", false);
      return;
    }
    const out = await window.aiwfDesktop.exportTemplatePack({ id: packId });
    if (out?.ok) setStatus(`模板包已导出: ${out.path}`, true);
    else if (!out?.canceled) setStatus(`导出模板包失败: ${formatAiwfError(out)}`, false);
  }

  return {
    allTemplates,
    exportTemplatePackByCurrentTemplate,
    installTemplatePack,
    loadLocalTemplates,
    refreshTemplateMarketplace,
    removeTemplatePackByCurrentTemplate,
    saveCurrentAsTemplate,
    saveLocalTemplates,
  };
}

export { createWorkflowTemplateMarketplaceSupport };
