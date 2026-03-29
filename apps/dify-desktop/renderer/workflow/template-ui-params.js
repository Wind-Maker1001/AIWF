import { validateTemplateParams, applyTemplateVars } from "./template-utils.js";
import { combineWorkflowMigrationReports } from "./workflow-contract.js";
import {
  collectTemplateParamsFromFormElement,
  mergeTemplateParamsWithSchema,
  parseTemplateParamsText,
  readTemplateParamsLooseText,
} from "./template-ui-param-state.js";
import {
  applyTemplateDependencyStateToForm,
  renderTemplateParamsFormInto,
} from "./template-ui-param-renderer.js";
import { resolveTemplateWorkflowDefinition } from "./template-storage-contract.js";

function createWorkflowTemplateParamSupport(els, deps = {}) {
  const {
    allTemplates = () => [],
    setStatus = () => {},
    renderAll = () => {},
    renderMigrationReport = () => {},
    store,
    syncRunParamsFormFromJson = () => {},
    clearSelectedEdge = () => {},
    validateWorkflowDefinition = async (workflowDefinition) => ({
      ok: true,
      workflow_definition: workflowDefinition,
      notes: [],
    }),
  } = deps;

  function currentTemplateItem() {
    const id = String(els.templateSelect?.value || "").trim();
    if (!id) return null;
    return allTemplates().find((item) => String(item.id || "") === id) || null;
  }

  function readTemplateParamsLoose() {
    return readTemplateParamsLooseText(els.templateParams?.value || "");
  }

  function collectTemplateParamsFromForm() {
    return collectTemplateParamsFromFormElement(els.templateParamsForm);
  }

  function currentTemplateSchema() {
    return currentTemplateItem()?.params_schema;
  }

  function applyTemplateDependencyState() {
    const formEl = els.templateParamsForm;
    const schema = currentTemplateSchema();
    if (!formEl || !schema || typeof schema !== "object") return;
    const params = collectTemplateParamsFromForm() || readTemplateParamsLoose();
    applyTemplateDependencyStateToForm(formEl, schema, params);
  }

  function syncTemplateParamsJsonFromForm() {
    const params = collectTemplateParamsFromForm();
    if (!params || !els.templateParams) return;
    els.templateParams.value = JSON.stringify(params, null, 2);
    applyTemplateDependencyState();
  }

  function renderTemplateParamsForm() {
    const formEl = els.templateParamsForm;
    const schema = currentTemplateSchema();
    if (!formEl) return;
    if (!schema || typeof schema !== "object" || !Object.keys(schema).length) {
      renderTemplateParamsFormInto(formEl, null, {}, syncTemplateParamsJsonFromForm);
      return;
    }
    const merged = mergeTemplateParamsWithSchema(schema, readTemplateParamsLoose());
    if (els.templateParams) els.templateParams.value = JSON.stringify(merged, null, 2);
    renderTemplateParamsFormInto(formEl, schema, merged, syncTemplateParamsJsonFromForm);
    applyTemplateDependencyState();
  }

  function parseTemplateParams() {
    const fromForm = collectTemplateParamsFromForm();
    if (fromForm) return fromForm;
    return parseTemplateParamsText(els.templateParams?.value || "");
  }

  async function applySelectedTemplate() {
    const id = String(els.templateSelect?.value || "").trim();
    if (!id) {
      setStatus("请先选择模板", false);
      return;
    }
    const item = currentTemplateItem();
    const workflowDefinition = resolveTemplateWorkflowDefinition(item);
    if (!item || !workflowDefinition) {
      setStatus("模板不存在", false);
      return;
    }
    let params = {};
    try {
      params = parseTemplateParams();
      validateTemplateParams(item.params_schema, params);
    } catch (error) {
      setStatus(String(error?.message || error || "模板参数错误"), false);
      return;
    }
    const graph = applyTemplateVars(workflowDefinition, params);
    let validated;
    try {
      validated = await validateWorkflowDefinition(graph);
    } catch (error) {
      setStatus(String(error?.message || error || "模板流程校验失败"), false);
      return;
    }
    if (!validated?.ok) {
      setStatus(String(validated?.error || "模板流程校验失败"), false);
      return;
    }
    const validatedGraph =
      validated?.workflow_definition && typeof validated.workflow_definition === "object"
        ? validated.workflow_definition
        : graph;
    const imported = store.importGraph(validatedGraph);
    clearSelectedEdge();
    els.workflowName.value = store.state.graph.name || String(item.name || "模板流程");
    if (item.governance && typeof item.governance === "object" && els.publishRequirePreflight) {
      els.publishRequirePreflight.checked = item.governance.preflight_gate_required !== false;
    }
    if (item.runtime_defaults && typeof item.runtime_defaults === "object") {
      els.appRunParams.value = JSON.stringify(item.runtime_defaults, null, 2);
      syncRunParamsFormFromJson();
    }
    renderAll();
    renderMigrationReport(
      combineWorkflowMigrationReports(
        {
          migrated: Array.isArray(validated?.notes) && validated.notes.length > 0,
          notes: Array.isArray(validated?.notes) ? validated.notes : [],
        },
        imported?.contract,
      ),
    );
    setStatus(`已应用模板: ${item.name || id}`, true);
  }

  return {
    applySelectedTemplate,
    applyTemplateDependencyState,
    collectTemplateParamsFromForm,
    parseTemplateParams,
    readTemplateParamsLoose,
    renderTemplateParamsForm,
    syncTemplateParamsJsonFromForm,
  };
}

export { createWorkflowTemplateParamSupport };
