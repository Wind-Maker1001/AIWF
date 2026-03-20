import {
  appSchemaRowsFromSchemaObject,
  collectAppSchemaRows,
  normalizeAppSchemaObjectValue,
} from "./app-form-schema-support.js";
import { renderAppSchemaFormInto } from "./app-form-schema-renderer.js";
import {
  buildRunParamsFromSchema,
  collectRunParamsControls,
  defaultRunParamValueForRule,
  parseJsonObjectText,
} from "./app-form-run-params-support.js";
import { renderRunParamsFormInto } from "./app-form-run-renderer.js";

function createWorkflowAppFormUi(els, deps = {}) {
  const { setStatus = () => {} } = deps;

  function normalizeAppSchemaObject(obj) {
    return normalizeAppSchemaObjectValue(obj);
  }

  function appSchemaRowsFromObject(schemaObj) {
    return appSchemaRowsFromSchemaObject(schemaObj);
  }

  function collectAppSchemaFromForm() {
    if (!els.appSchemaForm) return {};
    const rows = els.appSchemaForm.querySelectorAll("div[data-app-schema-row='1']");
    return collectAppSchemaRows(rows);
  }

  function renderAppSchemaForm(rows) {
    renderAppSchemaFormInto(els.appSchemaForm, rows, {
      onSchemaChanged: () => {
        syncAppSchemaJsonFromForm();
        syncRunParamsFormFromJson();
      },
      onEmptyRequested: () => renderAppSchemaForm([]),
    });
  }

  function syncAppSchemaJsonFromForm() {
    if (!els.appSchemaJson) return;
    const schema = collectAppSchemaFromForm();
    els.appSchemaJson.value = JSON.stringify(schema, null, 2);
  }

  function syncAppSchemaFormFromJson() {
    const text = String(els.appSchemaJson?.value || "").trim();
    if (!text) {
      renderAppSchemaForm([]);
      return;
    }
    let parsed = {};
    try {
      parsed = JSON.parse(text);
    } catch (e) {
      setStatus(`Schema JSON 解析失败: ${e}`, false);
      return;
    }
    renderAppSchemaForm(appSchemaRowsFromObject(parsed));
    syncAppSchemaJsonFromForm();
  }

  function defaultRunParamValue(rule) {
    return defaultRunParamValueForRule(rule);
  }

  function collectRunParamsForm() {
    if (!els.appRunParamsForm) return {};
    return collectRunParamsControls(els.appRunParamsForm.querySelectorAll("[data-app-run-param]"));
  }

  function syncRunParamsJsonFromForm() {
    if (!els.appRunParams) return;
    els.appRunParams.value = JSON.stringify(collectRunParamsForm(), null, 2);
  }

  function renderRunParamsFormBySchema(schemaObj, preferredParams) {
    if (!els.appRunParamsForm) return;
    const schema = normalizeAppSchemaObject(schemaObj);
    const params = buildRunParamsFromSchema(schema, preferredParams);
    renderRunParamsFormInto(els.appRunParamsForm, schema, params, syncRunParamsJsonFromForm);
    syncRunParamsJsonFromForm();
  }

  function syncRunParamsFormFromJson() {
    const schema = normalizeAppSchemaObject(parseJsonObjectText(els.appSchemaJson?.value || "{}", {}));
    let params = {};
    try {
      params = parseJsonObjectText(els.appRunParams?.value || "{}", {});
    } catch (e) {
      setStatus(`运行参数 JSON 解析失败: ${e}`, false);
      return;
    }
    renderRunParamsFormBySchema(schema, params);
  }

  return {
    normalizeAppSchemaObject,
    appSchemaRowsFromObject,
    collectAppSchemaFromForm,
    renderAppSchemaForm,
    syncAppSchemaJsonFromForm,
    syncAppSchemaFormFromJson,
    defaultRunParamValue,
    collectRunParamsForm,
    syncRunParamsJsonFromForm,
    renderRunParamsFormBySchema,
    syncRunParamsFormFromJson,
  };
}

export { createWorkflowAppFormUi };
