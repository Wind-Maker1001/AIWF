import { resolveTemplateRule } from "./template-utils.js";

const EMPTY_TEMPLATE_PARAMS_HTML = '<div class="cfg-help">当前模板无参数定义，可直接编辑 JSON。</div>';
const INACTIVE_TEMPLATE_PARAM_TEXT = "该参数当前不生效（受联动条件控制）";

function applyTemplateDependencyStateToForm(formEl, schema, params) {
  if (!formEl?.querySelectorAll || !schema || typeof schema !== "object") return;
  const controls = formEl.querySelectorAll("[data-tparam]");
  controls.forEach((ctrl) => {
    const key = String(ctrl?.dataset?.tparam || "").trim();
    if (!key) return;
    const baseRule = schema[key];
    const rule = resolveTemplateRule(baseRule, params);
    const active = rule.__active !== false;
    ctrl.disabled = !active;
    if (String(ctrl.tagName || "").toLowerCase() === "input" && String(ctrl.type || "") === "number") {
      if (Number.isFinite(rule?.min)) ctrl.min = String(rule.min);
      else ctrl.removeAttribute("min");
      if (Number.isFinite(rule?.max)) ctrl.max = String(rule.max);
      else ctrl.removeAttribute("max");
    }
    const tip = formEl.querySelector(`[data-tparam-tip="${key}"]`);
    if (!tip) return;
    if (!active) tip.textContent = INACTIVE_TEMPLATE_PARAM_TEXT;
    else tip.textContent = String(rule?.description || baseRule?.description || "");
  });
}

function createTemplateParamControl(baseRule, rule, value, onInput) {
  const type = String(rule?.type || "string");
  let ctrl = null;
  if (Array.isArray(rule?.enum) && rule.enum.length) {
    ctrl = document.createElement("select");
    rule.enum.forEach((enumValue) => {
      const option = document.createElement("option");
      option.value = String(enumValue);
      option.textContent = String(enumValue);
      ctrl.appendChild(option);
    });
    const current = value == null ? "" : String(value);
    if (current) ctrl.value = current;
  } else if (type === "boolean") {
    ctrl = document.createElement("select");
    ctrl.innerHTML = '<option value="true">true</option><option value="false">false</option>';
    ctrl.value = value ? "true" : "false";
  } else if (type === "object" || type === "array") {
    ctrl = document.createElement("textarea");
    ctrl.value = JSON.stringify(value, null, 2);
    ctrl.style.minHeight = "66px";
  } else {
    ctrl = document.createElement("input");
    ctrl.type = type === "number" ? "number" : "text";
    if (type === "number") {
      if (Number.isFinite(rule?.min)) ctrl.min = String(rule.min);
      if (Number.isFinite(rule?.max)) ctrl.max = String(rule.max);
      if (Number.isFinite(rule?.step)) ctrl.step = String(rule.step);
    }
    ctrl.value = value == null ? "" : String(value);
  }
  ctrl.dataset.tparam = String(baseRule?.key || "");
  ctrl.dataset.ttype = String(baseRule?.type || type);
  ctrl.addEventListener("input", onInput);
  return ctrl;
}

function renderTemplateParamsFormInto(formEl, schema, merged, onInput) {
  if (!formEl) return;
  if (!schema || typeof schema !== "object" || !Object.keys(schema).length) {
    formEl.innerHTML = EMPTY_TEMPLATE_PARAMS_HTML;
    return;
  }
  formEl.innerHTML = "";
  const grouped = {};
  Object.entries(schema).forEach(([key, baseRule]) => {
    const rule = resolveTemplateRule(baseRule, merged);
    const group = String(rule?.group || baseRule?.group || "未分组");
    if (!grouped[group]) grouped[group] = [];
    grouped[group].push({ key, baseRule, rule });
  });
  Object.entries(grouped).forEach(([groupName, entries]) => {
    const box = document.createElement("div");
    box.style.border = "1px solid #dfe8f3";
    box.style.borderRadius = "8px";
    box.style.padding = "8px";
    box.style.marginBottom = "8px";
    const title = document.createElement("div");
    title.className = "cfg-help";
    title.style.fontWeight = "700";
    title.style.color = "#41566b";
    title.textContent = groupName;
    box.appendChild(title);
    entries.forEach(({ key, baseRule, rule }) => {
      const wrap = document.createElement("div");
      wrap.className = "cfg-field";
      const label = document.createElement("label");
      const required = rule?.required ? " *" : "";
      label.textContent = `${key}${required}`;
      wrap.appendChild(label);
      if (rule?.description || baseRule?.description) {
        const desc = document.createElement("div");
        desc.className = "cfg-help";
        desc.style.marginTop = "-2px";
        desc.style.marginBottom = "5px";
        desc.dataset.tparamTip = key;
        desc.textContent = String(rule?.description || baseRule?.description || "");
        wrap.appendChild(desc);
      }
      const ctrl = createTemplateParamControl({ ...baseRule, key }, rule, merged[key], onInput);
      wrap.appendChild(ctrl);
      box.appendChild(wrap);
    });
    formEl.appendChild(box);
  });
}

export {
  EMPTY_TEMPLATE_PARAMS_HTML,
  INACTIVE_TEMPLATE_PARAM_TEXT,
  applyTemplateDependencyStateToForm,
  renderTemplateParamsFormInto,
};
