import { defaultTemplateParamValue } from "./template-utils.js";

function readTemplateParamsLooseText(text) {
  const normalized = String(text || "").trim();
  if (!normalized) return {};
  try {
    const obj = JSON.parse(normalized);
    if (obj && typeof obj === "object" && !Array.isArray(obj)) return obj;
  } catch {}
  return {};
}

function readTemplateParamControlValue(ctrl) {
  const type = String(ctrl?.dataset?.ttype || "string");
  const raw = String(ctrl?.value || "");
  if (type === "number") {
    const value = Number(raw);
    return Number.isFinite(value) ? value : raw;
  }
  if (type === "boolean") return raw === "true";
  if (type === "object" || type === "array") {
    try {
      return raw.trim() ? JSON.parse(raw) : (type === "array" ? [] : {});
    } catch {
      return raw;
    }
  }
  return raw;
}

function collectTemplateParamsFromControls(controls = []) {
  const list = Array.from(controls || []);
  if (!list.length) return null;
  const out = {};
  let seen = 0;
  list.forEach((ctrl) => {
    if (ctrl?.disabled) return;
    const key = String(ctrl?.dataset?.tparam || "").trim();
    if (!key) return;
    seen += 1;
    out[key] = readTemplateParamControlValue(ctrl);
  });
  return seen ? out : null;
}

function collectTemplateParamsFromFormElement(formEl) {
  if (!formEl?.querySelectorAll) return null;
  return collectTemplateParamsFromControls(formEl.querySelectorAll("[data-tparam]"));
}

function mergeTemplateParamsWithSchema(schema, existing = {}) {
  const merged = {};
  Object.entries(schema || {}).forEach(([key, rule]) => {
    if (Object.prototype.hasOwnProperty.call(existing, key)) merged[key] = existing[key];
    else merged[key] = defaultTemplateParamValue(rule);
  });
  return merged;
}

function parseTemplateParamsText(text) {
  const normalized = String(text || "").trim();
  if (!normalized) return {};
  const obj = JSON.parse(normalized);
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) throw new Error("模板参数必须是 JSON 对象");
  return obj;
}

export {
  collectTemplateParamsFromControls,
  collectTemplateParamsFromFormElement,
  mergeTemplateParamsWithSchema,
  parseTemplateParamsText,
  readTemplateParamControlValue,
  readTemplateParamsLooseText,
};
