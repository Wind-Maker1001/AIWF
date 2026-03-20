function defaultRunParamValueForRule(rule) {
  if (rule && Object.prototype.hasOwnProperty.call(rule, "default")) return rule.default;
  const type = String(rule?.type || "string");
  if (type === "number") return 0;
  if (type === "boolean") return false;
  if (type === "object") return {};
  if (type === "array") return [];
  return "";
}

function collectRunParamsControls(controls = []) {
  const out = {};
  Array.from(controls || []).forEach((ctrl) => {
    const key = String(ctrl?.dataset?.appRunParam || "").trim();
    const type = String(ctrl?.dataset?.appRunType || "string");
    if (!key) return;
    if (type === "boolean") {
      out[key] = String(ctrl.value || "false") === "true";
      return;
    }
    if (type === "number") {
      const n = Number(ctrl.value || 0);
      out[key] = Number.isFinite(n) ? n : 0;
      return;
    }
    if (type === "object" || type === "array") {
      const raw = String(ctrl.value || "").trim();
      if (!raw) {
        out[key] = type === "array" ? [] : {};
        return;
      }
      try {
        out[key] = JSON.parse(raw);
      } catch {
        out[key] = raw;
      }
      return;
    }
    out[key] = String(ctrl.value || "");
  });
  return out;
}

function buildRunParamsFromSchema(schemaObj, preferredParams) {
  const schema = schemaObj && typeof schemaObj === "object" && !Array.isArray(schemaObj) ? schemaObj : {};
  const preferred = preferredParams && typeof preferredParams === "object" && !Array.isArray(preferredParams) ? preferredParams : {};
  const params = {};
  Object.entries(schema).forEach(([key, rule]) => {
    if (Object.prototype.hasOwnProperty.call(preferred, key)) params[key] = preferred[key];
    else params[key] = defaultRunParamValueForRule(rule);
  });
  return params;
}

function parseJsonObjectText(text, fallback = {}) {
  try {
    const normalized = String(text || "").trim();
    return normalized ? JSON.parse(normalized) : fallback;
  } catch {
    return fallback;
  }
}

export {
  buildRunParamsFromSchema,
  collectRunParamsControls,
  defaultRunParamValueForRule,
  parseJsonObjectText,
};
