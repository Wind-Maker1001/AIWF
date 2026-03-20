function normalizeAppSchemaObjectValue(obj) {
  const src = obj && typeof obj === "object" && !Array.isArray(obj) ? obj : {};
  const out = {};
  Object.entries(src).forEach(([key, raw]) => {
    const k = String(key || "").trim();
    if (!k) return;
    const rule = raw && typeof raw === "object" && !Array.isArray(raw) ? { ...raw } : {};
    const type = String(rule.type || "string").trim() || "string";
    const item = { type };
    if (rule.required === true) item.required = true;
    if (Object.prototype.hasOwnProperty.call(rule, "default")) item.default = rule.default;
    if (rule.description != null && String(rule.description).trim()) item.description = String(rule.description);
    out[k] = item;
  });
  return out;
}

function appSchemaRowsFromSchemaObject(schemaObj) {
  return Object.entries(normalizeAppSchemaObjectValue(schemaObj)).map(([key, rule]) => ({
    key,
    type: String(rule.type || "string"),
    required: rule.required === true,
    defaultText: Object.prototype.hasOwnProperty.call(rule, "default") ? JSON.stringify(rule.default) : "",
    description: String(rule.description || ""),
  }));
}

function parseSchemaDefaultValue(text) {
  const normalized = String(text || "").trim();
  if (!normalized) return undefined;
  try {
    return JSON.parse(normalized);
  } catch {
    return normalized;
  }
}

function collectAppSchemaRows(rows = []) {
  const out = {};
  Array.from(rows || []).forEach((row) => {
    const key = String(row.querySelector("[data-app-schema='key']")?.value || "").trim();
    const type = String(row.querySelector("[data-app-schema='type']")?.value || "string").trim() || "string";
    const required = !!row.querySelector("[data-app-schema='required']")?.checked;
    const defaultText = String(row.querySelector("[data-app-schema='default']")?.value || "").trim();
    const description = String(row.querySelector("[data-app-schema='description']")?.value || "").trim();
    if (!key) return;
    const item = { type };
    if (required) item.required = true;
    const parsedDefault = parseSchemaDefaultValue(defaultText);
    if (typeof parsedDefault !== "undefined") item.default = parsedDefault;
    if (description) item.description = description;
    out[key] = item;
  });
  return out;
}

export {
  appSchemaRowsFromSchemaObject,
  collectAppSchemaRows,
  normalizeAppSchemaObjectValue,
  parseSchemaDefaultValue,
};
