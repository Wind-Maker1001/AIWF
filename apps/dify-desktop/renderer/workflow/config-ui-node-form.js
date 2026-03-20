const NODE_CONFIG_GENERIC_FIELDS = [
  { key: "isolation_level", label: "隔离等级(none/process/sandbox)", type: "text" },
  { key: "sandbox_limits", label: "Sandbox限制(JSON对象)", type: "json" },
  { key: "input_map", label: "输入映射(JSON对象: 目标字段->来源路径)", type: "json" },
  { key: "output_map", label: "输出映射(JSON对象: 别名->输出路径)", type: "json" },
];

function buildNodeConfigFormSchema(nodeType, nodeFormSchemas = {}) {
  const schema = nodeFormSchemas[String(nodeType || "")] || [];
  return [...schema, ...NODE_CONFIG_GENERIC_FIELDS];
}

function prettyJson(value) {
  return JSON.stringify(value || {}, null, 2);
}

function parseNodeConfigTextValue(text) {
  const normalized = String(text || "").trim();
  if (!normalized) return {};
  let obj = null;
  try {
    obj = JSON.parse(normalized);
  } catch {
    throw new Error("配置必须是合法 JSON");
  }
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
    throw new Error("配置必须是 JSON 对象");
  }
  return obj;
}

function toFieldDisplayValue(type, value) {
  if (type === "json") return prettyJson(value ?? (Array.isArray(value) ? [] : {}));
  if (type === "csv") return Array.isArray(value) ? value.join(",") : "";
  if (type === "bool") return value === true ? "true" : "false";
  if (value === null || value === undefined) return "";
  return String(value);
}

function parseFieldValue(type, text) {
  const raw = String(text || "");
  if (type === "number") {
    const numberValue = Number(raw);
    if (!Number.isFinite(numberValue)) throw new Error(`数字字段格式错误: ${raw}`);
    return numberValue;
  }
  if (type === "csv") {
    return raw.split(",").map((item) => item.trim()).filter(Boolean);
  }
  if (type === "bool") {
    const lowered = raw.trim().toLowerCase();
    if (lowered === "true" || lowered === "1" || lowered === "yes") return true;
    if (lowered === "false" || lowered === "0" || lowered === "no" || lowered === "") return false;
    throw new Error(`布尔字段格式错误: ${raw}`);
  }
  if (type === "json") {
    const trimmed = raw.trim();
    if (!trimmed) return {};
    return JSON.parse(trimmed);
  }
  return raw;
}

function renderNodeConfigFormInto(formEl, node, cfg, schema = []) {
  if (!formEl) return;
  formEl.innerHTML = "";
  if (!node || !Array.isArray(schema) || !schema.length) {
    formEl.innerHTML = '<div class="cfg-help">该节点暂无预置表单，请切到 JSON 模式编辑。</div>';
    return;
  }
  schema.forEach((field) => {
    const wrap = document.createElement("div");
    wrap.className = "cfg-field";
    const label = document.createElement("label");
    label.textContent = field.label;
    const control = field.type === "json" ? document.createElement("textarea") : document.createElement("input");
    control.dataset.key = field.key;
    control.dataset.kind = field.type;
    if (field.type !== "json") control.type = "text";
    if (field.type === "number") control.inputMode = "numeric";
    control.value = toFieldDisplayValue(field.type, cfg?.[field.key]);
    wrap.append(label, control);
    formEl.appendChild(wrap);
  });
}

function parseNodeConfigFormElement(formEl, baseCfg = {}, schema = [], parseValue = parseFieldValue) {
  if (!Array.isArray(schema) || !schema.length) return { ...baseCfg };
  const out = { ...(baseCfg || {}) };
  const controls = formEl?.querySelectorAll ? formEl.querySelectorAll("[data-key]") : [];
  Array.from(controls || []).forEach((el) => {
    const key = String(el?.dataset?.key || "");
    const kind = String(el?.dataset?.kind || "text");
    if (!key) return;
    out[key] = parseValue(kind, el.value);
  });
  return out;
}

export {
  NODE_CONFIG_GENERIC_FIELDS,
  buildNodeConfigFormSchema,
  parseFieldValue,
  parseNodeConfigFormElement,
  parseNodeConfigTextValue,
  prettyJson,
  renderNodeConfigFormInto,
  toFieldDisplayValue,
};
