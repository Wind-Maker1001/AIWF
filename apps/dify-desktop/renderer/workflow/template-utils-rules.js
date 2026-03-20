function defaultTemplateParamValue(rule) {
  if (rule && Object.prototype.hasOwnProperty.call(rule, "default")) return rule.default;
  const t = String(rule?.type || "");
  if (t === "number") return 0;
  if (t === "boolean") return false;
  if (t === "object") return {};
  if (t === "array") return [];
  return "";
}

function matchesTemplateRuleCondition(cond, params) {
  if (!cond || typeof cond !== "object") return true;
  const key = String(cond.field || "").trim();
  if (!key) return true;
  const has = Object.prototype.hasOwnProperty.call(params || {}, key);
  const value = params ? params[key] : undefined;
  if (Object.prototype.hasOwnProperty.call(cond, "exists")) {
    if (!!cond.exists !== has) return false;
  }
  if (Object.prototype.hasOwnProperty.call(cond, "equals")) {
    if (value !== cond.equals) return false;
  }
  if (Object.prototype.hasOwnProperty.call(cond, "not_equals")) {
    if (value === cond.not_equals) return false;
  }
  if (Array.isArray(cond.in)) {
    if (!cond.in.some((x) => x === value)) return false;
  }
  if (Array.isArray(cond.not_in)) {
    if (cond.not_in.some((x) => x === value)) return false;
  }
  return true;
}

function resolveTemplateRule(rule, params) {
  const resolved = { ...(rule || {}) };
  if (rule && rule.depends_on) {
    const deps = Array.isArray(rule.depends_on) ? rule.depends_on : [rule.depends_on];
    resolved.__active = deps.every((d) => matchesTemplateRuleCondition(d, params));
  } else {
    resolved.__active = true;
  }
  if (Array.isArray(rule?.conditional)) {
    for (const c of rule.conditional) {
      if (!c || typeof c !== "object") continue;
      if (matchesTemplateRuleCondition(c.when || {}, params)) {
        Object.assign(resolved, c);
        break;
      }
    }
  }
  return resolved;
}

function validateTemplateParams(schema, params) {
  if (!schema || typeof schema !== "object") return;
  const errs = [];
  Object.entries(schema).forEach(([key, baseRule]) => {
    if (!baseRule || typeof baseRule !== "object") return;
    const rule = resolveTemplateRule(baseRule, params);
    if (rule.__active === false) return;
    const has = Object.prototype.hasOwnProperty.call(params, key);
    const val = params[key];
    if (rule.required && !has) {
      errs.push(`缺少参数: ${key}`);
      return;
    }
    if (!has) return;
    if (rule.type === "string") {
      if (typeof val !== "string") errs.push(`${key} 必须是字符串`);
      if (typeof val === "string" && Number.isFinite(rule.min_length) && val.length < Number(rule.min_length)) {
        errs.push(`${key} 长度不能小于 ${Number(rule.min_length)}`);
      }
    } else if (rule.type === "number") {
      if (typeof val !== "number" || !Number.isFinite(val)) {
        errs.push(`${key} 必须是数字`);
      } else {
        if (Number.isFinite(rule.min) && val < Number(rule.min)) errs.push(`${key} 不能小于 ${Number(rule.min)}`);
        if (Number.isFinite(rule.max) && val > Number(rule.max)) errs.push(`${key} 不能大于 ${Number(rule.max)}`);
      }
    } else if (rule.type === "boolean") {
      if (typeof val !== "boolean") errs.push(`${key} 必须是布尔值`);
    } else if (rule.type === "object") {
      if (!val || typeof val !== "object" || Array.isArray(val)) errs.push(`${key} 必须是对象`);
    } else if (rule.type === "array") {
      if (!Array.isArray(val)) errs.push(`${key} 必须是数组`);
    }
    if (Array.isArray(rule.enum) && rule.enum.length) {
      const ok = rule.enum.some((it) => it === val);
      if (!ok) errs.push(`${key} 必须是枚举值: ${rule.enum.join(", ")}`);
    }
  });
  if (errs.length) throw new Error(`模板参数校验失败: ${errs.join("; ")}`);
}

function applyTemplateVars(value, params) {
  if (Array.isArray(value)) return value.map((x) => applyTemplateVars(x, params));
  if (value && typeof value === "object") {
    const out = {};
    Object.entries(value).forEach(([k, v]) => {
      out[k] = applyTemplateVars(v, params);
    });
    return out;
  }
  if (typeof value !== "string") return value;
  const exact = value.match(/^\{\{\s*([A-Za-z0-9_]+)\s*\}\}$/);
  if (exact) {
    const key = exact[1];
    if (Object.prototype.hasOwnProperty.call(params, key)) return params[key];
    return value;
  }
  return value.replace(/\{\{\s*([A-Za-z0-9_]+)\s*\}\}/g, (_m, key) => {
    if (!Object.prototype.hasOwnProperty.call(params, key)) return `{{${key}}}`;
    return String(params[key]);
  });
}

export {
  applyTemplateVars,
  defaultTemplateParamValue,
  matchesTemplateRuleCondition,
  resolveTemplateRule,
  validateTemplateParams,
};
