import {
  NODE_CONFIG_CONTRACTS_BY_TYPE,
  NODE_CONFIG_CONTRACT_QUALITY_BY_TYPE,
  NODE_CONFIG_CONTRACT_TYPES,
} from "./node_config_contract.generated.js";
import { findUnknownWorkflowNodeTypes } from "./node-catalog-contract.js";

const WORKFLOW_SCHEMA_VERSION = "1.0.0";
const WORKFLOW_CONTRACT_AUTHORITY = "contracts/workflow/workflow.schema.json";
const NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY = "contracts/desktop/node_config_validation_errors.v1.json";
const WORKFLOW_VALIDATION_GUIDANCE_SCHEMA_VERSION = "workflow_validation_guidance.v1";
const WORKFLOW_VALIDATION_GUIDANCE_AUTHORITY = "apps/dify-desktop/renderer/workflow/workflow-contract.js";
const WORKFLOW_VALIDATION_GUIDANCE_DEFAULTS_BY_CODE = Object.freeze({
  array_min_items: "请补齐最少数量要求。",
  conditional_required: "请补齐该字段依赖的必填搭配项。",
  empty_key: "请删除空键名，或补齐有效键名。",
  enum_not_allowed: "请改成契约允许的枚举值。",
  json_not_compatible: "请改成 JSON 兼容的数据结构。",
  membership_required: "请确保该值出现在允许列表中。",
  min_value: "请把该数值调整到契约允许范围。",
  missing_one_of: "请至少补齐一个必需识别字段。",
  paired_required: "请补齐该字段依赖的必填搭配项。",
  required: "请补齐该必填字段。",
  string_empty: "请填写非空字符串值。",
  type_array: "请修正该字段的数据类型。",
  type_boolean: "请修正该字段的数据类型。",
  type_integer: "请修正该字段的数据类型。",
  type_number: "请修正该字段的数据类型。",
  type_object: "请修正该字段的数据类型。",
  type_string: "请修正该字段的数据类型。",
  undefined_not_allowed: "请提供明确值，不要保留未定义状态。",
  unknown_node_type: "请替换节点类型，或先同步 Rust manifest / local node policy。",
  unsupported_validator_kind: "当前运行时还不支持该校验器，请先同步 contract/runtime。",
  validation_error: "请根据错误路径检查当前流程或节点配置。",
  workflow_contract_invalid: "请按流程契约修复当前 workflow。",
  workflow_load_invalid_json: "无法解析流程 JSON，请检查文件是否损坏或格式不正确。",
  workflow_load_failed: "加载流程失败，请检查文件是否存在或内容是否可读。",
});
const WORKFLOW_VALIDATION_GUIDANCE_PATH_OVERRIDES = Object.freeze([
  {
    code: "required",
    path: "workflow.version",
    resolution_hint: "请先把流程迁移到带顶层 version 的格式后再保存、运行或发布。",
  },
]);
const WORKFLOW_VALIDATION_GUIDANCE_OVERRIDE_MAP = Object.freeze({
  "required::workflow.version": "请先把流程迁移到带顶层 version 的格式后再保存、运行或发布。",
});
const WORKFLOW_REQUIRED_TOP_LEVEL_FIELDS = Object.freeze([
  "workflow_id",
  "version",
  "nodes",
  "edges",
]);
const WORKFLOW_VERSION_MIGRATION_NOTE = `workflow.version migrated to ${WORKFLOW_SCHEMA_VERSION}`;
const NODE_CONFIG_SCHEMA_IDS = NODE_CONFIG_CONTRACT_TYPES;
const NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE = NODE_CONFIG_CONTRACT_QUALITY_BY_TYPE;
const NODE_CONFIG_VALIDATION_ERROR_CODES = Object.freeze([
  "array_min_items",
  "conditional_required",
  "empty_key",
  "enum_not_allowed",
  "json_not_compatible",
  "membership_required",
  "min_value",
  "missing_one_of",
  "paired_required",
  "required",
  "string_empty",
  "type_array",
  "type_boolean",
  "type_integer",
  "type_number",
  "type_object",
  "type_string",
  "undefined_not_allowed",
  "unsupported_validator_kind",
  "unknown_node_type",
  "validation_error",
]);

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function isPlainObject(value) {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function validateOptionalBoolean(value, label, errors) {
  if (typeof value === "undefined") return;
  if (typeof value !== "boolean") errors.push(`${label} must be a boolean`);
}

function validateOptionalString(value, label, errors, options = {}) {
  if (typeof value === "undefined") return;
  if (typeof value !== "string") {
    errors.push(`${label} must be a string`);
    return;
  }
  if (options.nonEmpty && !value.trim()) {
    errors.push(`${label} must not be empty`);
  }
}

function validateOptionalEnum(value, allowed, label, errors) {
  if (typeof value === "undefined") return;
  if (typeof value !== "string") {
    errors.push(`${label} must be a string`);
    return;
  }
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    errors.push(`${label} must not be empty`);
    return;
  }
  if (!allowed.includes(normalized)) {
    errors.push(`${label} must be one of: ${allowed.join(", ")}`);
  }
}

function validateOptionalArray(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!Array.isArray(value)) errors.push(`${label} must be an array`);
}

function validateOptionalObject(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!isPlainObject(value)) errors.push(`${label} must be an object`);
}

function validateJsonCompatibleValue(value, label, errors) {
  if (typeof value === "undefined") return;
  if (value === null) return;
  if (Array.isArray(value)) {
    value.forEach((item, index) => validateJsonCompatibleValue(item, `${label}[${index}]`, errors));
    return;
  }
  if (isPlainObject(value)) {
    Object.keys(value).forEach((key) => {
      if (!String(key || "").trim()) {
        errors.push(`${label} keys must not be empty`);
        return;
      }
      validateJsonCompatibleValue(value[key], `${label}.${key}`, errors);
    });
    return;
  }
  if (["string", "number", "boolean"].includes(typeof value)) return;
  errors.push(`${label} must be JSON-compatible`);
}

function validateOptionalJsonObject(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!isPlainObject(value)) {
    errors.push(`${label} must be an object`);
    return;
  }
  Object.keys(value).forEach((key) => {
    if (!String(key || "").trim()) {
      errors.push(`${label} keys must not be empty`);
      return;
    }
    validateJsonCompatibleValue(value[key], `${label}.${key}`, errors);
  });
}

function validateOptionalRowObjects(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!Array.isArray(value)) {
    errors.push(`${label} must be an array`);
    return;
  }
  value.forEach((item, index) => {
    if (!isPlainObject(item)) {
      errors.push(`${label}[${index}] must be an object`);
    }
  });
}

function validateOptionalStringArray(value, label, errors, options = {}) {
  if (typeof value === "undefined") return;
  if (!Array.isArray(value)) {
    errors.push(`${label} must be an array`);
    return;
  }
  value.forEach((item, index) => {
    if (typeof item !== "string") {
      errors.push(`${label}[${index}] must be a string`);
      return;
    }
    if (options.nonEmpty && !item.trim()) {
      errors.push(`${label}[${index}] must not be empty`);
    }
  });
}

function validateOptionalInteger(value, label, errors, options = {}) {
  if (typeof value === "undefined") return;
  if (!Number.isInteger(Number(value))) {
    errors.push(`${label} must be an integer`);
    return;
  }
  const num = Number(value);
  if (typeof options.min === "number" && num < options.min) {
    errors.push(`${label} must be >= ${options.min}`);
  }
}

function validateOptionalNumber(value, label, errors, options = {}) {
  if (typeof value === "undefined") return;
  if (!Number.isFinite(Number(value))) {
    errors.push(`${label} must be a number`);
    return;
  }
  const num = Number(value);
  if (typeof options.min === "number" && num < options.min) {
    errors.push(`${label} must be >= ${options.min}`);
  }
}

function validateRulesObject(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!isPlainObject(value)) {
    errors.push(`${label} must be an object`);
    return;
  }
  validateOptionalStringArray(value.required_columns, `${label}.required_columns`, errors, { nonEmpty: true });
  validateOptionalStringArray(value.forbidden_columns, `${label}.forbidden_columns`, errors, { nonEmpty: true });
  validateOptionalStringArray(value.unique_columns, `${label}.unique_columns`, errors, { nonEmpty: true });
}

function validateManifestObject(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!isPlainObject(value)) {
    errors.push(`${label} must be an object`);
    return;
  }
  validateOptionalString(value.name, `${label}.name`, errors, { nonEmpty: true });
  validateOptionalString(value.version, `${label}.version`, errors, { nonEmpty: true });
  validateOptionalString(value.api_version, `${label}.api_version`, errors, { nonEmpty: true });
  validateOptionalString(value.entry, `${label}.entry`, errors, { nonEmpty: true });
  validateOptionalString(value.command, `${label}.command`, errors, { nonEmpty: true });
  validateOptionalBoolean(value.enabled, `${label}.enabled`, errors);
  validateOptionalStringArray(value.capabilities, `${label}.capabilities`, errors, { nonEmpty: true });
  validateOptionalStringArray(value.args, `${label}.args`, errors);
}

function validateComputedFields(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!Array.isArray(value)) {
    errors.push(`${label} must be an array`);
    return;
  }
  value.forEach((item, index) => {
    if (!isPlainObject(item)) {
      errors.push(`${label}[${index}] must be an object`);
      return;
    }
    const target = String(item.as || item.name || item.field || "").trim();
    if (!target) {
      errors.push(`${label}[${index}] requires one of as/name/field`);
    }
    if ("expr" in item && typeof item.expr !== "string") {
      errors.push(`${label}[${index}].expr must be a string`);
    }
  });
}

function validateWorkflowSteps(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!Array.isArray(value)) {
    errors.push(`${label} must be an array`);
    return;
  }
  value.forEach((item, index) => {
    if (!isPlainObject(item)) {
      errors.push(`${label}[${index}] must be an object`);
      return;
    }
    if (!String(item.id || "").trim()) errors.push(`${label}[${index}].id is required`);
    validateOptionalStringArray(item.depends_on, `${label}[${index}].depends_on`, errors, { nonEmpty: true });
    validateOptionalString(item.operator, `${label}[${index}].operator`, errors);
  });
}

function validateConstraintDefs(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!Array.isArray(value)) {
    errors.push(`${label} must be an array`);
    return;
  }
  value.forEach((item, index) => {
    if (!isPlainObject(item)) {
      errors.push(`${label}[${index}] must be an object`);
      return;
    }
    const kind = String(item.kind || "").trim().toLowerCase();
    if (!kind) {
      errors.push(`${label}[${index}].kind is required`);
      return;
    }
    if (!["sum_equals", "non_negative"].includes(kind)) {
      errors.push(`${label}[${index}].kind must be one of: sum_equals, non_negative`);
      return;
    }
    if (kind === "sum_equals") {
      validateOptionalStringArray(item.left, `${label}[${index}].left`, errors, { nonEmpty: true });
      validateOptionalString(item.right, `${label}[${index}].right`, errors, { nonEmpty: true });
      validateOptionalNumber(item.tolerance, `${label}[${index}].tolerance`, errors, { min: 0 });
    }
    if (kind === "non_negative") {
      validateOptionalString(item.field, `${label}[${index}].field`, errors, { nonEmpty: true });
    }
  });
}

function validateAggregateDefs(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!Array.isArray(value)) {
    errors.push(`${label} must be an array`);
    return;
  }
  value.forEach((item, index) => {
    if (!isPlainObject(item)) {
      errors.push(`${label}[${index}] must be an object`);
      return;
    }
    if (!String(item.op || "").trim()) errors.push(`${label}[${index}].op is required`);
    if (!String(item.as || "").trim()) errors.push(`${label}[${index}].as is required`);
  });
}

function validateAiProviders(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!Array.isArray(value)) {
    errors.push(`${label} must be an array`);
    return;
  }
  value.forEach((item, index) => {
    if (!isPlainObject(item)) {
      errors.push(`${label}[${index}] must be an object`);
      return;
    }
    const identity = String(item.name || item.model || item.endpoint || "").trim();
    if (!identity) {
      errors.push(`${label}[${index}] requires one of name/model/endpoint`);
    }
    validateOptionalString(item.name, `${label}[${index}].name`, errors);
    validateOptionalString(item.model, `${label}[${index}].model`, errors);
    validateOptionalString(item.endpoint, `${label}[${index}].endpoint`, errors);
  });
}

function validateWindowFunctions(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!Array.isArray(value)) {
    errors.push(`${label} must be an array`);
    return;
  }
  value.forEach((item, index) => {
    if (!isPlainObject(item)) {
      errors.push(`${label}[${index}] must be an object`);
      return;
    }
    if (!String(item.op || "").trim()) errors.push(`${label}[${index}].op is required`);
    if (!String(item.as || "").trim()) errors.push(`${label}[${index}].as is required`);
  });
}

function validateSlotBindings(value, label, errors) {
  if (typeof value === "undefined") return;
  if (!isPlainObject(value)) {
    errors.push(`${label} must be an object`);
    return;
  }
  Object.keys(value).forEach((key) => {
    if (!String(key || "").trim()) {
      errors.push(`${label} keys must not be empty`);
      return;
    }
    const slotValue = value[key];
    if (typeof slotValue === "undefined") {
      errors.push(`${label}.${key} must not be undefined`);
      return;
    }
    if (key === "chart_main" && isPlainObject(slotValue)) {
      validateOptionalArray(slotValue.categories, `${label}.${key}.categories`, errors);
      validateOptionalArray(slotValue.series, `${label}.${key}.series`, errors);
    }
  });
}

function getConfigValueAtPath(config, rawPath) {
  const path = String(rawPath || "").trim();
  if (!path) return undefined;
  return path.split(".").reduce((current, segment) => {
    if (!segment) return current;
    if (!isPlainObject(current)) return undefined;
    return current[segment];
  }, config);
}

function validateConditionalRequiredNonEmpty(config, rule, label, prefix, errors) {
  const expected = Array.isArray(rule?.one_of)
    ? rule.one_of.map((item) => String(item || "").trim().toLowerCase()).filter(Boolean)
    : [];
  const whenValue = String(getConfigValueAtPath(config, rule?.when_path) || "").trim().toLowerCase();
  if (!whenValue || !expected.includes(whenValue)) return;
  const value = getConfigValueAtPath(config, rule?.path);
  if (!String(value || "").trim()) {
    errors.push(`${label} is required when ${prefix}.${String(rule?.when_path || "").trim()} is ${whenValue}`);
  }
}

function validatePairedRequired(config, rule, label, prefix, errors) {
  const value = getConfigValueAtPath(config, rule?.path);
  const pairedValue = getConfigValueAtPath(config, rule?.paired_path);
  const hasValue = typeof value !== "undefined" && value !== null && (!String(value).trim || !!String(value).trim());
  const hasPairedValue = typeof pairedValue !== "undefined";
  if (!hasValue && hasPairedValue) {
    errors.push(`${label} is required when ${prefix}.${String(rule?.paired_path || "").trim()} is provided`);
    return;
  }
  if (hasValue && !hasPairedValue) {
    errors.push(`${prefix}.${String(rule?.paired_path || "").trim()} is required when ${label} is provided`);
  }
}

function validateOpInAllowedOps(config, rule, label, prefix, errors) {
  const op = String(getConfigValueAtPath(config, rule?.path) || "").trim().toLowerCase();
  const allowed = getConfigValueAtPath(config, rule?.allowed_path);
  const normalizedAllowed = Array.isArray(allowed)
    ? allowed.map((item) => String(item || "").trim().toLowerCase()).filter(Boolean)
    : [];
  if (op && normalizedAllowed.length > 0 && !normalizedAllowed.includes(op)) {
    errors.push(`${label} must be included in ${prefix}.${String(rule?.allowed_path || "").trim()} when both are provided`);
  }
}

function validateContractBackedNodeConfig(type, config, prefix, errors) {
  const contract = NODE_CONFIG_CONTRACTS_BY_TYPE[String(type || "").trim()];
  if (!contract) return false;
  const validators = Array.isArray(contract.validators) ? contract.validators : [];
  validators.forEach((rule, index) => {
    const kind = String(rule?.kind || "").trim();
    const path = String(rule?.path || "").trim();
    const value = getConfigValueAtPath(config, path);
    const label = path ? `${prefix}.${path}` : `${prefix}.contract[${index}]`;
    if (kind === "boolean") validateOptionalBoolean(value, label, errors);
    else if (kind === "string") validateOptionalString(value, label, errors);
    else if (kind === "string_non_empty") validateOptionalString(value, label, errors, { nonEmpty: true });
    else if (kind === "enum") validateOptionalEnum(value, Array.isArray(rule?.allowed) ? rule.allowed : [], label, errors);
    else if (kind === "array") validateOptionalArray(value, label, errors);
    else if (kind === "object") validateOptionalObject(value, label, errors);
    else if (kind === "row_objects") validateOptionalRowObjects(value, label, errors);
    else if (kind === "string_array_non_empty") validateOptionalStringArray(value, label, errors, { nonEmpty: true });
    else if (kind === "integer_min") validateOptionalInteger(value, label, errors, { min: Number(rule?.min || 0) });
    else if (kind === "json_object") validateOptionalJsonObject(value, label, errors);
    else if (kind === "json_compatible") validateJsonCompatibleValue(value, label, errors);
    else if (kind === "rules_object") validateRulesObject(value, label, errors);
    else if (kind === "computed_fields") validateComputedFields(value, label, errors);
    else if (kind === "workflow_steps") validateWorkflowSteps(value, label, errors);
    else if (kind === "constraint_defs") validateConstraintDefs(value, label, errors);
    else if (kind === "aggregate_defs") validateAggregateDefs(value, label, errors);
    else if (kind === "window_functions") validateWindowFunctions(value, label, errors);
    else if (kind === "slot_bindings") validateSlotBindings(value, label, errors);
    else if (kind === "manifest_object") validateManifestObject(value, label, errors);
    else if (kind === "ai_providers") validateAiProviders(value, label, errors);
    else if (kind === "conditional_required_non_empty") validateConditionalRequiredNonEmpty(config, rule, label, prefix, errors);
    else if (kind === "paired_required") validatePairedRequired(config, rule, label, prefix, errors);
    else if (kind === "op_in_allowed_ops") validateOpInAllowedOps(config, rule, label, prefix, errors);
    else errors.push(`${prefix}.contract[${index}] validator kind unsupported: ${kind}`);
  });
  return true;
}

function validateNodeConfig(node, index) {
  const config = typeof node?.config === "undefined" ? {} : node?.config;
  const prefix = `workflow.nodes[${index}].config`;
  const errors = [];

  if (!isPlainObject(config)) {
    errors.push(`${prefix} must be an object`);
    return errors;
  }

  validateContractBackedNodeConfig(String(node?.type || "").trim(), config, prefix, errors);
  return errors;
}

function validateWorkflowTopLevel(graph, options = {}) {
  const source = graph && typeof graph === "object" ? graph : null;
  const requireNonEmptyNodes = options?.requireNonEmptyNodes === true;
  const skipNodeCatalogValidation = options?.skipNodeCatalogValidation === true;
  const errors = [];

  if (!source) {
    errors.push("workflow must be an object");
    return { ok: false, errors };
  }
  if (!String(source.workflow_id || "").trim()) errors.push("workflow.workflow_id is required");
  if (!String(source.version || "").trim()) errors.push("workflow.version is required");
  if (!Array.isArray(source.nodes)) errors.push("workflow.nodes must be an array");
  if (!Array.isArray(source.edges)) errors.push("workflow.edges must be an array");

  if (Array.isArray(source.nodes)) {
    if (requireNonEmptyNodes && source.nodes.length === 0) {
      errors.push("workflow.nodes must contain at least one node");
    }
    source.nodes.forEach((node, index) => {
      if (!String(node?.id || "").trim()) errors.push(`workflow.nodes[${index}].id is required`);
      if (!String(node?.type || "").trim()) errors.push(`workflow.nodes[${index}].type is required`);
      errors.push(...validateNodeConfig(node, index));
    });
    if (!skipNodeCatalogValidation) {
      const unknownNodeTypes = findUnknownWorkflowNodeTypes(source);
      if (unknownNodeTypes.length > 0) {
        errors.push(`workflow contains unregistered node types: ${unknownNodeTypes.join(", ")}`);
      }
    }
  }

  if (Array.isArray(source.edges)) {
    source.edges.forEach((edge, index) => {
      if (!String(edge?.from || "").trim()) errors.push(`workflow.edges[${index}].from is required`);
      if (!String(edge?.to || "").trim()) errors.push(`workflow.edges[${index}].to is required`);
    });
  }

  return { ok: errors.length === 0, errors };
}

function normalizeValidationErrorItem(message) {
  const text = String(message || "").trim();
  if (/^workflow contains unregistered node types:/i.test(text)) {
    return { path: "workflow.nodes", code: "unknown_node_type", message: text };
  }
  let path = text;
  let code = "validation_error";

  const pathPatterns = [
    [/^(.*?) keys must not be empty$/, 1],
    [/^(.*?) must be included in .* when both are provided$/, 1],
    [/^(.*?) is required when .*$/, 1],
    [/^(.*?) requires one of .*$/, 1],
    [/^(.*?) must contain at least one node$/, 1],
    [/^(.*?) must not be empty$/, 1],
    [/^(.*?) must be .*$/, 1],
    [/^(.*?) is required$/, 1],
  ];
  for (const [pattern, groupIndex] of pathPatterns) {
    const match = text.match(pattern);
    if (match) {
      path = String(match[groupIndex] || "").trim() || text;
      break;
    }
  }

  if (/ must be a boolean$/.test(text)) code = "type_boolean";
  else if (/ must be a string$/.test(text)) code = "type_string";
  else if (/ must not be empty$/.test(text)) code = "string_empty";
  else if (/ must be one of: /.test(text)) code = "enum_not_allowed";
  else if (/ must be an array$/.test(text)) code = "type_array";
  else if (/ must contain at least one node$/.test(text)) code = "array_min_items";
  else if (/ must be an object$/.test(text)) code = "type_object";
  else if (/ keys must not be empty$/.test(text)) code = "empty_key";
  else if (/ must be JSON-compatible$/.test(text)) code = "json_not_compatible";
  else if (/ must be an integer$/.test(text)) code = "type_integer";
  else if (/ must be a number$/.test(text)) code = "type_number";
  else if (/ must be >= /.test(text)) code = "min_value";
  else if (/ requires one of /.test(text)) code = "missing_one_of";
  else if (/ is required when .* is provided$/.test(text)) code = "paired_required";
  else if (/ is required when /.test(text)) code = "conditional_required";
  else if (/ must be included in .* when both are provided$/.test(text)) code = "membership_required";
  else if (/ validator kind unsupported: /.test(text)) code = "unsupported_validator_kind";
  else if (/ must not be undefined$/.test(text)) code = "undefined_not_allowed";
  else if (/ is required$/.test(text)) code = "required";

  return { path, code, message: text };
}

function buildValidationErrorItems(errors) {
  return Array.isArray(errors)
    ? errors
        .filter(Boolean)
        .map((value) => normalizeValidationErrorItem(value))
    : [];
}

function getWorkflowContractResolutionHint(item = {}) {
  const code = String(item?.code || "").trim();
  const path = String(item?.path || "").trim();
  const overrideKey = `${code}::${path}`;
  if (overrideKey && Object.prototype.hasOwnProperty.call(WORKFLOW_VALIDATION_GUIDANCE_OVERRIDE_MAP, overrideKey)) {
    return String(WORKFLOW_VALIDATION_GUIDANCE_OVERRIDE_MAP[overrideKey] || "").trim();
  }
  return String(WORKFLOW_VALIDATION_GUIDANCE_DEFAULTS_BY_CODE[code] || "").trim();
}

function describeWorkflowContractIssue(item = {}) {
  const path = String(item?.path || "").trim();
  const code = String(item?.code || "").trim();
  const message = String(item?.message || "").trim();
  const resolution = getWorkflowContractResolutionHint(item);
  return {
    path,
    code,
    message,
    resolution_hint: resolution,
    summary: `[${code || "validation_error"}] ${path || message}`.trim(),
  };
}

function createWorkflowContractError(errors) {
  const details = Array.isArray(errors) ? errors.filter(Boolean).map((value) => String(value)) : [];
  const error = new Error(`workflow contract invalid: ${details.join("; ") || "unknown error"}`);
  error.name = "WorkflowContractError";
  error.code = "workflow_contract_invalid";
  error.details = {
    errors: details,
    error_items: buildValidationErrorItems(details),
    graph_contract: WORKFLOW_CONTRACT_AUTHORITY,
    error_item_contract: NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY,
  };
  return error;
}

function formatWorkflowContractError(error) {
  const items = Array.isArray(error?.details?.error_items)
    ? error.details.error_items
    : (Array.isArray(error?.error_items) ? error.error_items : []);
  const first = items.find((item) => String(item?.code || "").trim());
  if (first) {
    const described = describeWorkflowContractIssue(first);
    return described.resolution_hint
      ? `${described.summary} | ${described.resolution_hint}`
      : described.summary;
  }
  const errorCode = String(error?.error_code || error?.code || "").trim();
  if (errorCode === "workflow_load_invalid_json") {
    return `[${errorCode}] 无法解析流程 JSON，请检查文件是否损坏或格式不正确。`;
  }
  if (errorCode) {
    return `[${errorCode}] ${String(error?.error || error?.message || "unknown error")}`;
  }
  return String(error?.error || error?.message || error || "workflow contract invalid");
}

const formatAiwfError = formatWorkflowContractError;

function normalizeWorkflowContract(graph, options = {}) {
  const source = graph && typeof graph === "object" ? graph : {};
  const allowVersionMigration = options?.allowVersionMigration === true;
  const validation = validateWorkflowTopLevel(source, {
    requireNonEmptyNodes: options?.requireNonEmptyNodes === true,
    skipNodeCatalogValidation: options?.skipNodeCatalogValidation === true,
  });
  const errors = validation.errors.filter((error) => error !== "workflow.version is required");
  const notes = [];
  let version = String(source.version || "").trim();

  if (!version) {
    if (!allowVersionMigration) {
      errors.push("workflow.version is required");
    } else {
      version = WORKFLOW_SCHEMA_VERSION;
      notes.push(WORKFLOW_VERSION_MIGRATION_NOTE);
    }
  }

  if (errors.length > 0) {
    return {
      ok: false,
      migrated: false,
      notes,
      errors,
      graph: null,
    };
  }

  return {
    ok: true,
    migrated: notes.length > 0,
    notes,
    errors: [],
    graph: {
      ...source,
      workflow_id: String(source.workflow_id || "").trim(),
      version,
      nodes: cloneJson(Array.isArray(source.nodes) ? source.nodes : []),
      edges: cloneJson(Array.isArray(source.edges) ? source.edges : []),
    },
  };
}

function assertWorkflowContract(graph, options = {}) {
  const result = normalizeWorkflowContract(graph, options);
  if (!result.ok) throw createWorkflowContractError(result.errors);
  return result;
}

function combineWorkflowMigrationReports(...reports) {
  const notes = [];
  const errors = [];
  let migrated = false;

  reports
    .filter((report) => report && typeof report === "object")
    .forEach((report) => {
      if (report.migrated) migrated = true;
      const reportNotes = Array.isArray(report.notes) ? report.notes : [];
      const reportErrors = Array.isArray(report.errors) ? report.errors : [];
      reportNotes.forEach((note) => {
        const text = String(note || "").trim();
        if (text && !notes.includes(text)) notes.push(text);
      });
      reportErrors.forEach((error) => {
        const text = String(error || "").trim();
        if (text && !errors.includes(text)) errors.push(text);
      });
    });

  return { migrated, notes, errors };
}

export {
  NODE_CONFIG_SCHEMA_IDS,
  NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE,
  NODE_CONFIG_VALIDATION_ERROR_CODES,
  NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY,
  WORKFLOW_VALIDATION_GUIDANCE_AUTHORITY,
  WORKFLOW_VALIDATION_GUIDANCE_DEFAULTS_BY_CODE,
  WORKFLOW_VALIDATION_GUIDANCE_OVERRIDE_MAP,
  WORKFLOW_VALIDATION_GUIDANCE_PATH_OVERRIDES,
  WORKFLOW_VALIDATION_GUIDANCE_SCHEMA_VERSION,
  WORKFLOW_CONTRACT_AUTHORITY,
  WORKFLOW_REQUIRED_TOP_LEVEL_FIELDS,
  WORKFLOW_SCHEMA_VERSION,
  WORKFLOW_VERSION_MIGRATION_NOTE,
  assertWorkflowContract,
  combineWorkflowMigrationReports,
  createWorkflowContractError,
  buildValidationErrorItems,
  describeWorkflowContractIssue,
  formatAiwfError,
  formatWorkflowContractError,
  getWorkflowContractResolutionHint,
  normalizeWorkflowContract,
  normalizeValidationErrorItem,
  validateWorkflowTopLevel,
};
