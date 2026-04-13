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
  membership_required: "请确认该值出现在允许列表中。",
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
const WORKFLOW_VERSION_MIGRATION_NOTE = `workflow.version migrated to ${WORKFLOW_SCHEMA_VERSION}`;

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
    ? errors.filter(Boolean).map((value) => normalizeValidationErrorItem(value))
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
  NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY,
  WORKFLOW_VALIDATION_GUIDANCE_AUTHORITY,
  WORKFLOW_VALIDATION_GUIDANCE_DEFAULTS_BY_CODE,
  WORKFLOW_VALIDATION_GUIDANCE_OVERRIDE_MAP,
  WORKFLOW_VALIDATION_GUIDANCE_PATH_OVERRIDES,
  WORKFLOW_VALIDATION_GUIDANCE_SCHEMA_VERSION,
  WORKFLOW_CONTRACT_AUTHORITY,
  WORKFLOW_SCHEMA_VERSION,
  WORKFLOW_VERSION_MIGRATION_NOTE,
  buildValidationErrorItems,
  combineWorkflowMigrationReports,
  createWorkflowContractError,
  describeWorkflowContractIssue,
  formatAiwfError,
  formatWorkflowContractError,
  getWorkflowContractResolutionHint,
  normalizeValidationErrorItem,
};
