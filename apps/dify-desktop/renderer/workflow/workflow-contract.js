const WORKFLOW_SCHEMA_VERSION = "1.0.0";
const WORKFLOW_CONTRACT_AUTHORITY = "contracts/workflow/workflow.schema.json";
const NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY = "contracts/desktop/node_config_validation_errors.v1.json";
const WORKFLOW_VALIDATION_GUIDANCE_SCHEMA_VERSION = "workflow_validation_guidance.v1";
const WORKFLOW_VALIDATION_GUIDANCE_AUTHORITY = "apps/dify-desktop/renderer/workflow/workflow-contract.js";
const WORKFLOW_VALIDATION_GUIDANCE_DEFAULTS_BY_CODE = Object.freeze({
  array_min_items: "璇疯ˉ榻愭渶灏戞暟閲忚姹傘€?",
  conditional_required: "璇疯ˉ榻愯瀛楁渚濊禆鐨勫繀濉惌閰嶉」銆?",
  empty_key: "璇峰垹闄ょ┖閿悕锛屾垨琛ラ綈鏈夋晥閿悕銆?",
  enum_not_allowed: "璇锋敼鎴愬绾﹀厑璁哥殑鏋氫妇鍊笺€?",
  json_not_compatible: "璇锋敼鎴?JSON 鍏煎鐨勬暟鎹粨鏋勩€?",
  membership_required: "璇风‘淇濊鍊煎嚭鐜板湪鍏佽鍒楄〃涓€?",
  min_value: "璇锋妸璇ユ暟鍊艰皟鏁村埌濂戠害鍏佽鑼冨洿銆?",
  missing_one_of: "璇疯嚦灏戣ˉ榻愪竴涓繀闇€璇嗗埆瀛楁銆?",
  paired_required: "璇疯ˉ榻愯瀛楁渚濊禆鐨勫繀濉惌閰嶉」銆?",
  required: "璇疯ˉ榻愯蹇呭～瀛楁銆?",
  string_empty: "璇峰～鍐欓潪绌哄瓧绗︿覆鍊笺€?",
  type_array: "璇蜂慨姝ｈ瀛楁鐨勬暟鎹被鍨嬨€?",
  type_boolean: "璇蜂慨姝ｈ瀛楁鐨勬暟鎹被鍨嬨€?",
  type_integer: "璇蜂慨姝ｈ瀛楁鐨勬暟鎹被鍨嬨€?",
  type_number: "璇蜂慨姝ｈ瀛楁鐨勬暟鎹被鍨嬨€?",
  type_object: "璇蜂慨姝ｈ瀛楁鐨勬暟鎹被鍨嬨€?",
  type_string: "璇蜂慨姝ｈ瀛楁鐨勬暟鎹被鍨嬨€?",
  undefined_not_allowed: "璇锋彁渚涙槑纭€硷紝涓嶈淇濈暀鏈畾涔夌姸鎬併€?",
  unknown_node_type: "璇锋浛鎹㈣妭鐐圭被鍨嬶紝鎴栧厛鍚屾 Rust manifest / local node policy銆?",
  unsupported_validator_kind: "褰撳墠杩愯鏃惰繕涓嶆敮鎸佽鏍￠獙鍣紝璇峰厛鍚屾 contract/runtime銆?",
  validation_error: "璇锋牴鎹敊璇矾寰勬鏌ュ綋鍓嶆祦绋嬫垨鑺傜偣閰嶇疆銆?",
  workflow_contract_invalid: "璇锋寜娴佺▼濂戠害淇褰撳墠 workflow銆?",
  workflow_load_invalid_json: "鏃犳硶瑙ｆ瀽娴佺▼ JSON锛岃妫€鏌ユ枃浠舵槸鍚︽崯鍧忔垨鏍煎紡涓嶆纭€?",
  workflow_load_failed: "鍔犺浇娴佺▼澶辫触锛岃妫€鏌ユ枃浠舵槸鍚﹀瓨鍦ㄦ垨鍐呭鏄惁鍙銆?",
});
const WORKFLOW_VALIDATION_GUIDANCE_PATH_OVERRIDES = Object.freeze([
  {
    code: "required",
    path: "workflow.version",
    resolution_hint: "璇峰厛鎶婃祦绋嬭縼绉诲埌甯﹂《灞?version 鐨勬牸寮忓悗鍐嶄繚瀛樸€佽繍琛屾垨鍙戝竷銆?",
  },
]);
const WORKFLOW_VALIDATION_GUIDANCE_OVERRIDE_MAP = Object.freeze({
  "required::workflow.version": "璇峰厛鎶婃祦绋嬭縼绉诲埌甯﹂《灞?version 鐨勬牸寮忓悗鍐嶄繚瀛樸€佽繍琛屾垨鍙戝竷銆?",
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
    return `[${errorCode}] 鏃犳硶瑙ｆ瀽娴佺▼ JSON锛岃妫€鏌ユ枃浠舵槸鍚︽崯鍧忔垨鏍煎紡涓嶆纭€?`;
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
