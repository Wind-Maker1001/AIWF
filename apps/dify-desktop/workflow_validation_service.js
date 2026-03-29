const { createWorkflowStoreRemoteError } = require("./workflow_store_remote_error");

const DEFAULT_ACCEL_URL = "http://127.0.0.1:18082";
const WORKFLOW_GRAPH_CONTRACT_AUTHORITY = "contracts/workflow/workflow.schema.json";
const NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY = "contracts/desktop/node_config_validation_errors.v1.json";

function normalizeBaseUrl(url) {
  return String(url || "").trim().replace(/\/$/, "");
}

function parseResponseText(text) {
  const raw = String(text || "").trim();
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    return { ok: false, error: raw };
  }
}

function buildValidationErrorMessage(payload = {}, fallback = "workflow contract invalid") {
  const errorItems = Array.isArray(payload?.error_items) ? payload.error_items : [];
  const first = errorItems.find((item) => String(item?.message || "").trim());
  if (first) return String(first.message || "").trim();
  return String(payload?.error || fallback).trim() || fallback;
}

function createWorkflowValidationSupport(deps = {}) {
  const {
    loadConfig = () => ({}),
    fetchImpl = typeof fetch === "function" ? fetch : null,
    env = process.env,
  } = deps;

  function mergedConfig(cfg = null) {
    return { ...loadConfig(), ...(cfg && typeof cfg === "object" ? cfg : {}) };
  }

  function resolveAccelUrl(cfg = null, override = "") {
    const merged = mergedConfig(cfg);
    return normalizeBaseUrl(
      override
      || merged.accelUrl
      || merged.rustEndpoint
      || env.AIWF_ACCEL_URL
      || DEFAULT_ACCEL_URL,
    );
  }

  async function validateWorkflowDefinitionAuthoritatively(options = {}) {
    const workflowDefinition = options?.workflowDefinition && typeof options.workflowDefinition === "object"
      ? options.workflowDefinition
      : {};
    const validationScope = String(options?.validationScope || "authoring").trim() || "authoring";
    const baseUrl = resolveAccelUrl(options?.cfg || null, options?.rustEndpoint || "");

    if (typeof fetchImpl !== "function") {
      throw createWorkflowStoreRemoteError({
        ok: false,
        provider: "accel-rust",
        error: "workflow validation unavailable: fetch is not available",
        error_code: "workflow_validation_unavailable",
        validation_scope: validationScope,
      });
    }

    let resp;
    let payload = {};
    try {
      resp = await fetchImpl(`${baseUrl}/operators/workflow_contract_v1/validate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workflow_definition: workflowDefinition,
          allow_version_migration: options?.allowVersionMigration === true,
          require_non_empty_nodes: options?.requireNonEmptyNodes === true,
          validation_scope: validationScope,
        }),
      });
      payload = parseResponseText(await resp.text());
    } catch (error) {
      throw createWorkflowStoreRemoteError({
        ok: false,
        provider: "accel-rust",
        error: `workflow validation unavailable: ${String(error?.message || error || "unknown error")}`,
        error_code: "workflow_validation_unavailable",
        validation_scope: validationScope,
      });
    }

    if (!resp.ok || payload?.ok === false) {
      throw createWorkflowStoreRemoteError({
        ok: false,
        provider: "accel-rust",
        ...payload,
        error: String(payload?.error || `workflow validation unavailable: http ${resp.status}`),
        error_code: String(payload?.error_code || "workflow_validation_unavailable"),
        validation_scope: validationScope,
      });
    }

    if (payload?.valid === false || String(payload?.status || "").trim().toLowerCase() === "invalid") {
      throw createWorkflowStoreRemoteError({
        ok: false,
        provider: "accel-rust",
        error: buildValidationErrorMessage(payload),
        error_code: "workflow_graph_invalid",
        graph_contract: String(payload?.graph_contract || WORKFLOW_GRAPH_CONTRACT_AUTHORITY),
        error_item_contract: String(payload?.error_item_contract || NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY),
        error_items: Array.isArray(payload?.error_items) ? payload.error_items : [],
        notes: Array.isArray(payload?.notes) ? payload.notes : [],
        node_type_inventory: payload?.node_type_inventory && typeof payload.node_type_inventory === "object"
          ? payload.node_type_inventory
          : {},
        operator_resolutions: Array.isArray(payload?.operator_resolutions) ? payload.operator_resolutions : [],
        validation_scope: validationScope,
        normalized_workflow_definition: payload?.normalized_workflow_definition && typeof payload.normalized_workflow_definition === "object"
          ? payload.normalized_workflow_definition
          : workflowDefinition,
      });
    }

    return {
      ok: true,
      provider: "accel-rust",
      ...payload,
      validation_scope: validationScope,
      normalized_workflow_definition: payload?.normalized_workflow_definition && typeof payload.normalized_workflow_definition === "object"
        ? payload.normalized_workflow_definition
        : workflowDefinition,
      notes: Array.isArray(payload?.notes) ? payload.notes : [],
      error_items: Array.isArray(payload?.error_items) ? payload.error_items : [],
      node_type_inventory: payload?.node_type_inventory && typeof payload.node_type_inventory === "object"
        ? payload.node_type_inventory
        : {},
      operator_resolutions: Array.isArray(payload?.operator_resolutions) ? payload.operator_resolutions : [],
    };
  }

  return {
    resolveAccelUrl,
    validateWorkflowDefinitionAuthoritatively,
  };
}

module.exports = {
  createWorkflowValidationSupport,
};
