const { TEMPLATE_PACK_ENTRY_SCHEMA_VERSION } = require("./workflow_ipc_state");

const TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION = "template_pack_artifact.v1";
const TEMPLATE_WORKFLOW_DEFINITION_FIELD = "workflow_definition";

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function createTemplatePackContractError(message, code = "template_pack_contract_invalid", notes = []) {
  const error = new Error(String(message || "template pack contract invalid"));
  error.code = code;
  error.notes = Array.isArray(notes) ? notes.slice() : [];
  return error;
}

function resolveTemplateWorkflowDefinition(source) {
  const candidate = source?.workflow_definition !== undefined
    ? source.workflow_definition
    : source?.graph;
  return candidate && typeof candidate === "object" ? clone(candidate) : null;
}

function normalizeTemplatePackTemplate(template, index = 0) {
  const source = template && typeof template === "object" ? template : {};
  const { graph: _legacyGraph, workflow_definition: _legacyWorkflowDefinition, ...rest } = source;
  const id = String(source.id || `tpl_${index + 1}`).trim();
  const name = String(source.name || id || `template_${index + 1}`).trim();
  const workflowDefinition = resolveTemplateWorkflowDefinition(source);
  if (!id) throw createTemplatePackContractError(`template[${index}] id is required`);
  if (!name) throw createTemplatePackContractError(`template[${index}] name is required`);
  if (!workflowDefinition) throw createTemplatePackContractError(`template[${index}] workflow_definition is required`);
  return {
    ...rest,
    id,
    name,
    workflow_definition: workflowDefinition,
    template_spec_version: Number(source.template_spec_version || 1),
    params_schema: source.params_schema && typeof source.params_schema === "object" && !Array.isArray(source.params_schema)
      ? clone(source.params_schema)
      : {},
    governance: source.governance && typeof source.governance === "object" && !Array.isArray(source.governance)
      ? clone(source.governance)
      : {},
    runtime_defaults: source.runtime_defaults && typeof source.runtime_defaults === "object" && !Array.isArray(source.runtime_defaults)
      ? clone(source.runtime_defaults)
      : {},
    created_at: String(source.created_at || "").trim(),
  };
}

function normalizeTemplatePackArtifact(pack, options = {}) {
  const {
    allowVersionMigration = true,
    source = "",
  } = options;
  const raw = pack && typeof pack === "object" ? pack : null;
  if (!raw) {
    throw createTemplatePackContractError("template pack must be an object");
  }

  const notes = [];
  const rawSchemaVersion = String(raw.schema_version || "").trim();
  if (!rawSchemaVersion) {
    if (!allowVersionMigration) {
      throw createTemplatePackContractError("template pack schema_version is required");
    }
    notes.push("template pack schema_version migrated to template_pack_artifact.v1");
  } else if (rawSchemaVersion !== TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION) {
    throw createTemplatePackContractError(`unsupported template pack schema_version: ${rawSchemaVersion}`);
  }

  const templates = Array.isArray(raw.templates) ? raw.templates.map(normalizeTemplatePackTemplate) : [];
  if (!templates.length) {
    throw createTemplatePackContractError("template pack templates are required");
  }

  const id = String(raw.id || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`).trim();
  const name = String(raw.name || id).trim();
  if (!id) throw createTemplatePackContractError("template pack id is required");
  if (!name) throw createTemplatePackContractError("template pack name is required");

  return {
    schema_version: TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION,
    id,
    name,
    version: String(raw.version || "v1").trim() || "v1",
    source: String(raw.source || source || "artifact").trim() || "artifact",
    templates,
    created_at: String(raw.created_at || "").trim(),
    notes,
    migrated: notes.length > 0,
  };
}

function exportTemplatePackArtifact(entry, options = {}) {
  const source = entry && typeof entry === "object" ? entry : {};
  const rawSchemaVersion = String(source.schema_version || "").trim();
  let normalized;
  if (!rawSchemaVersion || rawSchemaVersion === TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION) {
    normalized = normalizeTemplatePackArtifact(source, {
      allowVersionMigration: true,
      source: options.source || "marketplace_export",
    });
  } else if (rawSchemaVersion === TEMPLATE_PACK_ENTRY_SCHEMA_VERSION) {
      const templates = Array.isArray(source.templates)
      ? source.templates.map(normalizeTemplatePackTemplate)
      : [];
    if (!templates.length) {
      throw createTemplatePackContractError("template pack templates are required");
    }
    normalized = {
      schema_version: TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION,
      id: String(source.id || "").trim(),
      name: String(source.name || source.id || "").trim(),
      version: String(source.version || "v1").trim() || "v1",
      source: String(options.source || source.source || "marketplace_export"),
      templates,
      created_at: String(source.created_at || "").trim(),
    };
    if (!normalized.id) {
      throw createTemplatePackContractError("template pack id is required");
    }
    if (!normalized.name) {
      throw createTemplatePackContractError("template pack name is required");
    }
  } else {
    throw createTemplatePackContractError(`unsupported template pack schema_version: ${rawSchemaVersion}`);
  }
  return {
    schema_version: TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION,
    id: normalized.id,
    name: normalized.name,
    version: normalized.version,
    source: String(options.source || normalized.source || "marketplace_export"),
    templates: normalized.templates.map((template) => clone(template)),
    created_at: String(options.created_at || normalized.created_at || ""),
  };
}

module.exports = {
  TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION,
  TEMPLATE_WORKFLOW_DEFINITION_FIELD,
  createTemplatePackContractError,
  exportTemplatePackArtifact,
  normalizeTemplatePackArtifact,
  normalizeTemplatePackTemplate,
  resolveTemplateWorkflowDefinition,
};
