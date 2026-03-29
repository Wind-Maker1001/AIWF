const LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION = "local_template_storage.v1";
const LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION = "local_template_entry.v1";
const TEMPLATE_WORKFLOW_DEFINITION_FIELD = "workflow_definition";

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function createLocalTemplateStorageError(message, code = "local_template_storage_invalid", notes = []) {
  const error = new Error(String(message || "local template storage invalid"));
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

function normalizeLocalTemplateEntry(entry, index = 0, options = {}) {
  const {
    allowSchemaMigration = true,
  } = options;
  const source = entry && typeof entry === "object" ? entry : null;
  if (!source || Array.isArray(source)) {
    throw createLocalTemplateStorageError(`local template entry[${index}] must be an object`);
  }

  const notes = [];
  const rawSchemaVersion = String(source.schema_version || "").trim();
  if (!rawSchemaVersion) {
    if (!allowSchemaMigration) {
      throw createLocalTemplateStorageError(`local template entry[${index}] schema_version is required`);
    }
    notes.push(`local template entry[${index}] schema_version migrated to ${LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION}`);
  } else if (rawSchemaVersion !== LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION) {
    throw createLocalTemplateStorageError(`unsupported local template entry schema_version: ${rawSchemaVersion}`);
  }

  const id = String(source.id || `custom_${index + 1}`).trim();
  const name = String(source.name || id).trim();
  const workflowDefinition = resolveTemplateWorkflowDefinition(source);
  if (!id) throw createLocalTemplateStorageError(`local template entry[${index}] id is required`, "local_template_entry_invalid", notes);
  if (!name) throw createLocalTemplateStorageError(`local template entry[${index}] name is required`, "local_template_entry_invalid", notes);
  if (!workflowDefinition) throw createLocalTemplateStorageError(`local template entry[${index}] workflow_definition is required`, "local_template_entry_invalid", notes);

  return {
    schema_version: LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
    id,
    name,
    workflow_definition: workflowDefinition,
    template_spec_version: Number.isFinite(Number(source.template_spec_version))
      ? Math.max(1, Math.floor(Number(source.template_spec_version)))
      : 1,
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
    notes,
    migrated: notes.length > 0,
  };
}

function normalizeLocalTemplateStorage(raw, options = {}) {
  const {
    allowStorageSchemaMigration = true,
    allowEntrySchemaMigration = true,
  } = options;

  const notes = [];
  let itemsSource = [];
  if (Array.isArray(raw)) {
    if (!allowStorageSchemaMigration) {
      throw createLocalTemplateStorageError("local template storage schema_version is required");
    }
    itemsSource = raw;
    notes.push(`local template storage schema_version migrated to ${LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION}`);
  } else {
    const source = raw && typeof raw === "object" ? raw : {};
    const rawSchemaVersion = String(source.schema_version || "").trim();
    if (!rawSchemaVersion) {
      if (!allowStorageSchemaMigration) {
        throw createLocalTemplateStorageError("local template storage schema_version is required");
      }
      notes.push(`local template storage schema_version migrated to ${LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION}`);
    } else if (rawSchemaVersion !== LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION) {
      throw createLocalTemplateStorageError(`unsupported local template storage schema_version: ${rawSchemaVersion}`);
    }
    itemsSource = Array.isArray(source.items) ? source.items : [];
  }

  const normalizedItems = itemsSource.map((item, index) => normalizeLocalTemplateEntry(item, index, {
    allowSchemaMigration: allowEntrySchemaMigration,
  }));
  normalizedItems.forEach((item) => {
    const itemNotes = Array.isArray(item.notes) ? item.notes : [];
    itemNotes.forEach((note) => notes.push(note));
  });

  return {
    schema_version: LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
    items: normalizedItems.map((item) => {
      const { notes: _notes, migrated: _migrated, ...rest } = item;
      return rest;
    }),
    notes,
    migrated: notes.length > 0,
  };
}

function parseLocalTemplateStorageText(text, options = {}) {
  const raw = String(text || "").trim();
  if (!raw) {
    return normalizeLocalTemplateStorage({ schema_version: LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION, items: [] }, {
      allowStorageSchemaMigration: false,
      allowEntrySchemaMigration: false,
      ...options,
    });
  }
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw createLocalTemplateStorageError(`local template storage parse failed: ${String(error?.message || error)}`);
  }
  return normalizeLocalTemplateStorage(parsed, options);
}

function stringifyLocalTemplateStorage(raw, options = {}) {
  return JSON.stringify(normalizeLocalTemplateStorage(raw, options));
}

export {
  LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
  LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
  TEMPLATE_WORKFLOW_DEFINITION_FIELD,
  createLocalTemplateStorageError,
  normalizeLocalTemplateEntry,
  normalizeLocalTemplateStorage,
  parseLocalTemplateStorageText,
  resolveTemplateWorkflowDefinition,
  stringifyLocalTemplateStorage,
};
