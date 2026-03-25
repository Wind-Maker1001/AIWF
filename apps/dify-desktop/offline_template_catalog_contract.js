const OFFICE_THEME_CATALOG_SCHEMA_VERSION = "office_theme_catalog.v1";
const OFFICE_LAYOUT_CATALOG_SCHEMA_VERSION = "office_layout_catalog.v1";
const CLEANING_TEMPLATE_REGISTRY_SCHEMA_VERSION = "cleaning_template_registry.v1";

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function createOfflineTemplateCatalogError(message, code = "offline_template_catalog_invalid", notes = []) {
  const error = new Error(String(message || "offline template catalog invalid"));
  error.code = code;
  error.notes = Array.isArray(notes) ? notes.slice() : [];
  return error;
}

function normalizeOfficeThemeCatalog(raw, options = {}) {
  const { allowLegacyMap = true } = options;
  const notes = [];
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw createOfflineTemplateCatalogError("office theme catalog must be an object");
  }
  let themes = {};
  const rawSchemaVersion = String(raw.schema_version || "").trim();
  if (!rawSchemaVersion) {
    if (!allowLegacyMap) {
      throw createOfflineTemplateCatalogError("office theme catalog schema_version is required");
    }
    themes = clone(raw);
    notes.push(`office theme catalog schema_version migrated to ${OFFICE_THEME_CATALOG_SCHEMA_VERSION}`);
  } else if (rawSchemaVersion === OFFICE_THEME_CATALOG_SCHEMA_VERSION) {
    themes = raw.themes && typeof raw.themes === "object" && !Array.isArray(raw.themes) ? clone(raw.themes) : {};
  } else {
    throw createOfflineTemplateCatalogError(`unsupported office theme catalog schema_version: ${rawSchemaVersion}`);
  }
  return {
    schema_version: OFFICE_THEME_CATALOG_SCHEMA_VERSION,
    themes,
    notes,
    migrated: notes.length > 0,
  };
}

function normalizeOfficeLayoutCatalog(raw, options = {}) {
  const { allowLegacyMap = true } = options;
  const notes = [];
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw createOfflineTemplateCatalogError("office layout catalog must be an object");
  }
  let layouts = {};
  const rawSchemaVersion = String(raw.schema_version || "").trim();
  if (!rawSchemaVersion) {
    if (!allowLegacyMap) {
      throw createOfflineTemplateCatalogError("office layout catalog schema_version is required");
    }
    layouts = clone(raw);
    notes.push(`office layout catalog schema_version migrated to ${OFFICE_LAYOUT_CATALOG_SCHEMA_VERSION}`);
  } else if (rawSchemaVersion === OFFICE_LAYOUT_CATALOG_SCHEMA_VERSION) {
    layouts = raw.layouts && typeof raw.layouts === "object" && !Array.isArray(raw.layouts) ? clone(raw.layouts) : {};
  } else {
    throw createOfflineTemplateCatalogError(`unsupported office layout catalog schema_version: ${rawSchemaVersion}`);
  }
  return {
    schema_version: OFFICE_LAYOUT_CATALOG_SCHEMA_VERSION,
    layouts,
    notes,
    migrated: notes.length > 0,
  };
}

function normalizeCleaningTemplateRegistry(raw, options = {}) {
  const { allowLegacyRegistry = true } = options;
  const notes = [];
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw createOfflineTemplateCatalogError("cleaning template registry must be an object");
  }
  let templates = [];
  const rawSchemaVersion = String(raw.schema_version || "").trim();
  if (!rawSchemaVersion) {
    if (!allowLegacyRegistry) {
      throw createOfflineTemplateCatalogError("cleaning template registry schema_version is required");
    }
    templates = Array.isArray(raw.templates) ? clone(raw.templates) : [];
    notes.push(`cleaning template registry schema_version migrated to ${CLEANING_TEMPLATE_REGISTRY_SCHEMA_VERSION}`);
  } else if (rawSchemaVersion === CLEANING_TEMPLATE_REGISTRY_SCHEMA_VERSION) {
    templates = Array.isArray(raw.templates) ? clone(raw.templates) : [];
  } else {
    throw createOfflineTemplateCatalogError(`unsupported cleaning template registry schema_version: ${rawSchemaVersion}`);
  }
  return {
    schema_version: CLEANING_TEMPLATE_REGISTRY_SCHEMA_VERSION,
    templates,
    notes,
    migrated: notes.length > 0,
  };
}

module.exports = {
  CLEANING_TEMPLATE_REGISTRY_SCHEMA_VERSION,
  OFFICE_LAYOUT_CATALOG_SCHEMA_VERSION,
  OFFICE_THEME_CATALOG_SCHEMA_VERSION,
  createOfflineTemplateCatalogError,
  normalizeCleaningTemplateRegistry,
  normalizeOfficeLayoutCatalog,
  normalizeOfficeThemeCatalog,
};
