const test = require("node:test");
const assert = require("node:assert/strict");

const {
  CLEANING_TEMPLATE_REGISTRY_SCHEMA_VERSION,
  OFFICE_LAYOUT_CATALOG_SCHEMA_VERSION,
  OFFICE_THEME_CATALOG_SCHEMA_VERSION,
  normalizeCleaningTemplateRegistry,
  normalizeOfficeLayoutCatalog,
  normalizeOfficeThemeCatalog,
} = require("../offline_template_catalog_contract");

test("offline template catalog contract migrates legacy office catalogs", () => {
  const themeCatalog = normalizeOfficeThemeCatalog({
    fluent_ms: { title: "Fluent", primary: "0F6CBD" },
  }, { allowLegacyMap: true });
  assert.equal(themeCatalog.schema_version, OFFICE_THEME_CATALOG_SCHEMA_VERSION);
  assert.equal(themeCatalog.migrated, true);
  assert.equal(themeCatalog.themes.fluent_ms.title, "Fluent");

  const layoutCatalog = normalizeOfficeLayoutCatalog({
    default: { xlsx_data_sheet_name: "cleaned" },
  }, { allowLegacyMap: true });
  assert.equal(layoutCatalog.schema_version, OFFICE_LAYOUT_CATALOG_SCHEMA_VERSION);
  assert.equal(layoutCatalog.migrated, true);
  assert.equal(layoutCatalog.layouts.default.xlsx_data_sheet_name, "cleaned");
});

test("offline template catalog contract migrates legacy cleaning registry and rejects unsupported schema", () => {
  const registry = normalizeCleaningTemplateRegistry({
    templates: [{ id: "finance_report_v1", file: "generic_finance_strict.json" }],
  }, { allowLegacyRegistry: true });
  assert.equal(registry.schema_version, CLEANING_TEMPLATE_REGISTRY_SCHEMA_VERSION);
  assert.equal(registry.migrated, true);
  assert.equal(registry.templates.length, 1);

  assert.throws(
    () => normalizeCleaningTemplateRegistry({
      schema_version: "cleaning_template_registry.v999",
      templates: [],
    }),
    /unsupported cleaning template registry schema_version/i
  );
});
