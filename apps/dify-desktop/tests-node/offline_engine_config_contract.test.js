const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

function loadConfigModule() {
  const modulePath = path.resolve(__dirname, "../offline_engine_config.js");
  delete require.cache[modulePath];
  return require(modulePath);
}

function withTemplateFiles(files, fn) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-offline-template-catalog-"));
  const prevTheme = process.env.AIWF_OFFICE_THEME_FILE_DESKTOP;
  const prevLayout = process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP;
  const prevTemplateDir = process.env.AIWF_CLEANING_TEMPLATE_DIR;
  const prevForceFluent = process.env.AIWF_FORCE_FLUENT_STYLE;
  Object.entries(files).forEach(([name, value]) => {
    fs.writeFileSync(path.join(root, name), `${JSON.stringify(value, null, 2)}\n`, "utf8");
  });
  process.env.AIWF_OFFICE_THEME_FILE_DESKTOP = path.join(root, "office_themes_desktop.json");
  process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP = path.join(root, "office_layouts_desktop.json");
  process.env.AIWF_CLEANING_TEMPLATE_DIR = root;
  process.env.AIWF_FORCE_FLUENT_STYLE = "0";
  try {
    return fn(root);
  } finally {
    if (prevTheme === undefined) delete process.env.AIWF_OFFICE_THEME_FILE_DESKTOP;
    else process.env.AIWF_OFFICE_THEME_FILE_DESKTOP = prevTheme;
    if (prevLayout === undefined) delete process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP;
    else process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP = prevLayout;
    if (prevTemplateDir === undefined) delete process.env.AIWF_CLEANING_TEMPLATE_DIR;
    else process.env.AIWF_CLEANING_TEMPLATE_DIR = prevTemplateDir;
    if (prevForceFluent === undefined) delete process.env.AIWF_FORCE_FLUENT_STYLE;
    else process.env.AIWF_FORCE_FLUENT_STYLE = prevForceFluent;
  }
}

test("offline engine config reads versioned office theme and layout catalogs", () => {
  withTemplateFiles({
    "office_themes_desktop.json": {
      schema_version: "office_theme_catalog.v1",
      themes: {
        custom: { title: "Custom Theme", primary: "123456", secondary: "654321", bg: "FFFFFF" },
      },
    },
    "office_layouts_desktop.json": {
      schema_version: "office_layout_catalog.v1",
      layouts: {
        custom: { docx_max_table_rows: 11, pptx_sample_rows: 3 },
      },
    },
  }, () => {
    const { createOfflineEngineConfig } = loadConfigModule();
    const config = createOfflineEngineConfig();
    const theme = config.resolveOfficeTheme("custom");
    const layout = config.resolveOfficeLayout("custom");

    assert.equal(theme.title, "Custom Theme");
    assert.equal(layout.docx_max_table_rows, 11);
  });
});

test("offline engine config still accepts legacy office theme and layout catalogs", () => {
  withTemplateFiles({
    "office_themes_desktop.json": {
      custom: { title: "Legacy Theme", primary: "123456", secondary: "654321", bg: "FFFFFF" },
    },
    "office_layouts_desktop.json": {
      custom: { docx_max_table_rows: 13, pptx_sample_rows: 4 },
    },
  }, () => {
    const { createOfflineEngineConfig } = loadConfigModule();
    const config = createOfflineEngineConfig();
    const theme = config.resolveOfficeTheme("custom");
    const layout = config.resolveOfficeLayout("custom");

    assert.equal(theme.title, "Legacy Theme");
    assert.equal(layout.docx_max_table_rows, 13);
  });
});

test("offline engine config loads cleaning_spec.v2 template entries from registry", () => {
  withTemplateFiles({
    "office_themes_desktop.json": { schema_version: "office_theme_catalog.v1", themes: {} },
    "office_layouts_desktop.json": { schema_version: "office_layout_catalog.v1", layouts: {} },
    "cleaning_templates_desktop.json": {
      schema_version: "cleaning_template_registry.v1",
      templates: [{
        id: "finance_report_v1",
        file: "finance_report_v1.cleaning_spec_v2.json",
        label: "Finance Spec",
        template_expected_profile: "finance_statement",
        blank_output_expected: false,
      }],
    },
    "finance_report_v1.cleaning_spec_v2.json": {
      schema_version: "cleaning_spec.v2",
      schema: { canonical_profile: "finance_statement" },
      transform: { rename_map: { Amt: "amount" }, casts: { amount: "float" } },
      quality: { required_fields: ["amount"], gates: { min_output_rows: 1 } },
    },
  }, () => {
    const { createOfflineEngineConfig } = loadConfigModule();
    const config = createOfflineEngineConfig();
    const templates = config.listCleaningTemplates().templates;
    const finance = templates.find((item) => item.id === "finance_report_v1");
    assert.equal(finance.template_format, "cleaning_spec_v2");
    assert.equal(finance.template_expected_profile, "finance_statement");
    assert.equal(finance.blank_output_expected, false);
    assert.equal(finance.cleaning_spec_v2.schema_version, "cleaning_spec.v2");
    const resolved = config.resolveCleaningTemplateParams({ cleaning_template: "finance_report_v1" });
    assert.equal(resolved.cleaning_spec_v2.schema.canonical_profile, "finance_statement");
    assert.equal(resolved.quality_rules.min_output_rows, 1);
    assert.equal(resolved.template_expected_profile, "finance_statement");
    assert.equal(resolved.blank_output_expected, false);
  });
});

test("offline engine config compiles legacy rules templates into cleaning_spec.v2", () => {
  withTemplateFiles({
    "office_themes_desktop.json": { schema_version: "office_theme_catalog.v1", themes: {} },
    "office_layouts_desktop.json": { schema_version: "office_layout_catalog.v1", layouts: {} },
    "cleaning_templates_desktop.json": {
      schema_version: "cleaning_template_registry.v1",
      templates: [{ id: "legacy_tpl", file: "legacy_tpl.json", label: "Legacy Rules" }],
    },
    "legacy_tpl.json": {
      rules: {
        platform_mode: "generic",
        rename_map: { Amt: "amount" },
        casts: { amount: "float" },
        required_fields: ["amount"],
        min_output_rows: 1,
      },
      params_schema: { region: { type: "string" } },
    },
  }, () => {
    const { createOfflineEngineConfig } = loadConfigModule();
    const config = createOfflineEngineConfig();
    const templates = config.listCleaningTemplates().templates;
    const legacy = templates.find((item) => item.id === "legacy_tpl");
    assert.equal(legacy.template_format, "legacy_rules");
    assert.equal(legacy.cleaning_spec_v2.schema_version, "cleaning_spec.v2");
    assert.equal(legacy.params_schema.region.type, "string");
    const resolved = config.resolveCleaningTemplateParams({ cleaning_template: "legacy_tpl" });
    assert.equal(resolved.cleaning_spec_v2.transform.rename_map.Amt, "amount");
    assert.equal(resolved.rules.rename_map.Amt, "amount");
  });
});

test("offline engine config expands direct cleaning_spec.v2 params without registry lookup", () => {
  withTemplateFiles({
    "office_themes_desktop.json": { schema_version: "office_theme_catalog.v1", themes: {} },
    "office_layouts_desktop.json": { schema_version: "office_layout_catalog.v1", layouts: {} },
  }, () => {
    const { createOfflineEngineConfig } = loadConfigModule();
    const config = createOfflineEngineConfig();
    const resolved = config.resolveCleaningTemplateParams({
      cleaning_template: "user_import",
      cleaning_spec_v2: {
        schema_version: "cleaning_spec.v2",
        schema: { canonical_profile: "finance_statement" },
        quality: { required_fields: ["amount"], gates: { min_output_rows: 2 } },
      },
    });
    assert.equal(resolved.cleaning_spec_v2.schema.canonical_profile, "finance_statement");
    assert.equal(resolved.canonical_profile, "finance_statement");
    assert.equal(resolved.quality_rules.min_output_rows, 2);
    assert.deepEqual(resolved.quality_rules.required_fields, ["amount"]);
  });
});
