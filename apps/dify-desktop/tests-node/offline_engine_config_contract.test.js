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
  const prevForceFluent = process.env.AIWF_FORCE_FLUENT_STYLE;
  Object.entries(files).forEach(([name, value]) => {
    fs.writeFileSync(path.join(root, name), `${JSON.stringify(value, null, 2)}\n`, "utf8");
  });
  process.env.AIWF_OFFICE_THEME_FILE_DESKTOP = path.join(root, "office_themes_desktop.json");
  process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP = path.join(root, "office_layouts_desktop.json");
  process.env.AIWF_FORCE_FLUENT_STYLE = "0";
  try {
    return fn(root);
  } finally {
    if (prevTheme === undefined) delete process.env.AIWF_OFFICE_THEME_FILE_DESKTOP;
    else process.env.AIWF_OFFICE_THEME_FILE_DESKTOP = prevTheme;
    if (prevLayout === undefined) delete process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP;
    else process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP = prevLayout;
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
