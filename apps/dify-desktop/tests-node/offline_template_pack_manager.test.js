const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8").replace(/^\uFEFF/, ""));
}

test("template pack manager exports versioned manifest and re-imports catalogs", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/template_pack_manager.ps1");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-template-pack-manager-"));
  const sourceDir = path.join(root, "source");
  const targetDir = path.join(root, "target");
  const packRoot = path.join(root, "packs");
  const version = "20260324_test";

  writeJson(path.join(sourceDir, "office_themes_desktop.json"), {
    schema_version: "office_theme_catalog.v1",
    themes: {
      fluent_ms: { title: "Fluent", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    },
  });
  writeJson(path.join(sourceDir, "office_layouts_desktop.json"), {
    schema_version: "office_layout_catalog.v1",
    layouts: {
      default: { docx_max_table_rows: 22 },
    },
  });
  writeJson(path.join(sourceDir, "cleaning_templates_desktop.json"), {
    schema_version: "cleaning_template_registry.v1",
    templates: [{ id: "finance_report_v1", file: "generic_finance_strict.json" }],
  });
  writeJson(path.join(sourceDir, "generic_finance_strict.json"), {
    rules: {
      platform_mode: "generic",
      required_fields: ["id", "amount"],
    },
  });
  fs.mkdirSync(targetDir, { recursive: true });

  const exportResult = spawnSync("powershell", [
    "-ExecutionPolicy", "Bypass",
    "-File", script,
    "-Action", "export",
    "-SourceDir", sourceDir,
    "-PackRoot", packRoot,
    "-Version", version,
  ], {
    cwd: repoRoot,
    encoding: "utf8",
  });
  assert.equal(exportResult.status, 0, `${exportResult.stdout}\n${exportResult.stderr}`);

  const packDir = path.join(packRoot, `pack_${version}`);
  const manifestPath = path.join(packDir, "manifest.json");
  const manifest = readJson(manifestPath);
  assert.equal(manifest.schema_version, "offline_template_catalog_pack_manifest.v1");
  assert.ok(Array.isArray(manifest.contract_schemas));
  assert.ok(manifest.contract_schemas.includes("contracts/desktop/offline_template_catalog_pack_manifest.schema.json"));
  assert.ok(Array.isArray(manifest.generic_templates));
  assert.ok(manifest.generic_templates.includes("generic_finance_strict.json"));
  assert.ok(manifest.files.includes("generic_finance_strict.json"));
  assert.equal(manifest.catalogs["office_themes_desktop.json"].schema_version, "office_theme_catalog.v1");
  assert.ok(fs.existsSync(path.join(packDir, "contracts", "desktop", "offline_template_catalog_pack_manifest.schema.json")));
  assert.ok(fs.existsSync(path.join(packDir, "generic_finance_strict.json")));

  const importResult = spawnSync("powershell", [
    "-ExecutionPolicy", "Bypass",
    "-File", script,
    "-Action", "import",
    "-TargetDir", targetDir,
    "-PackRoot", packRoot,
    "-Version", version,
  ], {
    cwd: repoRoot,
    encoding: "utf8",
  });
  assert.equal(importResult.status, 0, `${importResult.stdout}\n${importResult.stderr}`);

  const importedThemes = readJson(path.join(targetDir, "office_themes_desktop.json"));
  const importedLayouts = readJson(path.join(targetDir, "office_layouts_desktop.json"));
  const importedRegistry = readJson(path.join(targetDir, "cleaning_templates_desktop.json"));
  const importedGenericTemplate = readJson(path.join(targetDir, "generic_finance_strict.json"));
  assert.equal(importedThemes.schema_version, "office_theme_catalog.v1");
  assert.equal(importedLayouts.schema_version, "office_layout_catalog.v1");
  assert.equal(importedRegistry.schema_version, "cleaning_template_registry.v1");
  assert.deepEqual(importedGenericTemplate.rules.required_fields, ["id", "amount"]);
});
