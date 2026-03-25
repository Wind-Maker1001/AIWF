param(
  [string]$RepoRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  throw "node not found in PATH"
}

$nodeScript = @'
const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawnSync } = require("child_process");

function fail(payload) {
  console.log(JSON.stringify(payload));
  process.exit(1);
}

(async () => {
  const repoRoot = process.argv[2];
  const {
    CLEANING_TEMPLATE_REGISTRY_SCHEMA_VERSION,
    OFFICE_LAYOUT_CATALOG_SCHEMA_VERSION,
    OFFICE_THEME_CATALOG_SCHEMA_VERSION,
    normalizeCleaningTemplateRegistry,
    normalizeOfficeLayoutCatalog,
    normalizeOfficeThemeCatalog,
  } = require(path.join(repoRoot, "apps", "dify-desktop", "offline_template_catalog_contract.js"));
  const { createOfflineEngineConfig } = require(path.join(repoRoot, "apps", "dify-desktop", "offline_engine_config.js"));

  const themeSchemaPath = path.join(repoRoot, "contracts", "desktop", "office_theme_catalog.schema.json");
  const layoutSchemaPath = path.join(repoRoot, "contracts", "desktop", "office_layout_catalog.schema.json");
  const registrySchemaPath = path.join(repoRoot, "contracts", "desktop", "cleaning_template_registry.schema.json");
  const packManifestSchemaPath = path.join(repoRoot, "contracts", "desktop", "offline_template_catalog_pack_manifest.schema.json");
  const themeFilePath = path.join(repoRoot, "rules", "templates", "office_themes_desktop.json");
  const layoutFilePath = path.join(repoRoot, "rules", "templates", "office_layouts_desktop.json");
  const registryFilePath = path.join(repoRoot, "rules", "templates", "cleaning_templates_desktop.json");

  const themeSchema = JSON.parse(fs.readFileSync(themeSchemaPath, "utf8"));
  const layoutSchema = JSON.parse(fs.readFileSync(layoutSchemaPath, "utf8"));
  const registrySchema = JSON.parse(fs.readFileSync(registrySchemaPath, "utf8"));
  const packManifestSchema = JSON.parse(fs.readFileSync(packManifestSchemaPath, "utf8"));
  const themeSchemaConst = String(themeSchema?.properties?.schema_version?.const || "");
  const layoutSchemaConst = String(layoutSchema?.properties?.schema_version?.const || "");
  const registrySchemaConst = String(registrySchema?.properties?.schema_version?.const || "");
  const packManifestSchemaConst = String(packManifestSchema?.properties?.schema_version?.const || "");
  if (themeSchemaConst !== OFFICE_THEME_CATALOG_SCHEMA_VERSION) {
    fail({ status: "failed", issues: [`office theme schema const drift: ${themeSchemaConst || "(missing)"}`] });
  }
  if (layoutSchemaConst !== OFFICE_LAYOUT_CATALOG_SCHEMA_VERSION) {
    fail({ status: "failed", issues: [`office layout schema const drift: ${layoutSchemaConst || "(missing)"}`] });
  }
  if (registrySchemaConst !== CLEANING_TEMPLATE_REGISTRY_SCHEMA_VERSION) {
    fail({ status: "failed", issues: [`cleaning template registry schema const drift: ${registrySchemaConst || "(missing)"}`] });
  }
  if (packManifestSchemaConst !== "offline_template_catalog_pack_manifest.v1") {
    fail({ status: "failed", issues: [`offline template catalog pack manifest schema const drift: ${packManifestSchemaConst || "(missing)"}`] });
  }

  const themeCatalog = normalizeOfficeThemeCatalog(JSON.parse(fs.readFileSync(themeFilePath, "utf8")), { allowLegacyMap: false });
  const layoutCatalog = normalizeOfficeLayoutCatalog(JSON.parse(fs.readFileSync(layoutFilePath, "utf8")), { allowLegacyMap: false });
  const registryCatalog = normalizeCleaningTemplateRegistry(JSON.parse(fs.readFileSync(registryFilePath, "utf8")), { allowLegacyRegistry: false });

  const legacyTheme = normalizeOfficeThemeCatalog({
    custom: { title: "Legacy Theme", primary: "123456", secondary: "654321", bg: "FFFFFF" },
  }, { allowLegacyMap: true });
  const legacyLayout = normalizeOfficeLayoutCatalog({
    custom: { docx_max_table_rows: 13, pptx_sample_rows: 4 },
  }, { allowLegacyMap: true });
  const legacyRegistry = normalizeCleaningTemplateRegistry({
    templates: [{ id: "finance_report_v1", file: "generic_finance_strict.json" }],
  }, { allowLegacyRegistry: true });

  const prevTheme = process.env.AIWF_OFFICE_THEME_FILE_DESKTOP;
  const prevLayout = process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP;
  const prevForceFluent = process.env.AIWF_FORCE_FLUENT_STYLE;
  process.env.AIWF_OFFICE_THEME_FILE_DESKTOP = themeFilePath;
  process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP = layoutFilePath;
  process.env.AIWF_FORCE_FLUENT_STYLE = "0";
  let theme = null;
  let layout = null;
  let templates = null;
  try {
    const config = createOfflineEngineConfig();
    theme = config.resolveOfficeTheme("fluent_ms_vibrant");
    layout = config.resolveOfficeLayout("fluent_ms_vibrant");
    templates = config.listCleaningTemplates();
  } finally {
    if (prevTheme === undefined) delete process.env.AIWF_OFFICE_THEME_FILE_DESKTOP;
    else process.env.AIWF_OFFICE_THEME_FILE_DESKTOP = prevTheme;
    if (prevLayout === undefined) delete process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP;
    else process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP = prevLayout;
    if (prevForceFluent === undefined) delete process.env.AIWF_FORCE_FLUENT_STYLE;
    else process.env.AIWF_FORCE_FLUENT_STYLE = prevForceFluent;
  }

  const hasFinanceTemplate = Array.isArray(templates?.templates)
    && templates.templates.some((item) => String(item?.id || "") === "finance_report_v1");
  if (!hasFinanceTemplate) {
    fail({ status: "failed", issues: ["offline engine cleaning template registry no longer exposes finance_report_v1"] });
  }

  const templatePackManagerPath = path.join(repoRoot, "ops", "scripts", "template_pack_manager.ps1");
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-offline-template-pack-"));
  const packRoot = path.join(tempRoot, "packs");
  const targetDir = path.join(tempRoot, "target");
  fs.mkdirSync(targetDir, { recursive: true });
  const version = "gate_pack";
  const exportResult = spawnSync("powershell", [
    "-ExecutionPolicy", "Bypass",
    "-File", templatePackManagerPath,
    "-Action", "export",
    "-Version", version,
    "-SourceDir", path.join(repoRoot, "rules", "templates"),
    "-PackRoot", packRoot,
  ], {
    cwd: repoRoot,
    encoding: "utf8",
  });
  if (exportResult.status !== 0) {
    fail({ status: "failed", issues: [`template_pack_manager export failed: ${exportResult.stdout}\n${exportResult.stderr}`] });
  }
  const packDir = path.join(packRoot, `pack_${version}`);
  const packManifestPath = path.join(packDir, "manifest.json");
  const packManifest = JSON.parse(fs.readFileSync(packManifestPath, "utf8").replace(/^\uFEFF/, ""));
  const packManagerExported = String(packManifest.schema_version || "") === "offline_template_catalog_pack_manifest.v1"
    && Array.isArray(packManifest.contract_schemas)
    && packManifest.contract_schemas.includes("contracts/desktop/offline_template_catalog_pack_manifest.schema.json");
  if (!packManagerExported) {
    fail({ status: "failed", issues: ["template_pack_manager no longer exports versioned pack manifest"] });
  }
  const importResult = spawnSync("powershell", [
    "-ExecutionPolicy", "Bypass",
    "-File", templatePackManagerPath,
    "-Action", "import",
    "-Version", version,
    "-TargetDir", targetDir,
    "-PackRoot", packRoot,
  ], {
    cwd: repoRoot,
    encoding: "utf8",
  });
  if (importResult.status !== 0) {
    fail({ status: "failed", issues: [`template_pack_manager import failed: ${importResult.stdout}\n${importResult.stderr}`] });
  }
  const importedRegistry = JSON.parse(fs.readFileSync(path.join(targetDir, "cleaning_templates_desktop.json"), "utf8").replace(/^\uFEFF/, ""));
  const packManagerImported = String(importedRegistry.schema_version || "") === CLEANING_TEMPLATE_REGISTRY_SCHEMA_VERSION;
  if (!packManagerImported) {
    fail({ status: "failed", issues: ["template_pack_manager import no longer preserves catalog schema_version"] });
  }
  const importedGenericTemplatePath = path.join(targetDir, "generic_finance_strict.json");
  const packManagerIncludesGenericTemplate = fs.existsSync(path.join(packDir, "generic_finance_strict.json"))
    && fs.existsSync(importedGenericTemplatePath);
  if (!packManagerIncludesGenericTemplate) {
    fail({ status: "failed", issues: ["template_pack_manager no longer exports or imports generic template files"] });
  }

  console.log(JSON.stringify({
    status: "passed",
    schemaPaths: {
      theme: themeSchemaPath,
      layout: layoutSchemaPath,
      registry: registrySchemaPath,
      pack_manifest: packManifestSchemaPath,
    },
    filePaths: {
      theme: themeFilePath,
      layout: layoutFilePath,
      registry: registryFilePath,
    },
    schemaVersions: {
      theme: OFFICE_THEME_CATALOG_SCHEMA_VERSION,
      layout: OFFICE_LAYOUT_CATALOG_SCHEMA_VERSION,
      registry: CLEANING_TEMPLATE_REGISTRY_SCHEMA_VERSION,
    },
    migratedLegacy: {
      theme: legacyTheme.migrated,
      layout: legacyLayout.migrated,
      registry: legacyRegistry.migrated,
    },
    runtime: {
      themeTitle: String(theme?.title || ""),
      layoutRows: Number(layout?.docx_max_table_rows || 0),
      cleaningTemplateCount: Array.isArray(templates?.templates) ? templates.templates.length : 0,
    },
    packManager: {
      exported: packManagerExported,
      imported: packManagerImported,
      generic_template_roundtrip: packManagerIncludesGenericTemplate,
      packManifestSchemaVersion: String(packManifest.schema_version || ""),
    },
  }));
})().catch((error) => {
  fail({
    status: "failed",
    issues: [error && error.stack ? error.stack : String(error)],
  });
});
'@

$nodeScript | node - $RepoRoot
if ($LASTEXITCODE -ne 0) {
  throw "offline template catalog sync checks failed"
}

Ok "offline template catalog sync check passed"
