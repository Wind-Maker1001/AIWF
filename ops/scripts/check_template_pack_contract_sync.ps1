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

function fail(payload) {
  console.log(JSON.stringify(payload));
  process.exit(1);
}

(async () => {
  const repoRoot = process.argv[2];
  const schemaPath = path.join(repoRoot, "contracts", "desktop", "template_pack_artifact.schema.json");
  const {
    TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION,
    exportTemplatePackArtifact,
    normalizeTemplatePackArtifact,
  } = require(path.join(repoRoot, "apps", "dify-desktop", "workflow_template_pack_contract.js"));
  const { TEMPLATE_PACK_ENTRY_SCHEMA_VERSION } = require(path.join(repoRoot, "apps", "dify-desktop", "workflow_ipc_state.js"));
  const { registerWorkflowStoreIpc } = require(path.join(repoRoot, "apps", "dify-desktop", "workflow_ipc_store.js"));

  const schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
  const schemaVersionConst = String(schema?.properties?.schema_version?.const || "");
  if (schemaVersionConst !== TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION) {
    fail({
      status: "failed",
      schemaPath,
      issues: [`template pack schema_version const drift: ${schemaVersionConst || "(missing)"}`],
    });
  }

  function templateGraph() {
    return {
      workflow_id: "wf_template_pack_gate",
      version: "1.0.0",
      name: "Template Pack Gate",
      nodes: [{ id: "n1", type: "ingest_files", config: {} }],
      edges: [],
    };
  }

  const legacyPack = {
    id: "pack_1",
    name: "Finance Pack",
    version: "v1",
    templates: [{
      id: "tpl_1",
      name: "Finance Template",
      graph: templateGraph(),
    }],
  };

  const normalized = normalizeTemplatePackArtifact(legacyPack, {
    allowVersionMigration: true,
    source: "legacy_inline",
  });
  if (!normalized.migrated) {
    fail({
      status: "failed",
      issues: ["legacy template pack did not record schema_version migration"],
    });
  }
  if (normalized.schema_version !== TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION) {
    fail({
      status: "failed",
      issues: ["legacy template pack did not normalize to artifact schema version"],
    });
  }
  if (!normalized.templates?.[0]?.workflow_definition || Object.prototype.hasOwnProperty.call(normalized.templates[0], "graph")) {
    fail({
      status: "failed",
      issues: ["normalized template pack template did not converge to workflow_definition field"],
    });
  }

  const exportFromEntry = exportTemplatePackArtifact({
    schema_version: TEMPLATE_PACK_ENTRY_SCHEMA_VERSION,
    id: "pack_entry",
    name: "Entry Pack",
    version: "v2",
    source: "inline",
    templates: [{
      id: "tpl_entry",
      name: "Entry Template",
      graph: templateGraph(),
      template_spec_version: 1,
    }],
    created_at: "2026-03-24T00:00:00Z",
  }, {
    source: "marketplace_export",
  });
  if (exportFromEntry.schema_version !== TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION) {
    fail({
      status: "failed",
      issues: ["marketplace entry did not export as template_pack_artifact.v1"],
    });
  }

  const handlers = {};
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-template-pack-gate-"));
  const marketplace = [];
  registerWorkflowStoreIpc({
    ipcMain: {
      handle(name, fn) {
        handlers[name] = fn;
      },
    },
    dialog: {
      async showSaveDialog() {
        return { canceled: true, filePath: "" };
      },
    },
    app: {
      getPath() {
        return tempRoot;
      },
    },
    fs,
    path,
  }, {
    appendAudit: () => {},
    isMockIoAllowed: () => true,
    listTemplateMarketplace: () => marketplace.slice(),
    nowIso: () => "2026-03-24T00:00:00.000Z",
    qualityRuleSetSupport: {
      listQualityRuleSets: async () => ({ ok: true, sets: [] }),
      saveQualityRuleSet: async () => ({ ok: true }),
      removeQualityRuleSet: async () => ({ ok: true }),
    },
    resolveMockFilePath: (target) => ({ ok: true, path: target }),
    saveTemplateMarketplace: (items) => {
      marketplace.splice(0, marketplace.length, ...items);
    },
    workflowVersionStore: {
      recordVersion: async () => ({ ok: true }),
    },
  });

  const installHandler = handlers["aiwf:installTemplatePack"];
  const exportHandler = handlers["aiwf:exportTemplatePack"];
  if (typeof installHandler !== "function" || typeof exportHandler !== "function") {
    fail({
      status: "failed",
      issues: ["template pack IPC handlers are missing"],
    });
  }

  const installResult = await installHandler(null, { pack: legacyPack });
  if (!installResult?.ok) {
    fail({
      status: "failed",
      issues: [`template pack install failed: ${String(installResult?.error || "unknown")}`],
    });
  }
  if (!installResult.migrated) {
    fail({
      status: "failed",
      issues: ["template pack install did not surface migration for legacy artifact"],
    });
  }
  if (String(installResult?.item?.schema_version || "") !== TEMPLATE_PACK_ENTRY_SCHEMA_VERSION) {
    fail({
      status: "failed",
      issues: ["template pack install did not normalize marketplace entry schema version"],
    });
  }

  const exportPath = path.join(tempRoot, "template_pack_export.json");
  const exportResult = await exportHandler(null, {
    id: "pack_1",
    mock: true,
    path: exportPath,
  });
  if (!exportResult?.ok) {
    fail({
      status: "failed",
      issues: [`template pack export failed: ${String(exportResult?.error || "unknown")}`],
    });
  }
  const exportedArtifact = JSON.parse(fs.readFileSync(exportPath, "utf8"));
  if (String(exportedArtifact?.schema_version || "") !== TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION) {
    fail({
      status: "failed",
      issues: ["template pack export did not write artifact schema version"],
    });
  }
  const exportedTemplate = Array.isArray(exportedArtifact?.templates) ? exportedArtifact.templates[0] : null;
  if (!exportedTemplate?.workflow_definition || Object.prototype.hasOwnProperty.call(exportedTemplate, "graph")) {
    fail({
      status: "failed",
      issues: ["template pack export did not write workflow_definition as the canonical template field"],
    });
  }

  console.log(JSON.stringify({
    status: "passed",
    schemaPath,
    artifactSchemaVersion: TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION,
    marketplaceEntrySchemaVersion: TEMPLATE_PACK_ENTRY_SCHEMA_VERSION,
    importMigrated: normalized.migrated,
    installMigrated: !!installResult.migrated,
    exportedArtifactSchemaVersion: String(exportedArtifact.schema_version || ""),
    templateCount: Array.isArray(exportedArtifact.templates) ? exportedArtifact.templates.length : 0,
    exportedTemplateField: "workflow_definition",
    exportedLegacyGraphFieldPresent: Object.prototype.hasOwnProperty.call(exportedTemplate || {}, "graph"),
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
  throw "template pack contract sync checks failed"
}

Ok "template pack contract sync check passed"
