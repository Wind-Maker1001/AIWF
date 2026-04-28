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
const path = require("path");
const { pathToFileURL } = require("url");

function fail(payload) {
  console.log(JSON.stringify(payload));
  process.exit(1);
}

(async () => {
  const repoRoot = process.argv[2];
  const schemaPath = path.join(repoRoot, "contracts", "desktop", "local_template_storage.schema.json");
  const storageContract = await import(pathToFileURL(path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "template-storage-contract.js")).href);
  const marketplaceModule = await import(pathToFileURL(path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "template-ui-marketplace.js")).href);

  const {
    LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
    LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
    normalizeLocalTemplateStorage,
  } = storageContract;
  const { createWorkflowTemplateMarketplaceSupport } = marketplaceModule;

  const schema = JSON.parse(require("fs").readFileSync(schemaPath, "utf8"));
  const storageSchemaConst = String(schema?.properties?.schema_version?.const || "");
  const entrySchemaConst = String(schema?.definitions?.localTemplateEntry?.properties?.schema_version?.const || "");
  const schemaDeclaresLegacyGraph = Object.prototype.hasOwnProperty.call(schema?.definitions?.localTemplateEntry?.properties || {}, "graph");
  if (storageSchemaConst !== LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION) {
    fail({
      status: "failed",
      schemaPath,
      issues: [`local template storage schema_version const drift: ${storageSchemaConst || "(missing)"}`],
    });
  }
  if (entrySchemaConst !== LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION) {
    fail({
      status: "failed",
      schemaPath,
      issues: [`local template entry schema_version const drift: ${entrySchemaConst || "(missing)"}`],
    });
  }
  if (schemaDeclaresLegacyGraph) {
    fail({
      status: "failed",
      schemaPath,
      issues: ["local template storage schema still declares legacy graph field"],
    });
  }

  function templateGraph() {
    return {
      workflow_id: "wf_local_template_gate",
      version: "1.0.0",
      name: "Local Template Gate",
      nodes: [{ id: "n1", type: "ingest_files", config: {} }],
      edges: [],
    };
  }

  const normalized = normalizeLocalTemplateStorage([{
    id: "custom_1",
    name: "Legacy Local Template",
    graph: templateGraph(),
  }], {
    allowStorageSchemaMigration: true,
    allowEntrySchemaMigration: true,
    allowLegacyGraphAlias: true,
  });
  if (!normalized.migrated || normalized.schema_version !== LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION) {
    fail({
      status: "failed",
      issues: ["legacy local template storage did not migrate to versioned envelope"],
    });
  }
  if (!Array.isArray(normalized.items) || normalized.items.length !== 1 || normalized.items[0].schema_version !== LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION) {
    fail({
      status: "failed",
      issues: ["legacy local template entry did not migrate to versioned entry schema"],
    });
  }
  if (!normalized.items[0]?.workflow_definition || Object.prototype.hasOwnProperty.call(normalized.items[0], "graph")) {
    fail({
      status: "failed",
      issues: ["normalized local template entry did not converge to workflow_definition field"],
    });
  }

  const localStorageState = {
    "aiwf.workflow.templates.v1": JSON.stringify([{
      id: "custom_1",
      name: "Legacy Local Template",
      graph: templateGraph(),
    }]),
  };

  global.window = {
    localStorage: {
      getItem(key) {
        return localStorageState[key] || null;
      },
      setItem(key, value) {
        localStorageState[key] = value;
      },
    },
  };

  const support = createWorkflowTemplateMarketplaceSupport({
    workflowName: { value: "My Flow" },
    templateSelect: { value: "" },
  }, {
    graphPayload: () => templateGraph(),
    currentTemplateGovernance: () => ({ preflight_gate_required: true }),
    parseRunParamsLoose: () => ({ region: "cn" }),
    renderTemplateSelect: () => {},
    setStatus: () => {},
  });

  const loadedTemplates = support.loadLocalTemplates();
  const migratedStorage = JSON.parse(localStorageState["aiwf.workflow.templates.v1"]);
  const localStorageNormalizedOnLoad = Array.isArray(loadedTemplates)
    && loadedTemplates.length === 1
    && loadedTemplates[0].schema_version === LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION
    && migratedStorage.schema_version === LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION
    && Array.isArray(migratedStorage.items)
    && migratedStorage.items[0].schema_version === LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION;
  if (!localStorageNormalizedOnLoad) {
    fail({
      status: "failed",
      issues: ["template marketplace support did not rewrite legacy local storage envelope"],
    });
  }

  global.prompt = () => "My Local Template";
  support.saveCurrentAsTemplate();
  const savedStorage = JSON.parse(localStorageState["aiwf.workflow.templates.v1"]);
  const localSaveVersioned = savedStorage.schema_version === LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION
    && Array.isArray(savedStorage.items)
    && savedStorage.items.some((item) => String(item?.name || "") === "My Local Template" && item.schema_version === LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION);
  delete global.prompt;
  delete global.window;

  if (!localSaveVersioned) {
    fail({
      status: "failed",
      issues: ["saveCurrentAsTemplate did not persist versioned local template storage"],
    });
  }
  const savedTemplate = Array.isArray(savedStorage.items)
    ? savedStorage.items.find((item) => String(item?.name || "") === "My Local Template")
    : null;
  if (!savedTemplate?.workflow_definition || Object.prototype.hasOwnProperty.call(savedTemplate, "graph")) {
    fail({
      status: "failed",
      issues: ["saveCurrentAsTemplate did not persist workflow_definition as the canonical template field"],
    });
  }

  console.log(JSON.stringify({
    status: "passed",
    schemaPath,
    storageSchemaVersion: LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
    entrySchemaVersion: LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
    legacyStorageMigrated: normalized.migrated,
    localStorageNormalizedOnLoad,
    localSaveVersioned,
    savedEntryCount: Array.isArray(savedStorage.items) ? savedStorage.items.length : 0,
    savedTemplateField: "workflow_definition",
    savedLegacyGraphFieldPresent: Object.prototype.hasOwnProperty.call(savedTemplate || {}, "graph"),
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
  throw "local template storage contract sync checks failed"
}

Ok "local template storage contract sync check passed"
