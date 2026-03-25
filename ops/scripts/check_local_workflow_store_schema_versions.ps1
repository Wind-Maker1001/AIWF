param(
  [string]$RepoRoot = "",
  [string[]]$RequireRuntimeOutputs = @()
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

$requiredOutputsJson = ConvertTo-Json -InputObject @($RequireRuntimeOutputs) -Compress

$nodeScript = @'
const fs = require("fs");
const os = require("os");
const path = require("path");

function uniqueSorted(values) {
  return Array.from(new Set((Array.isArray(values) ? values : []).filter(Boolean))).sort();
}

function failPayload(payload) {
  console.log(JSON.stringify(payload));
  process.exit(1);
}

(async () => {
  const repoRoot = process.argv[2];
  const requiredRuntimeOutputs = JSON.parse(process.env.AIWF_REQUIRED_RUNTIME_OUTPUTS_JSON || "[]");

  const stateModulePath = path.join(repoRoot, "apps", "dify-desktop", "workflow_ipc_state.js");
  const stateModuleText = fs.readFileSync(stateModulePath, "utf8");
  const missingSourceSchemaVersionModules = stateModuleText.includes("schema_version")
    ? []
    : ["workflow_ipc_state"];

  const stateModule = require(stateModulePath);
  const {
    TEMPLATE_PACK_ENTRY_SCHEMA_VERSION,
    TEMPLATE_MARKETPLACE_SCHEMA_VERSION,
    WORKFLOW_NODE_CACHE_METRICS_SCHEMA_VERSION,
    WORKFLOW_NODE_CACHE_SCHEMA_VERSION,
    WORKFLOW_QUEUE_CONTROL_SCHEMA_VERSION,
    WORKFLOW_TASK_QUEUE_SCHEMA_VERSION,
    createWorkflowIpcStateSupport,
  } = stateModule;

  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-local-workflow-store-"));
  const paths = {
    userData: path.join(root, "userData"),
    documents: path.join(root, "documents"),
    desktop: path.join(root, "desktop"),
    output: path.join(root, "output"),
  };
  Object.values(paths).forEach((dir) => fs.mkdirSync(dir, { recursive: true }));

  const support = createWorkflowIpcStateSupport({
    app: {
      isPackaged: false,
      getPath(name) {
        return paths[name];
      },
    },
    fs,
    path,
    loadConfig: () => ({ outputRoot: paths.output }),
    nowIso: () => "2026-03-24T00:00:00.000Z",
  });

  const runtimeCoveredOutputs = [];
  const missingRuntimeSchemaVersionOutputs = [];
  function recordOutput(name, predicate) {
    if (predicate) runtimeCoveredOutputs.push(name);
    else missingRuntimeSchemaVersionOutputs.push(name);
  }

  support.saveWorkflowQueue([{ run_id: "run_1" }]);
  const queueJson = JSON.parse(fs.readFileSync(support.workflowQueuePath(), "utf8"));
  recordOutput("workflow_task_queue_file", queueJson.schema_version === WORKFLOW_TASK_QUEUE_SCHEMA_VERSION);

  support.saveQueueControl({ paused: true, quotas: { alpha: 2 } });
  const queueControlJson = JSON.parse(fs.readFileSync(support.workflowQueueControlPath(), "utf8"));
  recordOutput("workflow_queue_control_file", queueControlJson.schema_version === WORKFLOW_QUEUE_CONTROL_SCHEMA_VERSION);

  support.saveTemplateMarketplace([{ id: "pack_1", name: "Pack One", templates: [] }]);
  const templateMarketplaceJson = JSON.parse(fs.readFileSync(support.templateMarketplacePath(), "utf8"));
  recordOutput("template_marketplace_file", templateMarketplaceJson.schema_version === TEMPLATE_MARKETPLACE_SCHEMA_VERSION);
  recordOutput(
    "template_pack_entry",
    Array.isArray(templateMarketplaceJson.items)
      && templateMarketplaceJson.items.length === 1
      && templateMarketplaceJson.items[0].schema_version === TEMPLATE_PACK_ENTRY_SCHEMA_VERSION
  );

  const cache = support.createNodeCacheApi();
  cache.set("n1", { ok: true });
  const nodeCacheJson = JSON.parse(fs.readFileSync(support.nodeCachePath(), "utf8"));
  const nodeCacheMetricsJson = JSON.parse(fs.readFileSync(support.nodeCacheMetricsPath(), "utf8"));
  recordOutput("workflow_node_cache_file", nodeCacheJson.schema_version === WORKFLOW_NODE_CACHE_SCHEMA_VERSION);
  recordOutput("workflow_node_cache_metrics_file", nodeCacheMetricsJson.schema_version === WORKFLOW_NODE_CACHE_METRICS_SCHEMA_VERSION);

  fs.writeFileSync(support.workflowQueuePath(), `${JSON.stringify({ items: [{ run_id: "legacy_run" }] }, null, 2)}\n`, "utf8");
  fs.writeFileSync(support.workflowQueueControlPath(), `${JSON.stringify({ paused: false, quotas: { beta: 3 } }, null, 2)}\n`, "utf8");
  fs.writeFileSync(support.templateMarketplacePath(), `${JSON.stringify({ items: [{ id: "legacy_pack", name: "Legacy Pack", templates: [] }] }, null, 2)}\n`, "utf8");
  fs.writeFileSync(support.nodeCachePath(), `${JSON.stringify({ items: { n2: { output: { ok: true }, ts: "2026-03-24T00:00:00.000Z" } }, order: ["n2"] }, null, 2)}\n`, "utf8");
  fs.writeFileSync(support.nodeCacheMetricsPath(), `${JSON.stringify({ hits: 2, misses: 1, sets: 1, last_reset_at: "", updated_at: "" }, null, 2)}\n`, "utf8");

  const legacyQueueRead = JSON.stringify(support.loadWorkflowQueue()) === JSON.stringify([{ run_id: "legacy_run" }]);
  const legacyControl = support.loadQueueControl();
  const legacyControlRead = JSON.stringify(legacyControl) === JSON.stringify({ paused: false, quotas: { beta: 3 } });
  const legacyTemplateItems = support.listTemplateMarketplace(20);
  const legacyTemplateRead = Array.isArray(legacyTemplateItems)
    && legacyTemplateItems.length === 1
    && legacyTemplateItems[0].schema_version === TEMPLATE_PACK_ENTRY_SCHEMA_VERSION
    && legacyTemplateItems[0].id === "legacy_pack"
    && legacyTemplateItems[0].name === "Legacy Pack"
    && legacyTemplateItems[0].version === "v1"
    && legacyTemplateItems[0].source === "unknown"
    && Array.isArray(legacyTemplateItems[0].templates)
    && legacyTemplateItems[0].templates.length === 0;
  const legacyCacheStore = support.createNodeCacheApi();
  const legacyCacheRead = JSON.stringify(legacyCacheStore.get("n2")) === JSON.stringify({ ok: true });

  const requiredOutputSet = uniqueSorted([
    "template_pack_entry",
    "workflow_task_queue_file",
    "workflow_queue_control_file",
    "template_marketplace_file",
    "workflow_node_cache_file",
    "workflow_node_cache_metrics_file",
  ]);
  const coveredOutputSet = uniqueSorted(runtimeCoveredOutputs);
  const missingRequiredRuntimeOutputs = uniqueSorted((Array.isArray(requiredRuntimeOutputs) ? requiredRuntimeOutputs : []).filter((name) => !coveredOutputSet.includes(String(name))));

  const issues = [];
  if (missingSourceSchemaVersionModules.length > 0) {
    issues.push(`source modules missing schema_version markers: ${missingSourceSchemaVersionModules.join(", ")}`);
  }
  if (missingRuntimeSchemaVersionOutputs.length > 0) {
    issues.push(`runtime outputs missing schema_version: ${uniqueSorted(missingRuntimeSchemaVersionOutputs).join(", ")}`);
  }
  if (!legacyQueueRead) issues.push("legacy workflow queue payload no longer migrates");
  if (!legacyControlRead) issues.push("legacy workflow queue control payload no longer migrates");
  if (!legacyTemplateRead) issues.push("legacy template marketplace payload no longer migrates");
  if (!legacyCacheRead) issues.push("legacy workflow node cache payload no longer migrates");
  if (missingRequiredRuntimeOutputs.length > 0) {
    issues.push(`required runtime outputs missing: ${missingRequiredRuntimeOutputs.join(", ")}`);
  }

  const payload = {
    status: issues.length > 0 ? "failed" : "passed",
    requiredStoreModules: ["workflow_ipc_state"],
    sourceModuleCount: 1,
    sourceSchemaVersionCount: 1 - missingSourceSchemaVersionModules.length,
    requiredRuntimeOutputs: requiredOutputSet,
    runtimeCheckCount: requiredOutputSet.length,
    runtimeSchemaVersionCount: coveredOutputSet.length,
    legacyReads: {
      workflow_task_queue: legacyQueueRead,
      workflow_queue_control: legacyControlRead,
      template_marketplace: legacyTemplateRead,
      workflow_node_cache: legacyCacheRead,
    },
    drift: {
      missingSourceSchemaVersionModules: uniqueSorted(missingSourceSchemaVersionModules),
      missingRuntimeSchemaVersionOutputs: uniqueSorted(missingRuntimeSchemaVersionOutputs),
      missingRequiredRuntimeOutputs,
    },
    issues: uniqueSorted(issues),
  };

  if (payload.status !== "passed") {
    failPayload(payload);
  }
  console.log(JSON.stringify(payload));
})().catch((error) => {
  failPayload({
    status: "failed",
    requiredStoreModules: ["workflow_ipc_state"],
    sourceModuleCount: 1,
    sourceSchemaVersionCount: 0,
    requiredRuntimeOutputs: [],
    runtimeCheckCount: 0,
    runtimeSchemaVersionCount: 0,
    legacyReads: {},
    drift: {
      missingSourceSchemaVersionModules: [],
      missingRuntimeSchemaVersionOutputs: [],
      missingRequiredRuntimeOutputs: [],
    },
    issues: [error && error.stack ? error.stack : String(error)],
  });
});
'@

$env:AIWF_REQUIRED_RUNTIME_OUTPUTS_JSON = $requiredOutputsJson
try {
  $nodeScript | node - $RepoRoot
}
finally {
  Remove-Item Env:AIWF_REQUIRED_RUNTIME_OUTPUTS_JSON -ErrorAction SilentlyContinue
}
if ($LASTEXITCODE -ne 0) {
  throw "local workflow store schema version checks failed"
}

Ok "local workflow store schema version check passed"
