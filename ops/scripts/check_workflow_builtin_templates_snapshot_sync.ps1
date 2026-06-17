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
const path = require("path");
const { pathToFileURL } = require("url");

function fail(payload) {
  console.log(JSON.stringify(payload));
  process.exit(1);
}

function normalizeBuiltinTemplate(item = {}) {
  const workflowDefinition = JSON.parse(JSON.stringify(item.workflow_definition || item.graph || {}));
  if (workflowDefinition && typeof workflowDefinition === "object") {
    delete workflowDefinition.version;
  }
  return {
    id: String(item.id || ""),
    name: String(item.name || ""),
    params_schema: item.params_schema || {},
    workflow_definition: workflowDefinition,
  };
}

function sortTemplates(items) {
  return [...items].sort((left, right) => {
    const byId = String(left.id || "").localeCompare(String(right.id || ""), undefined, { sensitivity: "base" });
    if (byId !== 0) return byId;
    return String(left.name || "").localeCompare(String(right.name || ""), undefined, { sensitivity: "base" });
  });
}

(async () => {
  const repoRoot = process.argv[2];
  const staticConfigPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "static-config.js");
  const snapshotPath = path.join(repoRoot, "contracts", "desktop", "workflow_builtin_templates.v1.json");

  const staticConfigModule = await import(pathToFileURL(staticConfigPath).href);
  const snapshot = JSON.parse(fs.readFileSync(snapshotPath, "utf8"));
  const builtinTemplates = Array.isArray(staticConfigModule.BUILTIN_TEMPLATES)
    ? staticConfigModule.BUILTIN_TEMPLATES
    : [];
  const snapshotItems = Array.isArray(snapshot?.items) ? snapshot.items : [];

  if (String(snapshot?.schema_version || "") !== "workflow_builtin_templates.v1") {
    fail({
      status: "failed",
      issues: [`workflow builtin template snapshot schema_version drift: ${String(snapshot?.schema_version || "(missing)")}`],
      snapshotPath,
      staticConfigPath,
    });
  }

  const normalizedSnapshot = sortTemplates(snapshotItems.map(normalizeBuiltinTemplate));
  const normalizedStaticConfig = sortTemplates(builtinTemplates.map(normalizeBuiltinTemplate));
  const equivalent = JSON.stringify(normalizedSnapshot) === JSON.stringify(normalizedStaticConfig);
  if (!equivalent) {
    fail({
      status: "failed",
      issues: ["workflow builtin template snapshot drifted from desktop static-config builtins"],
      snapshotPath,
      staticConfigPath,
      schemaVersion: String(snapshot.schema_version || ""),
      snapshotTemplateIds: normalizedSnapshot.map((item) => item.id),
      staticConfigTemplateIds: normalizedStaticConfig.map((item) => item.id),
    });
  }

  console.log(JSON.stringify({
    status: "passed",
    snapshotPath,
    staticConfigPath,
    schemaVersion: String(snapshot.schema_version || ""),
    templateCount: normalizedSnapshot.length,
    templateIds: normalizedSnapshot.map((item) => item.id),
    equivalent,
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
  throw "workflow builtin template snapshot sync checks failed"
}

Ok "workflow builtin template snapshot sync check passed"
