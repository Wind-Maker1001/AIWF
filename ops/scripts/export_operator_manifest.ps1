param(
  [string]$RepoRoot = "",
  [string]$OutputPath = "",
  [string]$DesktopModulePath = "",
  [string]$RendererModulePath = ""
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

$supportPath = Join-Path $PSScriptRoot "operator_manifest_support.js"
if (-not (Test-Path $supportPath)) {
  throw "operator manifest support script not found: $supportPath"
}

$nodeScript = @'
const fs = require("fs");
const path = require("path");
const support = require(process.argv[2]);

const repoRoot = process.argv[3];
const outputPathArg = process.argv[4];
const desktopModulePathArg = process.argv[5];
const rendererModulePathArg = process.argv[6];
const manifestPath = support.getManifestPath(repoRoot, outputPathArg || support.DEFAULT_MANIFEST_RELATIVE_PATH);
const schemaPath = support.getManifestPath(repoRoot, support.DEFAULT_MANIFEST_SCHEMA_RELATIVE_PATH);
const desktopModulePath = support.getDesktopModulePath(repoRoot, desktopModulePathArg || support.DEFAULT_DESKTOP_MODULE_RELATIVE_PATH);
const rendererModulePath = support.getRendererModulePath(repoRoot, rendererModulePathArg || support.DEFAULT_RENDERER_MODULE_RELATIVE_PATH);
if (!fs.existsSync(schemaPath)) {
  throw new Error(`operator manifest schema missing: ${schemaPath}`);
}
const manifest = support.buildOperatorManifest(repoRoot);
const desktopModuleSource = support.buildDesktopRustOperatorModuleSource(manifest);
const rendererModuleSource = support.buildRendererRustOperatorModuleSource(manifest);

fs.mkdirSync(path.dirname(manifestPath), { recursive: true });
fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
fs.mkdirSync(path.dirname(desktopModulePath), { recursive: true });
fs.writeFileSync(desktopModulePath, desktopModuleSource, "utf8");
fs.mkdirSync(path.dirname(rendererModulePath), { recursive: true });
fs.writeFileSync(rendererModulePath, rendererModuleSource, "utf8");

console.log(JSON.stringify({
  manifestPath,
  schemaPath,
  desktopModulePath,
  rendererModulePath,
  operatorCount: manifest.summary.known_operator_count,
  publishedCount: manifest.summary.published_operator_count,
  workflowExposableCount: manifest.summary.workflow_exposable_count,
  desktopExposableCount: manifest.summary.desktop_exposable_count,
  directHttpOnlyCount: manifest.summary.direct_http_only_count,
  internalOnlyCount: manifest.summary.internal_only_count,
}));
'@

$resolvedOutputPath = $OutputPath
if ([string]::IsNullOrWhiteSpace($resolvedOutputPath)) {
  $resolvedOutputPath = ""
}
$resolvedDesktopModulePath = $DesktopModulePath
if ([string]::IsNullOrWhiteSpace($resolvedDesktopModulePath)) {
  $resolvedDesktopModulePath = ""
}
$resolvedRendererModulePath = $RendererModulePath
if ([string]::IsNullOrWhiteSpace($resolvedRendererModulePath)) {
  $resolvedRendererModulePath = ""
}

$nodeScript | node - $supportPath $RepoRoot $resolvedOutputPath $resolvedDesktopModulePath $resolvedRendererModulePath
if ($LASTEXITCODE -ne 0) {
  throw "operator manifest export failed"
}

Ok "operator manifest exported"
