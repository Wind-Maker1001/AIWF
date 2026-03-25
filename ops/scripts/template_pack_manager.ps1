param(
  [ValidateSet("export", "import", "rollback")][string]$Action = "export",
  [string]$PackRoot = "",
  [string]$Version = "",
  [string]$SourceDir = "",
  [string]$TargetDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $SourceDir) { $SourceDir = Join-Path $root "rules\templates" }
if (-not $TargetDir) { $TargetDir = Join-Path $root "rules\templates" }
if (-not $PackRoot) { $PackRoot = Join-Path $root "release\template_packs" }
if (-not $Version) { $Version = Get-Date -Format "yyyyMMdd_HHmmss" }

$baseFiles = @(
  "office_themes_desktop.json",
  "office_layouts_desktop.json",
  "cleaning_templates_desktop.json"
)
$schemaVersion = "offline_template_catalog_pack_manifest.v1"
$contractSchemaFiles = @(
  "contracts\desktop\office_theme_catalog.schema.json",
  "contracts\desktop\office_layout_catalog.schema.json",
  "contracts\desktop\cleaning_template_registry.schema.json",
  "contracts\desktop\offline_template_catalog_pack_manifest.schema.json"
)

function Ensure-Dir([string]$p) {
  if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null }
}

function Read-PackManifest([string]$ManifestPath) {
  if (-not (Test-Path $ManifestPath)) {
    throw "pack manifest missing: $ManifestPath"
  }
  $manifest = Get-Content -Raw -Encoding UTF8 $ManifestPath | ConvertFrom-Json
  if ([string]::IsNullOrWhiteSpace([string]$manifest.schema_version)) {
    throw "pack manifest schema_version is required: $ManifestPath"
  }
  if ([string]$manifest.schema_version -ne $schemaVersion) {
    throw "unsupported pack manifest schema_version: $([string]$manifest.schema_version)"
  }
  $manifestFiles = @($manifest.files)
  if ($manifestFiles.Count -eq 0) {
    throw "pack manifest files are required: $ManifestPath"
  }
  return $manifest
}

Ensure-Dir $PackRoot

if ($Action -eq "export") {
  $dst = Join-Path $PackRoot ("pack_{0}" -f $Version)
  Ensure-Dir $dst
  $contractsDst = Join-Path $dst "contracts\desktop"
  Ensure-Dir $contractsDst
  $genericFiles = @()
  if (Test-Path $SourceDir) {
    $genericFiles = Get-ChildItem -Path $SourceDir -File -Filter "generic_*.json" | Select-Object -ExpandProperty Name
  }
  $files = @($baseFiles + $genericFiles | Sort-Object -Unique)
  foreach ($f in $files) {
    $src = Join-Path $SourceDir $f
    if (-not (Test-Path $src)) { throw "missing template file: $src" }
    Copy-Item $src (Join-Path $dst $f) -Force
  }
  foreach ($schemaRel in $contractSchemaFiles) {
    $src = Join-Path $root $schemaRel
    if (-not (Test-Path $src)) { throw "missing contract schema: $src" }
    Copy-Item $src (Join-Path $contractsDst (Split-Path $src -Leaf)) -Force
  }
  $catalogs = [ordered]@{}
  foreach ($f in $baseFiles) {
    $parsed = Get-Content -Raw -Encoding UTF8 (Join-Path $SourceDir $f) | ConvertFrom-Json
    $catalogs[$f] = [ordered]@{
      schema_version = [string]$parsed.schema_version
    }
  }
  $manifest = [ordered]@{
    schema_version = $schemaVersion
    version = $Version
    created_at = (Get-Date).ToString("o")
    files = $files
    contract_schemas = @($contractSchemaFiles | ForEach-Object { $_ -replace "\\", "/" })
    catalogs = $catalogs
    generic_templates = @($genericFiles)
  } | ConvertTo-Json -Depth 6
  Set-Content -Path (Join-Path $dst "manifest.json") -Value $manifest -Encoding UTF8
  Ok "template pack exported: $dst"
  exit 0
}

if ($Action -eq "import") {
  $srcPack = Join-Path $PackRoot ("pack_{0}" -f $Version)
  if (-not (Test-Path $srcPack)) { throw "pack not found: $srcPack" }
  $manifest = Read-PackManifest (Join-Path $srcPack "manifest.json")
  foreach ($f in @($manifest.files)) {
    $src = Join-Path $srcPack $f
    if (-not (Test-Path $src)) { throw "missing pack file: $src" }
    Copy-Item $src (Join-Path $TargetDir $f) -Force
  }
  Ok "template pack imported: $srcPack -> $TargetDir"
  exit 0
}

if ($Action -eq "rollback") {
  $srcPack = Join-Path $PackRoot ("pack_{0}" -f $Version)
  if (-not (Test-Path $srcPack)) { throw "pack not found for rollback: $srcPack" }
  $manifest = Read-PackManifest (Join-Path $srcPack "manifest.json")
  foreach ($f in @($manifest.files)) {
    $src = Join-Path $srcPack $f
    if (-not (Test-Path $src)) { throw "missing rollback file: $src" }
    Copy-Item $src (Join-Path $TargetDir $f) -Force
  }
  Ok "template rollback applied from: $srcPack"
  exit 0
}
