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

$files = @(
  "office_themes_desktop.json",
  "office_layouts_desktop.json",
  "cleaning_templates_desktop.json"
)

function Ensure-Dir([string]$p) {
  if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p -Force | Out-Null }
}

Ensure-Dir $PackRoot

if ($Action -eq "export") {
  $dst = Join-Path $PackRoot ("pack_{0}" -f $Version)
  Ensure-Dir $dst
  foreach ($f in $files) {
    $src = Join-Path $SourceDir $f
    if (-not (Test-Path $src)) { throw "missing template file: $src" }
    Copy-Item $src (Join-Path $dst $f) -Force
  }
  $manifest = @{
    version = $Version
    created_at = (Get-Date).ToString("o")
    files = $files
  } | ConvertTo-Json -Depth 4
  Set-Content -Path (Join-Path $dst "manifest.json") -Value $manifest -Encoding UTF8
  Ok "template pack exported: $dst"
  exit 0
}

if ($Action -eq "import") {
  $srcPack = Join-Path $PackRoot ("pack_{0}" -f $Version)
  if (-not (Test-Path $srcPack)) { throw "pack not found: $srcPack" }
  foreach ($f in $files) {
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
  foreach ($f in $files) {
    $src = Join-Path $srcPack $f
    if (-not (Test-Path $src)) { throw "missing rollback file: $src" }
    Copy-Item $src (Join-Path $TargetDir $f) -Force
  }
  Ok "template rollback applied from: $srcPack"
  exit 0
}
