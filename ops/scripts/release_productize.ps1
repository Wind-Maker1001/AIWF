param(
  [Parameter(Mandatory = $true)][string]$Version,
  [string]$Channel = "stable",
  [switch]$IncludeBundledTools,
  [switch]$CollectBundledTools
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$pkg = Join-Path $PSScriptRoot "package_offline_bundle.ps1"
$zip = Join-Path $PSScriptRoot "zip_offline_bundle.ps1"
$asyncTrend = Join-Path $PSScriptRoot "check_async_bench_trend.ps1"
$outRoot = Join-Path $root "release"
New-Item -ItemType Directory -Path $outRoot -Force | Out-Null

if (Test-Path $asyncTrend) {
  Info "running async bench trend release gate"
  powershell -ExecutionPolicy Bypass -File $asyncTrend -AccelUrl "http://127.0.0.1:18082" -Tasks 10 -RowsPerTask 500 -TimeoutSeconds 90
  if ($LASTEXITCODE -ne 0) { throw "release blocked by async bench trend gate" }
  Ok "async bench trend release gate passed"
}

foreach ($type in @("installer", "portable")) {
  Info "packaging $type"
  $args = @{
    Version = $Version
    PackageType = $type
    CleanOldReleases = $false
    ReleaseChannel = $Channel
    RequireChineseOcr = $true
  }
  if ($IncludeBundledTools) { $args.IncludeBundledTools = $true }
  if ($CollectBundledTools) { $args.CollectBundledTools = $true }
  & $pkg @args

  Info "zipping $type"
  & $zip -Version $Version -PackageType $type

  $bundleDir = Join-Path $root ("release\offline_bundle_{0}_{1}\AIWF_Offline_Bundle" -f $Version, $type)
  $zipPath = Join-Path $root ("release\offline_bundle_{0}_{1}\AIWF_Offline_Bundle.zip" -f $Version, $type)
  if (-not (Test-Path $bundleDir)) { throw "bundle missing: $bundleDir" }
  if (-not (Test-Path $zipPath)) { throw "zip missing: $zipPath" }

  $sumPath = Join-Path $bundleDir "SHA256SUMS.txt"
  if (-not (Test-Path $sumPath)) { throw "sha sums missing: $sumPath" }

  $manifest = Join-Path $bundleDir "manifest.json"
  $notes = Join-Path $bundleDir "RELEASE_NOTES.md"
  if (-not (Test-Path $manifest)) { throw "manifest missing: $manifest" }
  if (-not (Test-Path $notes)) { throw "release notes missing: $notes" }
}

Ok "release ready: release/offline_bundle_${Version}_installer and _portable"
