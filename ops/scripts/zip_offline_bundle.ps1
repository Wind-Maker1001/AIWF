param(
  [string]$Version = "",
  [ValidateSet("installer", "portable", "")]
  [string]$PackageType = "",
  [string]$BundleDir = "",
  [string]$ZipPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $BundleDir) {
  if ($Version) {
    if ($PackageType) {
      $BundleDir = Join-Path $root ("release\offline_bundle_{0}_{1}\AIWF_Offline_Bundle" -f $Version, $PackageType)
    } else {
      $candNew = Join-Path $root ("release\offline_bundle_{0}_installer\AIWF_Offline_Bundle" -f $Version)
      $candOld = Join-Path $root ("release\offline_bundle_{0}\AIWF_Offline_Bundle" -f $Version)
      $BundleDir = if (Test-Path $candNew) { $candNew } else { $candOld }
    }
  } else {
    $releaseRoot = Join-Path $root "release"
    $latest = Get-ChildItem $releaseRoot -Directory -Filter "offline_bundle_*" -ErrorAction SilentlyContinue |
      Sort-Object LastWriteTime -Descending |
      Select-Object -First 1
    if (-not $latest) {
      throw "no offline bundle directory found under: $releaseRoot"
    }
    $BundleDir = Join-Path $latest.FullName "AIWF_Offline_Bundle"
  }
}
if (-not $ZipPath) {
  $ZipPath = Join-Path (Split-Path $BundleDir -Parent) "AIWF_Offline_Bundle.zip"
}

if (-not (Test-Path $BundleDir)) {
  throw "bundle dir not found: $BundleDir"
}

Add-Type -AssemblyName System.IO.Compression.FileSystem

if (Test-Path $ZipPath) {
  Remove-Item $ZipPath -Force
}

Info "zipping $BundleDir"
[System.IO.Compression.ZipFile]::CreateFromDirectory($BundleDir, $ZipPath, [System.IO.Compression.CompressionLevel]::Optimal, $false)
Ok "zip ready: $ZipPath"
