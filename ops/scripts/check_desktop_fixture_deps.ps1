param(
  [string]$DesktopDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if ([string]::IsNullOrWhiteSpace($DesktopDir)) {
  $DesktopDir = Join-Path $root "apps\dify-desktop"
}

$requirements = @(
  [ordered]@{
    name = "exceljs"
    path = Join-Path $DesktopDir "node_modules\exceljs\package.json"
    reason = "real XLSX regression fixtures and local workbook parsing checks"
  }
)

$missing = @()
foreach ($requirement in $requirements) {
  if (Test-Path ([string]$requirement.path)) {
    Ok ("desktop fixture dependency present: {0}" -f [string]$requirement.name)
  } else {
    Warn ("desktop fixture dependency missing: {0} ({1})" -f [string]$requirement.name, [string]$requirement.reason)
    $missing += $requirement
  }
}

if (@($missing).Count -gt 0) {
  $names = @($missing | ForEach-Object { [string]$_.name }) -join ", "
  throw ("desktop fixture dependency check failed: missing {0}. Run npm install in {1} before validating real XLSX/image desktop fixtures locally." -f $names, $DesktopDir)
}

Info "desktop fixture dependency check passed"
