param(
  [string]$Root = "",
  [string[]]$KeepNames = @(
    "native_winui_bundle_2026.03.21-personal",
    "native_winui_bundle_2026.03.21-managed",
    "native_winui_msix_2026.03.21-personal",
    "native_winui_msix_2026.03.21-managed",
    "release_frontend_audit_2026.03.21-personal.json",
    "release_frontend_audit_2026.03.21-managed.json"
  ),
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Act($m){ if($DryRun){ Write-Host "[DRY ] $m" -ForegroundColor Yellow } else { Write-Host "[ACT ] $m" -ForegroundColor DarkCyan } }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$releaseRoot = Join-Path $Root "release"
if (-not (Test-Path $releaseRoot)) {
  throw "release root not found: $releaseRoot"
}

$removed = 0
$items = Get-ChildItem $releaseRoot
foreach ($item in $items) {
  if ($KeepNames -contains $item.Name) { continue }

  $isNativeWinuiDir = $item.PSIsContainer -and ($item.Name -like "native_winui_*" -or $item.Name -like "msix_*")
  $isFrontendAudit = (-not $item.PSIsContainer) -and ($item.Name -like "release_frontend_audit_*")
  if (-not $isNativeWinuiDir -and -not $isFrontendAudit) { continue }

  Act ("remove " + $item.FullName)
  if (-not $DryRun) {
    if ($item.PSIsContainer) { Remove-Item $item.FullName -Recurse -Force }
    else { Remove-Item $item.FullName -Force }
  }
  $removed += 1
}

if ($DryRun) {
  Ok ("native winui release cleanup dry run finished; candidates=" + $removed)
} else {
  Ok ("native winui release cleanup finished; removed=" + $removed)
}
