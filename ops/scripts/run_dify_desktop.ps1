param(
  [string]$ProjectDir = "",
  [switch]$BuildWin,
  [switch]$BuildInstaller,
  [switch]$OfflineHome,
  [switch]$Workflow,
  [switch]$WorkflowAdmin,
  [switch]$SkipEnsureGlueBridge
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $ProjectDir) {
  $ProjectDir = Join-Path $root "apps\dify-desktop"
}

if (-not (Test-Path $ProjectDir)) {
  throw "dify-desktop not found: $ProjectDir"
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  throw "node not found in PATH"
}
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
  throw "npm not found in PATH"
}

if (($Workflow -and $WorkflowAdmin) -or ($OfflineHome -and ($Workflow -or $WorkflowAdmin))) {
  throw "Use exactly one of -OfflineHome, -Workflow, or -WorkflowAdmin."
}

if ($BuildWin -or $BuildInstaller) {
  throw "Electron packaging is compatibility-only and must use release_electron_compatibility.ps1."
}

if (-not $OfflineHome -and -not $Workflow -and -not $WorkflowAdmin) {
  throw "Electron compatibility launch now requires -OfflineHome, -Workflow, or -WorkflowAdmin."
}

Warn "Electron is the secondary compatibility frontend. WinUI is the primary frontend; use run_aiwf_frontend.ps1 or run_dify_native_winui.ps1 unless you explicitly need Workflow Studio compatibility. Governance and diagnostics surfaces now require explicit admin mode."

if (-not $SkipEnsureGlueBridge) {
  $ensureGlueScript = Join-Path $PSScriptRoot "ensure_local_governance_bridge.ps1"
  if (-not (Test-Path $ensureGlueScript)) {
    throw "ensure_local_governance_bridge script not found: $ensureGlueScript"
  }
  Info "ensuring local governance bridge is healthy"
  & $ensureGlueScript -Root $root -StartIfMissing
  if ($LASTEXITCODE -ne 0) {
    throw "local governance bridge is not healthy"
  }
  $env:AIWF_MANUAL_REVIEW_PROVIDER = "glue_http"
  $env:AIWF_QUALITY_RULE_SET_PROVIDER = "glue_http"
  $env:AIWF_WORKFLOW_APP_REGISTRY_PROVIDER = "glue_http"
  $env:AIWF_WORKFLOW_VERSION_PROVIDER = "glue_http"
  $env:AIWF_RUN_BASELINE_PROVIDER = "glue_http"
  Info "default compatibility launch will use backend-owned manual review, quality rule, app registry, workflow version, and run baseline providers; workflow run audit remains local-runtime by default"
} else {
  Warn "skip local governance bridge health/start check"
}

Push-Location $ProjectDir
try {
  Info "installing desktop dependencies"
  npm install
  if ($LASTEXITCODE -ne 0) { throw "npm install failed" }

  Info "running desktop smoke"
  npm run smoke
  if ($LASTEXITCODE -ne 0) { throw "desktop smoke failed" }
  Ok "desktop smoke passed"

  $devArgs = @("run", "dev")
  if ($WorkflowAdmin) {
    Info "starting desktop app in Legacy Workflow admin mode"
    $devArgs += "--"
    $devArgs += "--workflow-admin"
  } elseif ($OfflineHome) {
    Info "starting desktop app in compatibility offline home mode"
    $devArgs += "--"
    $devArgs += "--offline-home"
  } elseif ($Workflow) {
    Info "starting desktop app in Legacy Workflow compatibility mode"
    $devArgs += "--"
    $devArgs += "--workflow"
  } else {
    Info "starting desktop app"
  }
  & npm @devArgs
}
finally {
  Pop-Location
}
