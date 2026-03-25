param(
  [string]$Root = "",
  [ValidateSet("WinUI", "Electron")]
  [string]$Frontend = "WinUI",
  [ValidateSet("Debug", "Release")]
  [string]$Configuration = "Debug",
  [switch]$SkipBuild,
  [int]$AutoExitMs = 0,
  [string]$OutputDir = "",
  [string]$Version = "",
  [string]$ReleaseChannel = "dev",
  [switch]$CreateZip,
  [switch]$PublishSingleFile,
  [switch]$BuildWin,
  [switch]$BuildInstaller,
  [switch]$Workflow,
  [switch]$WorkflowAdmin,
  [switch]$SkipEnsureGlueBridge
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Resolve-LocalFrontendVersion([string]$RequestedVersion) {
  if (-not [string]::IsNullOrWhiteSpace($RequestedVersion)) {
    return $RequestedVersion.Trim()
  }
  return "0.0.0-local.{0}" -f (Get-Date -Format "yyyyMMddHHmmss")
}

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$winuiScript = Join-Path $PSScriptRoot "run_dify_native_winui.ps1"
$electronScript = Join-Path $PSScriptRoot "run_dify_desktop.ps1"
$publishWinUiScript = Join-Path $PSScriptRoot "publish_native_winui.ps1"
$packageWinUiScript = Join-Path $PSScriptRoot "package_native_winui_bundle.ps1"

if ($Frontend -eq "WinUI") {
  if ($Workflow -or $WorkflowAdmin) {
    throw "Workflow compatibility switches are only supported with -Frontend Electron."
  }
  if ($BuildInstaller) {
    if ($SkipBuild) {
      throw "SkipBuild is not supported for WinUI installer packaging through run_aiwf_frontend.ps1. Use package_native_winui_bundle.ps1 with -PublishedDir and -SkipPublish if you need to reuse an existing publish output."
    }
    $resolvedVersion = Resolve-LocalFrontendVersion $Version
    Info "packaging primary frontend installer bundle: WinUI"
    $packageArgs = @(
      "-ExecutionPolicy", "Bypass",
      "-File", $packageWinUiScript,
      "-Root", $Root,
      "-Version", $resolvedVersion,
      "-Configuration", $Configuration,
      "-ReleaseChannel", $ReleaseChannel
    )
    if ($OutputDir) { $packageArgs += @("-OutDir", $OutputDir) }
    if ($CreateZip -or $BuildInstaller) { $packageArgs += "-CreateZip" }
    powershell @packageArgs
    exit $LASTEXITCODE
  }
  if ($BuildWin) {
    if ($SkipBuild) {
      throw "SkipBuild is not supported for WinUI publish through run_aiwf_frontend.ps1. Use publish_native_winui.ps1 directly if you need a custom publish flow."
    }
    Info "publishing primary frontend app: WinUI"
    $publishArgs = @(
      "-ExecutionPolicy", "Bypass",
      "-File", $publishWinUiScript,
      "-Root", $Root,
      "-Configuration", $Configuration
    )
    if ($OutputDir) { $publishArgs += @("-OutDir", $OutputDir) }
    if ($Version) { $publishArgs += @("-Version", $Version) }
    if ($PublishSingleFile) { $publishArgs += "-PublishSingleFile" }
    powershell @publishArgs
    exit $LASTEXITCODE
  }
  Info "launching primary frontend: WinUI"
  & $winuiScript -Root $Root -Configuration $Configuration -SkipBuild:$SkipBuild -AutoExitMs $AutoExitMs -OutputDir $OutputDir -SkipEnsureGlueBridge:$SkipEnsureGlueBridge
  exit $LASTEXITCODE
}

Warn "Electron is a bounded secondary compatibility frontend. Use WinUI unless you explicitly need Workflow Studio compatibility or Electron packaging."
& $electronScript -ProjectDir (Join-Path $Root "apps\dify-desktop") -BuildWin:$BuildWin -BuildInstaller:$BuildInstaller -Workflow:$Workflow -WorkflowAdmin:$WorkflowAdmin -SkipEnsureGlueBridge:$SkipEnsureGlueBridge
exit $LASTEXITCODE
