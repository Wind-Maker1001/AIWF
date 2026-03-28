param(
  [string]$Root = "",
  [ValidateSet("Debug", "Release")]
  [string]$Configuration = "Debug",
  [switch]$SkipBuild,
  [int]$AutoExitMs = 0,
  [string]$OutputDir = "",
  [switch]$SkipEnsureGlueBridge
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

function Resolve-NativeWinUiExePath([string]$Root, [string]$Configuration) {
  $candidates = @(
    (Join-Path $Root ("apps\dify-native-winui\src\WinUI3Bootstrap\bin\x64\" + $Configuration + "\net8.0-windows10.0.19041.0\win-x64\WinUI3Bootstrap.exe")),
    (Join-Path $Root ("apps\dify-native-winui\src\WinUI3Bootstrap\bin\" + $Configuration + "\net8.0-windows10.0.19041.0\win-x64\WinUI3Bootstrap.exe"))
  )

  return $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
}

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$projectPath = Join-Path $Root "apps\dify-native-winui\src\WinUI3Bootstrap\WinUI3Bootstrap.csproj"
if (-not (Test-Path $projectPath)) {
  throw "native winui project not found: $projectPath"
}

if (-not $SkipEnsureGlueBridge) {
  $ensureGlueScript = Join-Path $PSScriptRoot "ensure_local_governance_bridge.ps1"
  if (-not (Test-Path $ensureGlueScript)) {
    throw "ensure_local_governance_bridge script not found: $ensureGlueScript"
  }
  Info "ensuring local governance bridge is healthy"
  & $ensureGlueScript -Root $Root -StartIfMissing
  if ($LASTEXITCODE -ne 0) {
    throw "local governance bridge is not healthy"
  }
  $env:AIWF_MANUAL_REVIEW_PROVIDER = "glue_http"
  $env:AIWF_QUALITY_RULE_SET_PROVIDER = "glue_http"
  $env:AIWF_WORKFLOW_APP_REGISTRY_PROVIDER = "glue_http"
  $env:AIWF_WORKFLOW_VERSION_PROVIDER = "glue_http"
  $env:AIWF_RUN_BASELINE_PROVIDER = "glue_http"
  Info "default launch will use backend-owned manual review, quality rule, app registry, workflow version, and run baseline providers; workflow run audit remains local-runtime by default"
} else {
  Warn "skip local governance bridge health/start check"
}

try {
  dotnet --info | Out-Null
}
catch {
  throw "dotnet SDK not found. Install .NET SDK 8+ and Windows App SDK prerequisites first."
}

if (-not $SkipBuild) {
  Info "building native winui project"
  dotnet build $projectPath -c $Configuration -p:Platform=x64
  if ($LASTEXITCODE -ne 0) {
    throw "native winui build failed"
  }
  Ok "native winui build passed"
} else {
  Warn "skip native winui build"
}

$exePath = Resolve-NativeWinUiExePath -Root $Root -Configuration $Configuration
if (-not $exePath) {
  throw "native winui executable not found: $exePath"
}

$startupLogPath = $null
if ($AutoExitMs -gt 0) {
  if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $OutputDir = Join-Path $Root "ops\logs\run\native-winui\$stamp"
  }

  New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
  $startupLogPath = Join-Path $OutputDir "startup.json"
  $env:AIWF_NATIVE_PERF_LOG_PATH = $startupLogPath
  $env:AIWF_NATIVE_PERF_AUTO_EXIT_MS = "$AutoExitMs"
}

try {
  Info ("launching native winui app: " + $exePath)
  $proc = Start-Process -FilePath $exePath -PassThru
  Ok ("native winui launched, pid=" + $proc.Id)

  if ($AutoExitMs -le 0) {
    return
  }

  $waitMs = [Math]::Max($AutoExitMs + 8000, 12000)
  if (-not $proc.WaitForExit($waitMs)) {
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    throw "native winui app did not exit within ${waitMs}ms"
  }

  Ok ("native winui app exited, exitCode=" + $proc.ExitCode)
  if ($startupLogPath -and (Test-Path $startupLogPath)) {
    Ok ("startup log captured: " + $startupLogPath)
  } else {
    Warn "startup log was not captured"
  }
}
finally {
  if ($AutoExitMs -gt 0) {
    Remove-Item Env:AIWF_NATIVE_PERF_LOG_PATH -ErrorAction SilentlyContinue
    Remove-Item Env:AIWF_NATIVE_PERF_AUTO_EXIT_MS -ErrorAction SilentlyContinue
  }
}
