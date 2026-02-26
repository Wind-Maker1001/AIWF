param(
  [string]$Version = "1.1.6",
  [string]$OutputRoot = "",
  [int]$FallbackTimeoutSec = 5,
  [switch]$WithAcceptance,
  [switch]$SkipPackagedStartup
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

function Invoke-Step([string]$Name, [scriptblock]$Action) {
  Info $Name
  try {
    $null = & $Action
    Ok "$Name passed"
    return [pscustomobject]([ordered]@{ name = $Name; status = "passed"; error = "" })
  } catch {
    $err = [string]$_.Exception.Message
    Write-Host "[ERR ] $Name failed: $err" -ForegroundColor Red
    return [pscustomobject]([ordered]@{ name = $Name; status = "failed"; error = $err })
  }
}

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$desktopDir = Join-Path $root "apps\dify-desktop"
$startupScript = Join-Path $PSScriptRoot "check_desktop_packaged_startup.ps1"
$fallbackScript = Join-Path $PSScriptRoot "dify_run_with_offline_fallback.ps1"
$acceptReal = Join-Path $PSScriptRoot "acceptance_desktop_real_sample.ps1"
$acceptFinance = Join-Path $PSScriptRoot "acceptance_desktop_finance_template.ps1"

if (-not (Test-Path $desktopDir)) { throw "desktop dir not found: $desktopDir" }
if (-not (Test-Path $fallbackScript)) { throw "fallback script not found: $fallbackScript" }

if (-not $OutputRoot) {
  $OutputRoot = Join-Path $root ("release\gate_v{0}" -f $Version)
}
New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$runDir = Join-Path $OutputRoot $stamp
New-Item -ItemType Directory -Path $runDir -Force | Out-Null
$fallbackOut = Join-Path $runDir "fallback_result.json"

$results = @()

$results += Invoke-Step "desktop unit tests" {
  Push-Location $desktopDir
  try {
    npm run test:unit
    $code = if ($null -ne $LASTEXITCODE) { [int]$LASTEXITCODE } else { 0 }
    if ($code -ne 0) { throw "npm run test:unit exit code $code" }
  } finally {
    Pop-Location
  }
}

$results += Invoke-Step "desktop smoke" {
  Push-Location $desktopDir
  try {
    npm run smoke
    $code = if ($null -ne $LASTEXITCODE) { [int]$LASTEXITCODE } else { 0 }
    if ($code -ne 0) { throw "npm run smoke exit code $code" }
  } finally {
    Pop-Location
  }
}

if (-not $SkipPackagedStartup) {
  $results += Invoke-Step "packaged startup check" {
    powershell -ExecutionPolicy Bypass -File $startupScript
    $code = if ($null -ne $LASTEXITCODE) { [int]$LASTEXITCODE } else { 0 }
    if ($code -ne 0) { throw "check_desktop_packaged_startup.ps1 exit code $code" }
  }
} else {
  Warn "skip packaged startup check"
  $results += [pscustomobject]([ordered]@{ name = "packaged startup check"; status = "skipped"; error = "" })
}

$results += Invoke-Step "fallback scenario check" {
  powershell -ExecutionPolicy Bypass -File $fallbackScript -BaseUrl "http://127.0.0.1:19999" -TimeoutSec $FallbackTimeoutSec -OutputFile $fallbackOut
  $code = if ($null -ne $LASTEXITCODE) { [int]$LASTEXITCODE } else { 0 }
  if ($code -ne 0) { throw "dify_run_with_offline_fallback.ps1 exit code $code" }
  if (-not (Test-Path $fallbackOut)) { throw "fallback output not found: $fallbackOut" }
  $obj = Get-Content $fallbackOut -Raw -Encoding UTF8 | ConvertFrom-Json
  if (-not $obj.ok) { throw "fallback output ok=false" }
  if ([string]$obj.mode -ne "offline_fallback") { throw "fallback mode mismatch: $($obj.mode)" }
}

if ($WithAcceptance) {
  $results += Invoke-Step "acceptance real sample" {
    powershell -ExecutionPolicy Bypass -File $acceptReal -OutputRoot (Join-Path $runDir "acceptance_real")
    $code = if ($null -ne $LASTEXITCODE) { [int]$LASTEXITCODE } else { 0 }
    if ($code -ne 0) { throw "acceptance_desktop_real_sample.ps1 exit code $code" }
  }
  $results += Invoke-Step "acceptance finance template" {
    powershell -ExecutionPolicy Bypass -File $acceptFinance -OutputRoot (Join-Path $runDir "acceptance_finance")
    $code = if ($null -ne $LASTEXITCODE) { [int]$LASTEXITCODE } else { 0 }
    if ($code -ne 0) { throw "acceptance_desktop_finance_template.ps1 exit code $code" }
  }
}

$allPassed = $true
foreach ($r in $results) {
  if ($r.status -eq "failed") { $allPassed = $false; break }
}

$summary = [ordered]@{
  version = $Version
  generated_at = (Get-Date).ToString("o")
  all_passed = $allPassed
  run_dir = $runDir
  results = $results
}

$jsonPath = Join-Path $runDir "release_gate_summary.json"
$mdPath = Join-Path $runDir "release_gate_summary.md"

($summary | ConvertTo-Json -Depth 8) | Set-Content -Path $jsonPath -Encoding UTF8

$lines = @()
$lines += "# Release Gate v$Version"
$lines += ""
$lines += "- generated_at: $($summary.generated_at)"
$lines += "- all_passed: $($summary.all_passed)"
$lines += "- run_dir: $runDir"
$lines += ""
$lines += "| Step | Status |"
$lines += "|---|---|"
foreach ($r in $results) {
  $lines += "| $($r.name) | $($r.status) |"
}
Set-Content -Path $mdPath -Value ($lines -join [Environment]::NewLine) -Encoding UTF8

Ok "release gate summary json: $jsonPath"
Ok "release gate summary md: $mdPath"
if (-not $allPassed) { exit 2 }
exit 0
