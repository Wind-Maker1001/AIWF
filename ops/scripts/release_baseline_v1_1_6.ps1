param(
  [string]$Version = "1.1.6",
  [switch]$SkipGate,
  [switch]$SkipAcceptance,
  [switch]$CopyArtifactsToDesktop
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
. (Join-Path $PSScriptRoot "cleaning_shadow_acceptance_support.ps1")

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$releaseDir = Join-Path $root ("release\v{0}" -f $Version)
New-Item -ItemType Directory -Path $releaseDir -Force | Out-Null

$gateScript = Join-Path $PSScriptRoot "release_gate_v1_1_6.ps1"
$acceptReal = Join-Path $PSScriptRoot "acceptance_desktop_real_sample.ps1"
$acceptFinance = Join-Path $PSScriptRoot "acceptance_desktop_finance_template.ps1"

$summary = [ordered]@{
  version = $Version
  generated_at = (Get-Date).ToString("o")
  gate = "skipped"
  acceptance_real = "skipped"
  acceptance_finance = "skipped"
  notes = @()
}

if (-not $SkipGate) {
  Info "running release gate v$Version"
  powershell -ExecutionPolicy Bypass -File $gateScript -Version $Version
  if ($LASTEXITCODE -ne 0) { throw "release gate failed" }
  $summary.gate = "passed"
  Ok "release gate passed"
}

if (-not $SkipAcceptance) {
  Info "running desktop real-sample acceptance"
  $argsReal = @(
    "-ExecutionPolicy", "Bypass",
    "-File", $acceptReal,
    "-OutputRoot", (Join-Path $releaseDir "acceptance_real")
  )
  if ($CopyArtifactsToDesktop) { $argsReal += "-CopyArtifactsToDesktop" }
  Invoke-ShadowCleaningMode { powershell @argsReal }
  if ($LASTEXITCODE -ne 0) { throw "acceptance_desktop_real_sample failed" }
  $summary.acceptance_real = "passed"

  Info "running desktop finance-template acceptance"
  $argsFin = @(
    "-ExecutionPolicy", "Bypass",
    "-File", $acceptFinance,
    "-OutputRoot", (Join-Path $releaseDir "acceptance_finance")
  )
  if ($CopyArtifactsToDesktop) { $argsFin += "-CopyArtifactsToDesktop" }
  Invoke-ShadowCleaningMode { powershell @argsFin }
  if ($LASTEXITCODE -ne 0) { throw "acceptance_desktop_finance_template failed" }
  $summary.acceptance_finance = "passed"
  Ok "acceptance baseline passed"
}

$checklistPath = Join-Path $releaseDir "clean_windows_checklist.md"
$lines = @()
$lines += "# AIWF v$Version Clean Windows Acceptance Checklist"
$lines += ""
$lines += "- generated_at: $((Get-Date).ToString('o'))"
$lines += "- version: v$Version"
$lines += "- goal: app starts after install, fallback works, deliverables usable"
$lines += ""
$lines += "## Install"
$lines += ("1. Run ""AIWF Dify Desktop Setup {0}.exe""" -f $Version)
$lines += "2. Choose custom install path and finish setup"
$lines += "3. Double-click desktop shortcut and verify main window opens"
$lines += '4. Verify option `Auto fallback to offline when backend fails` is enabled'
$lines += ""
$lines += "## GUI Flow"
$lines += '1. Switch to backend mode and set Base URL to `http://127.0.0.1:19999`'
$lines += "2. Drag mixed files (PDF/docx/txt/xlsx/image) and run"
$lines += "3. Verify status shows auto switch to offline mode"
$lines += '4. Verify result mode is `offline_fallback`'
$lines += '5. Verify `xlsx/docx/pptx/md` artifacts open correctly'
$lines += ""
$lines += "## Audit"
$lines += '1. Open `%APPDATA%\\AIWF Dify Desktop\\logs\\run_mode_audit.jsonl`'
$lines += '2. Verify latest line has `fallback_applied=true`, `reason`, and `job_id`'
$lines += ""
$lines += "## Automation Results"
$lines += "- gate: $($summary.gate)"
$lines += "- acceptance_real: $($summary.acceptance_real)"
$lines += "- acceptance_finance: $($summary.acceptance_finance)"
$lines += ""
Set-Content -Path $checklistPath -Value ($lines -join [Environment]::NewLine) -Encoding UTF8

$summaryPath = Join-Path $releaseDir "baseline_summary.json"
($summary | ConvertTo-Json -Depth 5) | Set-Content -Path $summaryPath -Encoding UTF8

Ok "baseline summary: $summaryPath"
Ok "checklist: $checklistPath"
exit 0
