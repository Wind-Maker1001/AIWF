param(
  [string]$RepoRoot = "",
  [string]$OutDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not $OutDir) {
  $OutDir = Join-Path $RepoRoot "ops\logs\cleaning_rollout\summary"
}
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

function Read-JsonFile([string]$Path) {
  if (-not (Test-Path $Path)) { return $null }
  try {
    return Get-Content -Raw -Encoding UTF8 $Path | ConvertFrom-Json
  } catch {
    return $null
  }
}

function Get-AcceptanceEvidencePaths([string]$Root) {
  return @(
    (Join-Path $Root "ops\logs\acceptance\desktop_real_sample\cleaning_shadow_rollout.json"),
    (Join-Path $Root "ops\logs\acceptance\desktop_finance_template\cleaning_shadow_rollout.json")
  )
}

function Get-AcceptancePassStreak([string]$AcceptanceRoot) {
  if (-not (Test-Path $AcceptanceRoot)) { return 0 }
  $dirs = Get-ChildItem -Path $AcceptanceRoot -Directory | Sort-Object Name -Descending
  $count = 0
  foreach ($dir in $dirs) {
    $path = Join-Path $dir.FullName "cleaning_shadow_rollout.json"
    $obj = Read-JsonFile $path
    if ($null -eq $obj) { break }
    $status = [string]($obj.shadow_compare.status)
    $requested = [string]($obj.requested_rust_v2_mode)
    $verify = [bool]$obj.verify_on_default
    $executionMode = [string]($obj.execution.execution_mode)
    if ($status -eq "matched" -and $requested -eq "default" -and $verify -and $executionMode -eq "rust_v2") {
      $count += 1
      continue
    }
    break
  }
  return $count
}

$evidenceItems = @()
foreach ($path in Get-AcceptanceEvidencePaths -Root $RepoRoot) {
  $obj = Read-JsonFile $path
  if ($null -eq $obj) { continue }
  $shadowStatus = [string]($obj.shadow_compare.status)
  $executionMode = [string]($obj.execution.execution_mode)
  $eligibility = [string]($obj.execution.execution_eligibility_reason)
  $evidenceItems += [pscustomobject]@{
    acceptance = [string]$obj.acceptance
    path = $path
    requested_rust_v2_mode = [string]$obj.requested_rust_v2_mode
    effective_rust_v2_mode = [string]$obj.effective_rust_v2_mode
    verify_on_default = [bool]$obj.verify_on_default
    execution_mode = $executionMode
    eligibility_reason = $eligibility
    shadow_compare_status = $shadowStatus
  }
}

$total = [double]([Math]::Max(1, $evidenceItems.Count))
$mismatchCount = @($evidenceItems | Where-Object { $_.shadow_compare_status -eq "mismatched" }).Count
$rustErrorCount = @($evidenceItems | Where-Object { $_.eligibility_reason -eq "rust_v2_error" -or $_.shadow_compare_status -eq "rust_error" }).Count
$fallbackCount = @($evidenceItems | Where-Object { $_.execution_mode -ne "rust_v2" }).Count
$rustUsedCount = @($evidenceItems | Where-Object { $_.execution_mode -eq "rust_v2" }).Count
$passStreak = [Math]::Min(
  (Get-AcceptancePassStreak (Join-Path $RepoRoot "ops\logs\acceptance\desktop_real_sample")),
  (Get-AcceptancePassStreak (Join-Path $RepoRoot "ops\logs\acceptance\desktop_finance_template"))
)

$summary = [ordered]@{
  generated_at = (Get-Date).ToString("o")
  total_acceptance_evidence = $evidenceItems.Count
  mismatch_rate = [Math]::Round($mismatchCount / $total, 4)
  rust_error_rate = [Math]::Round($rustErrorCount / $total, 4)
  fallback_rate = [Math]::Round($fallbackCount / $total, 4)
  rust_v2_used_rate = [Math]::Round($rustUsedCount / $total, 4)
  acceptance_pass_streak = [int]$passStreak
  items = $evidenceItems
}

$jsonPath = Join-Path $OutDir "cleaning_rust_v2_rollout_summary_latest.json"
$mdPath = Join-Path $OutDir "cleaning_rust_v2_rollout_summary_latest.md"
($summary | ConvertTo-Json -Depth 8) | Set-Content -Path $jsonPath -Encoding UTF8

$lines = @()
$lines += "# Cleaning Rust v2 Rollout Summary"
$lines += ""
$lines += "- generated_at: $($summary.generated_at)"
$lines += "- total_acceptance_evidence: $($summary.total_acceptance_evidence)"
$lines += "- mismatch_rate: $($summary.mismatch_rate)"
$lines += "- rust_error_rate: $($summary.rust_error_rate)"
$lines += "- fallback_rate: $($summary.fallback_rate)"
$lines += "- rust_v2_used_rate: $($summary.rust_v2_used_rate)"
$lines += "- acceptance_pass_streak: $($summary.acceptance_pass_streak)"
$lines += ""
$lines += "| acceptance | mode | verify | execution | shadow_compare |"
$lines += "|---|---|---|---|---|"
foreach ($item in $evidenceItems) {
  $lines += "| $($item.acceptance) | $($item.requested_rust_v2_mode) | $($item.verify_on_default) | $($item.execution_mode) | $($item.shadow_compare_status) |"
}
Set-Content -Path $mdPath -Value ($lines -join [Environment]::NewLine) -Encoding UTF8

Ok "cleaning rust v2 rollout summary written"
Write-Host $jsonPath
