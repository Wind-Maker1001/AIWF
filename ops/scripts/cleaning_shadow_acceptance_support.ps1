Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-CleaningShadowModeSettings {
  return [ordered]@{
    mode = "default"
    verify_on_default = $true
  }
}

function Get-AiwfGlueUrl {
  $value = [string]$env:AIWF_GLUE_URL
  if ([string]::IsNullOrWhiteSpace($value)) {
    return "http://127.0.0.1:18081"
  }
  return $value.TrimEnd("/")
}

function Get-AiwfAccelUrl {
  $value = [string]$env:AIWF_ACCEL_URL
  if ([string]::IsNullOrWhiteSpace($value)) {
    return "http://127.0.0.1:18082"
  }
  return $value.TrimEnd("/")
}

function Get-AiwfBaseUrl {
  $value = [string]$env:AIWF_BASE_URL
  if ([string]::IsNullOrWhiteSpace($value)) {
    return "http://127.0.0.1:18080"
  }
  return $value.TrimEnd("/")
}

function Invoke-ShadowCleaningMode([scriptblock]$Action) {
  $settings = Get-CleaningShadowModeSettings
  $prevMode = $env:AIWF_CLEANING_RUST_V2_MODE
  $prevVerify = $env:AIWF_CLEANING_RUST_V2_VERIFY_ON_DEFAULT
  $env:AIWF_CLEANING_RUST_V2_MODE = [string]$settings.mode
  $env:AIWF_CLEANING_RUST_V2_VERIFY_ON_DEFAULT = if ([bool]$settings.verify_on_default) { "true" } else { "false" }
  try {
    & $Action
  }
  finally {
    if ($null -eq $prevMode) {
      Remove-Item Env:AIWF_CLEANING_RUST_V2_MODE -ErrorAction SilentlyContinue
    } else {
      $env:AIWF_CLEANING_RUST_V2_MODE = $prevMode
    }
    if ($null -eq $prevVerify) {
      Remove-Item Env:AIWF_CLEANING_RUST_V2_VERIFY_ON_DEFAULT -ErrorAction SilentlyContinue
    } else {
      $env:AIWF_CLEANING_RUST_V2_VERIFY_ON_DEFAULT = $prevVerify
    }
  }
}

function Assert-JsonHealthOk([string]$BaseUrl, [string]$Label) {
  $url = ("{0}/health" -f $BaseUrl.TrimEnd("/"))
  try {
    $resp = Invoke-RestMethod -Uri $url -Method Get -TimeoutSec 10
  } catch {
    throw ("{0} health check failed: {1}" -f $Label, [string]$_.Exception.Message)
  }
  if (-not $resp -or -not $resp.ok) {
    $payload = if ($null -eq $resp) { "<empty>" } else { ($resp | ConvertTo-Json -Depth 8 -Compress) }
    throw ("{0} health check not ok: {1}" -f $Label, $payload)
  }
  return $resp
}

function Assert-CleaningShadowDependencies {
  return [ordered]@{
    glue = Assert-JsonHealthOk -BaseUrl (Get-AiwfGlueUrl) -Label "glue-python"
    accel = Assert-JsonHealthOk -BaseUrl (Get-AiwfAccelUrl) -Label "accel-rust"
  }
}

function New-AcceptanceJobContext(
  [string]$RunDir = "",
  [string]$RepoRoot = "",
  [string]$AcceptanceName = "",
  [string]$JobId = ""
) {
  if (-not [string]::IsNullOrWhiteSpace($RunDir)) {
    $jobRoot = Join-Path $RunDir "job_runtime"
  } else {
    $safeAcceptanceName = if ([string]::IsNullOrWhiteSpace($AcceptanceName)) { "acceptance" } else { $AcceptanceName.Trim() }
    $jobRoot = Join-Path $RepoRoot ("bus\jobs\shadow_acceptance\{0}\{1}" -f $safeAcceptanceName, $JobId)
  }
  $stageDir = Join-Path $jobRoot "stage"
  $artifactsDir = Join-Path $jobRoot "artifacts"
  $evidenceDir = Join-Path $jobRoot "evidence"
  foreach ($path in @($jobRoot, $stageDir, $artifactsDir, $evidenceDir)) {
    New-Item -ItemType Directory -Path $path -Force | Out-Null
  }
  return [ordered]@{
    job_root = $jobRoot
    stage_dir = $stageDir
    artifacts_dir = $artifactsDir
    evidence_dir = $evidenceDir
  }
}

function Invoke-GlueRunCleaningAcceptance(
  [string]$GlueUrl,
  [string]$JobId,
  [string]$Actor,
  [string]$RulesetVersion,
  [hashtable]$Params,
  [hashtable]$JobContext,
  [int]$TimeoutSec = 300
) {
  $body = [ordered]@{
    actor = $Actor
    ruleset_version = $RulesetVersion
    params = $Params
    job_context = $JobContext
  }
  $url = ("{0}/jobs/{1}/run/cleaning" -f $GlueUrl.TrimEnd("/"), $JobId)
  return Invoke-RestMethod -Uri $url -Method Post -ContentType "application/json" -Body ($body | ConvertTo-Json -Depth 16) -TimeoutSec $TimeoutSec
}

function Assert-ShadowCompareMatched($Result, [string]$Label = "acceptance") {
  if ($null -eq $Result) {
    throw "$Label returned empty cleaning result"
  }
  if (-not [bool]$Result.ok) {
    throw "$Label returned ok=false"
  }
  $execution = if ($Result.PSObject.Properties.Name -contains "execution") { $Result.execution } else { $null }
  if ($null -eq $execution) {
    throw "$Label missing execution report"
  }
  if ([string]$execution.requested_rust_v2_mode -ne "default") {
    throw "$Label requested_rust_v2_mode != default"
  }
  if ([string]$execution.effective_rust_v2_mode -ne "default") {
    throw "$Label effective_rust_v2_mode != default"
  }
  if (-not [bool]$execution.verify_on_default) {
    throw "$Label verify_on_default must be true"
  }
  $shadowCompare = if ($execution.PSObject.Properties.Name -contains "shadow_compare") { $execution.shadow_compare } else { $null }
  if ($null -eq $shadowCompare) {
    throw "$Label missing shadow_compare"
  }
  if ([string]$shadowCompare.status -ne "matched") {
    throw ("{0} shadow_compare.status != matched ({1})" -f $Label, [string]$shadowCompare.status)
  }
}
