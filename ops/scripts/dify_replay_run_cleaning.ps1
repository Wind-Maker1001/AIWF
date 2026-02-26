param(
  [string]$EnvFile = "",
  [string]$BaseUrl = "",
  [string]$PayloadFile = "",
  [string]$OutputFile = "",
  [int]$TimeoutSec = 120,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}
if (-not $PayloadFile) {
  $PayloadFile = Join-Path $root "ops\config\dify_run_cleaning.payload.example.json"
}

function Import-DotEnv([string]$Path) {
  if (-not (Test-Path $Path)) { return }
  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    $idx = $line.IndexOf("=")
    if ($idx -le 0) { return }
    $k = $line.Substring(0, $idx).Trim()
    $v = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
    [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}

Import-DotEnv $EnvFile

if (-not $BaseUrl) {
  $BaseUrl = if ($env:AIWF_BASE_URL) { $env:AIWF_BASE_URL } else { "http://127.0.0.1:18080" }
}
$BaseUrl = $BaseUrl.TrimEnd("/")

if (-not (Test-Path $PayloadFile)) {
  throw "payload file not found: $PayloadFile"
}

$payloadRaw = Get-Content $PayloadFile -Raw -Encoding UTF8
$payloadObj = $payloadRaw | ConvertFrom-Json
$payloadBody = $payloadObj | ConvertTo-Json -Depth 16

$headers = @{ "Content-Type" = "application/json" }
if ($env:AIWF_API_KEY -and $env:AIWF_API_KEY.Trim() -ne "") {
  $headers["X-API-Key"] = $env:AIWF_API_KEY
}

if (-not $OutputFile) {
  $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
  $OutputFile = Join-Path $root ("tmp\dify_replay_{0}.json" -f $stamp)
}
$outputDir = Split-Path -Parent $OutputFile
if (-not (Test-Path $outputDir)) {
  New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}

$url = "$BaseUrl/api/v1/integrations/dify/run_cleaning"

Write-Host ""
Write-Host "=== Dify Replay Run Cleaning ==="
Write-Host "url        : $url"
Write-Host "payload    : $PayloadFile"
Write-Host "output     : $OutputFile"
Write-Host "env_file   : $EnvFile"
Write-Host "dry_run    : $DryRun"

if ($DryRun) {
  Info "would post payload to run_cleaning endpoint"
  Ok "dry-run completed"
  exit 0
}

$resp = Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $payloadBody -TimeoutSec $TimeoutSec

if ($null -eq $resp) {
  throw "empty response from run_cleaning endpoint"
}

$respJson = $resp | ConvertTo-Json -Depth 20
Set-Content -Path $OutputFile -Value $respJson -Encoding UTF8

$artifactCount = 0
if ($resp.PSObject.Properties.Name -contains "artifacts" -and $resp.artifacts) {
  $artifactCount = @($resp.artifacts).Count
}

Write-Host ""
Write-Host "ok         : $($resp.ok)"
Write-Host "job_id     : $($resp.job_id)"
Write-Host "artifacts  : $artifactCount"
Write-Host "response   : $OutputFile"

if (-not $resp.ok) {
  throw "run_cleaning returned ok=false"
}

Ok "dify replay passed"
