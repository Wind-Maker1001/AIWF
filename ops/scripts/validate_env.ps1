param(
  [string]$EnvFile = "",
  [switch]$SkipSql,
  [switch]$SkipApiKey
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}

function Import-DotEnv([string]$Path) {
  if (-not (Test-Path $Path)) { return }
  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    $idx = $line.IndexOf('=')
    if ($idx -le 0) { return }
    $k = $line.Substring(0, $idx).Trim()
    $v = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
    [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}

Import-DotEnv $EnvFile

$required = @("AIWF_ROOT", "AIWF_BUS", "AIWF_LAKE", "AIWF_BASE_URL")
if (-not $SkipSql) {
  $required += @("AIWF_SQL_HOST", "AIWF_SQL_PORT", "AIWF_SQL_DB", "AIWF_SQL_USER", "AIWF_SQL_PASSWORD")
}
if (-not $SkipApiKey) {
  # API key can be optional by design; validate only when explicitly set to enforce non-empty.
  if ((Test-Path Env:AIWF_API_KEY) -and [string]::IsNullOrWhiteSpace($env:AIWF_API_KEY)) {
    throw "AIWF_API_KEY exists but empty"
  }
}

$missing = @()
foreach($k in $required){
  $v = [System.Environment]::GetEnvironmentVariable($k, "Process")
  if ([string]::IsNullOrWhiteSpace($v)) { $missing += $k }
}

if ($missing.Count -gt 0) {
  throw ("missing required env vars: " + ($missing -join ", "))
}

Info "validated env file: $EnvFile"
Ok "env validation passed"
