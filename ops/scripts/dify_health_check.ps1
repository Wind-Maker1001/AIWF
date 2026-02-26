param(
  [string]$EnvFile = "",
  [string]$BaseUrl = "",
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $EnvFile) {
  $root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
  $EnvFile = Join-Path $root "ops\config\dev.env"
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
$headers = @{}
if ($env:AIWF_API_KEY -and $env:AIWF_API_KEY.Trim() -ne "") {
  $headers["X-API-Key"] = $env:AIWF_API_KEY
}

$targets = @(
  @{ name = "base_health"; url = "$BaseUrl/actuator/health" },
  @{ name = "dify_bridge_health"; url = "$BaseUrl/api/v1/integrations/dify/health" }
)

Write-Host ""
Write-Host "=== Dify Health Check ==="
Write-Host "base_url: $BaseUrl"
Write-Host "env_file: $EnvFile"
Write-Host "dry_run : $DryRun"

if ($DryRun) {
  foreach ($t in $targets) {
    Info ("would call {0}" -f $t.url)
  }
  Ok "dry-run completed"
  exit 0
}

foreach ($t in $targets) {
  try {
    Info ("calling {0}" -f $t.url)
    $resp = Invoke-RestMethod -Uri $t.url -Method Get -Headers $headers -TimeoutSec 15
    if ($null -eq $resp) { throw "empty response" }
    $summary = ($resp | ConvertTo-Json -Compress -Depth 6)
    Ok ("{0}: {1}" -f $t.name, $summary)
  }
  catch {
    throw ("{0} failed: {1}" -f $t.name, $_.Exception.Message)
  }
}

Ok "dify integration health check passed"
