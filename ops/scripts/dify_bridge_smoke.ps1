param(
  [string]$EnvFile = "",
  [string]$Owner = "dify",
  [string]$Actor = "dify"
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
    $idx = $line.IndexOf('=')
    if ($idx -le 0) { return }
    $k = $line.Substring(0, $idx).Trim()
    $v = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
    [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}

Import-DotEnv $EnvFile

$base = if ($env:AIWF_BASE_URL) { $env:AIWF_BASE_URL } else { "http://127.0.0.1:18080" }
$headers = @{}
if ($env:AIWF_API_KEY -and $env:AIWF_API_KEY.Trim() -ne "") {
  $headers["X-API-Key"] = $env:AIWF_API_KEY
}

$body = @{
  owner = $Owner
  actor = $Actor
  ruleset_version = "v1"
  params = @{
    office_lang = "zh"
    office_theme = "debate"
  }
} | ConvertTo-Json -Depth 6

Info "calling Dify bridge endpoint"
$resp = Invoke-RestMethod "$base/api/v1/integrations/dify/run_cleaning" -Method Post -Headers $headers -ContentType "application/json" -Body $body

Write-Host ""
Write-Host "=== Dify Bridge Smoke ==="
Write-Host "ok         : $($resp.ok)"
Write-Host "job_id     : $($resp.job_id)"
Write-Host "artifacts  : $($resp.artifacts.Count)"
Write-Host "run_ok     : $($resp.run.ok)"

if (-not $resp.ok) {
  throw "dify bridge smoke failed"
}

Ok "dify bridge smoke passed"
