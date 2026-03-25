param(
  [string]$Root = "",
  [string]$EnvFile = "",
  [string]$GlueUrl = "",
  [int]$TimeoutSeconds = 45,
  [switch]$StartIfMissing
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not $EnvFile) {
  $EnvFile = Join-Path $Root "ops\config\dev.env"
}

function Test-GlueHealth([string]$BaseUrl) {
  $target = ($BaseUrl.Trim().TrimEnd('/')) + "/health"
  try {
    $resp = Invoke-RestMethod -Uri $target -Method Get -TimeoutSec 4
    return ($resp.ok -eq $true)
  } catch {
    return $false
  }
}

if (-not $GlueUrl) {
  $GlueUrl = if ($env:AIWF_GLUE_URL) { $env:AIWF_GLUE_URL } else { "http://127.0.0.1:18081" }
}

if (Test-GlueHealth $GlueUrl) {
  Ok "local governance bridge is healthy: $GlueUrl"
  exit 0
}

if (-not $StartIfMissing) {
  throw "local governance bridge is not healthy: $GlueUrl"
}

$runGlueScript = Join-Path $PSScriptRoot "run_glue_python.ps1"
if (-not (Test-Path $runGlueScript)) {
  throw "run_glue_python script not found: $runGlueScript"
}

Info "starting local governance bridge via run_glue_python.ps1"
$args = @(
  "-ExecutionPolicy", "Bypass",
  "-File", "`"$runGlueScript`"",
  "-EnvFile", "`"$EnvFile`""
)
$proc = Start-Process -FilePath "powershell" -ArgumentList $args -WindowStyle Hidden -PassThru

$deadline = (Get-Date).AddSeconds([Math]::Max(5, $TimeoutSeconds))
while ((Get-Date) -lt $deadline) {
  if (Test-GlueHealth $GlueUrl) {
    Ok "local governance bridge started: $GlueUrl (pid=$($proc.Id))"
    exit 0
  }
  if ($proc.HasExited) {
    break
  }
  Start-Sleep -Milliseconds 800
}

if (-not $proc.HasExited) {
  Warn "local governance bridge did not become healthy within timeout; leaving spawned process running for inspection"
} else {
  Warn "local governance bridge process exited before health check passed (pid=$($proc.Id), exitCode=$($proc.ExitCode))"
}

throw "local governance bridge failed health check: $GlueUrl"
