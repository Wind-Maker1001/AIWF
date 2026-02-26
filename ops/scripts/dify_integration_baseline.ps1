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

$healthScript = Join-Path $PSScriptRoot "dify_health_check.ps1"
$replayScript = Join-Path $PSScriptRoot "dify_replay_run_cleaning.ps1"

if (-not (Test-Path $healthScript)) { throw "missing script: $healthScript" }
if (-not (Test-Path $replayScript)) { throw "missing script: $replayScript" }

Write-Host ""
Write-Host "=== Dify Integration Baseline ==="
Write-Host "env_file : $EnvFile"
Write-Host "base_url : $BaseUrl"
Write-Host "payload  : $PayloadFile"
Write-Host "output   : $OutputFile"
Write-Host "dry_run  : $DryRun"

Info "step 1/2: health check"
& $healthScript -EnvFile $EnvFile -BaseUrl $BaseUrl -DryRun:$DryRun

Info "step 2/2: replay run_cleaning"
& $replayScript -EnvFile $EnvFile -BaseUrl $BaseUrl -PayloadFile $PayloadFile -OutputFile $OutputFile -TimeoutSec $TimeoutSec -DryRun:$DryRun

Ok "dify integration baseline passed"
