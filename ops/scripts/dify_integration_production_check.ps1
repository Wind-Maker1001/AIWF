param(
  [string]$EnvFile = "",
  [string]$BaseUrl = "",
  [string]$PayloadFile = "",
  [int]$TimeoutSec = 180,
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 5,
  [string]$AlertLog = "",
  [string]$AlertWebhook = "",
  [bool]$EnableOfflineFallback = $true,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $AlertLog) {
  $AlertLog = Join-Path $root "tmp\dify_integration_alert.log"
}
$baselineScript = Join-Path $PSScriptRoot "dify_integration_baseline.ps1"
$fallbackScript = Join-Path $PSScriptRoot "dify_run_with_offline_fallback.ps1"
if (-not (Test-Path $baselineScript)) {
  throw "missing script: $baselineScript"
}
if (-not (Test-Path $fallbackScript)) {
  throw "missing script: $fallbackScript"
}

function Write-Alert([string]$msg) {
  $dir = Split-Path -Parent $AlertLog
  if (-not (Test-Path $dir)) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
  }
  $line = "{0} {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
  Add-Content -Path $AlertLog -Value $line -Encoding UTF8
}

function Send-Webhook([string]$msg) {
  if (-not $AlertWebhook) { return }
  try {
    $payload = @{ text = $msg } | ConvertTo-Json -Depth 4
    Invoke-RestMethod -Uri $AlertWebhook -Method Post -ContentType "application/json" -Body $payload -TimeoutSec 15 | Out-Null
  }
  catch {
    Warn ("alert webhook failed: {0}" -f $_.Exception.Message)
  }
}

Write-Host ""
Write-Host "=== Dify Integration Production Check ==="
Write-Host "timeout_sec : $TimeoutSec"
Write-Host "max_retries : $MaxRetries"
Write-Host "retry_delay : $RetryDelaySec"
Write-Host "alert_log   : $AlertLog"
Write-Host "fallback    : $EnableOfflineFallback"
Write-Host "dry_run     : $DryRun"

if ($DryRun) {
  Info "would run integration check with retry/alert policy (and optional offline fallback)"
  Ok "dry-run completed"
  exit 0
}

if ($MaxRetries -lt 1) { $MaxRetries = 1 }

for ($i = 1; $i -le $MaxRetries; $i++) {
  try {
    Info ("attempt {0}/{1}" -f $i, $MaxRetries)
    if ($EnableOfflineFallback) {
      & $fallbackScript -EnvFile $EnvFile -BaseUrl $BaseUrl -PayloadFile $PayloadFile -TimeoutSec $TimeoutSec
    } else {
      & $baselineScript -EnvFile $EnvFile -BaseUrl $BaseUrl -PayloadFile $PayloadFile -TimeoutSec $TimeoutSec
    }
    Ok ("production check passed at attempt {0}" -f $i)
    exit 0
  }
  catch {
    $err = $_.Exception.Message
    Warn ("attempt {0} failed: {1}" -f $i, $err)
    if ($i -ge $MaxRetries) {
      $msg = "dify production check failed after $MaxRetries attempts: $err"
      Write-Alert $msg
      Send-Webhook $msg
      throw $msg
    }
    Start-Sleep -Seconds $RetryDelaySec
  }
}
