param(
  [string]$EnvFile = "",
  [int]$TimeoutSeconds = 180
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}
$validateScript = Join-Path $root "ops\scripts\validate_env.ps1"
if (Test-Path $validateScript) {
  powershell -ExecutionPolicy Bypass -File $validateScript -EnvFile $EnvFile
}

function Stop-PortListeners([int[]]$Ports) {
  foreach($p in $Ports){
    $conns = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue
    foreach($c in $conns){
      try {
        Stop-Process -Id $c.OwningProcess -Force -ErrorAction Stop
        Info "stopped pid=$($c.OwningProcess) on port=$p"
      } catch {
        Warn "failed stopping pid=$($c.OwningProcess) on port=$p"
      }
    }
  }
}

function Wait-Health([string]$Url, [string]$Kind, [datetime]$Deadline) {
  while((Get-Date) -lt $Deadline){
    try {
      $r = Invoke-RestMethod $Url -Method Get -TimeoutSec 5
      if ($Kind -eq "base" -and $r.status -eq "UP") { return $true }
      if ($Kind -eq "glue" -and $r.ok -eq $true) { return $true }
      if ($Kind -eq "accel" -and $r.ok -eq $true) { return $true }
    } catch {}
    Start-Sleep -Seconds 2
  }
  return $false
}

Stop-PortListeners @(18080,18081,18082)

Start-Process powershell -WindowStyle Hidden -ArgumentList "-ExecutionPolicy Bypass -File `"$root\ops\scripts\run_accel_rust.ps1`" -EnvFile `"$EnvFile`""
Start-Process powershell -WindowStyle Hidden -ArgumentList "-ExecutionPolicy Bypass -File `"$root\ops\scripts\run_glue_python.ps1`" -EnvFile `"$EnvFile`""
Start-Process powershell -WindowStyle Hidden -ArgumentList "-ExecutionPolicy Bypass -File `"$root\ops\scripts\run_base_java.ps1`" -EnvFile `"$EnvFile`""

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$okBase = Wait-Health "http://127.0.0.1:18080/actuator/health" "base" $deadline
$okGlue = Wait-Health "http://127.0.0.1:18081/health" "glue" $deadline
$okAccel = Wait-Health "http://127.0.0.1:18082/health" "accel" $deadline

if ($okBase -and $okGlue -and $okAccel) {
  Ok "all services are healthy"
  exit 0
}

Warn "base healthy=$okBase, glue healthy=$okGlue, accel healthy=$okAccel"
throw "restart_services failed health checks"
