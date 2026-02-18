param(
  [string]$EnvFile = "",
  [string]$Owner = "desktop_e2e"
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

$restartScript = Join-Path $root "ops\scripts\restart_services.ps1"
$consoleScript = Join-Path $root "ops\scripts\run_dify_console.ps1"
$logDir = Join-Path $root "ops\logs"
$consoleOut = Join-Path $logDir "e2e_dify_console.out.log"
$consoleErr = Join-Path $logDir "e2e_dify_console.err.log"
$port = if ($env:AIWF_DIFY_CONSOLE_PORT) { $env:AIWF_DIFY_CONSOLE_PORT } else { "18083" }
$consoleUrl = "http://127.0.0.1:$port"
$consoleProc = $null

function Wait-HttpOk([string]$Url, [int]$TimeoutSec = 40) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while((Get-Date) -lt $deadline){
    try {
      $null = Invoke-RestMethod -Uri $Url -Method Get -TimeoutSec 3
      return $true
    } catch {}
    Start-Sleep -Milliseconds 600
  }
  return $false
}

function Stop-Port([int]$p) {
  $conn = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue
  if ($conn) {
    try { Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue } catch {}
  }
}

try {
  Info "restarting core services"
  powershell -ExecutionPolicy Bypass -File $restartScript -EnvFile $EnvFile
  if ($LASTEXITCODE -ne 0) { throw "restart_services failed" }

  Stop-Port([int]$port)
  Info "starting dify-console for e2e"
  New-Item -ItemType Directory -Force -Path $logDir | Out-Null
  Remove-Item $consoleOut,$consoleErr -Force -ErrorAction SilentlyContinue
  $consoleProc = Start-Process powershell -ArgumentList "-ExecutionPolicy Bypass -File `"$consoleScript`" -EnvFile `"$EnvFile`"" -PassThru -WindowStyle Hidden -RedirectStandardOutput $consoleOut -RedirectStandardError $consoleErr

  Start-Sleep -Seconds 1
  if ($consoleProc.HasExited) {
    $detail = ""
    if (Test-Path $consoleErr) { $detail = Get-Content $consoleErr -Raw }
    throw ("dify-console process exited early. " + $detail)
  }

  if (-not (Wait-HttpOk "$consoleUrl/health" 40)) {
    $out = if (Test-Path $consoleOut) { Get-Content $consoleOut -Raw } else { "" }
    $err = if (Test-Path $consoleErr) { Get-Content $consoleErr -Raw } else { "" }
    throw ("dify-console /health timeout.`nSTDOUT:`n" + $out + "`nSTDERR:`n" + $err)
  }

  $body = @{
    owner = $Owner
    actor = "e2e"
    ruleset_version = "v1"
    params = @{ office_lang = "zh"; office_theme = "debate" }
  } | ConvertTo-Json -Depth 6

  Info "calling /api/run_cleaning"
  $resp = Invoke-RestMethod "$consoleUrl/api/run_cleaning" -Method Post -ContentType "application/json" -Body $body
  if (-not $resp.ok) { throw "run_cleaning failed" }

  $xlsx = $resp.artifacts | Where-Object { $_.kind -eq "xlsx" } | Select-Object -First 1
  $docx = $resp.artifacts | Where-Object { $_.kind -eq "docx" } | Select-Object -First 1
  $pptx = $resp.artifacts | Where-Object { $_.kind -eq "pptx" } | Select-Object -First 1

  if (-not $xlsx -or -not $docx -or -not $pptx) {
    throw "missing office artifacts in e2e response"
  }

  foreach($a in @($xlsx, $docx, $pptx)){
    if (-not (Test-Path $a.path)) {
      throw "artifact path not found: $($a.path)"
    }
    $size = (Get-Item $a.path).Length
    if ($size -le 0) {
      throw "artifact is empty: $($a.path)"
    }
  }

  Write-Host ""
  Write-Host "=== Desktop E2E ==="
  Write-Host "job_id : $($resp.job_id)"
  Write-Host "xlsx   : $($xlsx.path)"
  Write-Host "docx   : $($docx.path)"
  Write-Host "pptx   : $($pptx.path)"
  Ok "desktop-style end-to-end flow passed"
}
finally {
  if ($consoleProc -and -not $consoleProc.HasExited) {
    try { Stop-Process -Id $consoleProc.Id -Force -ErrorAction SilentlyContinue } catch {}
  }
  Stop-Port([int]$port)
}
