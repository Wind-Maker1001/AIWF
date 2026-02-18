param(
  [string]$EnvFile = "",
  [string]$Owner = "local",
  [string]$AccelProjectDir = "",
  [int]$TempAccelPort = 18092,
  [bool]$StartTempAccel = $true,
  [string]$GlueProjectDir = "",
  [int]$TempGluePort = 18091,
  [bool]$StartTempGlue = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Fail($m){ throw "[FAIL] $m" }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}
if (-not $AccelProjectDir) {
  $AccelProjectDir = Join-Path $root "apps\accel-rust"
}
if (-not $GlueProjectDir) {
  $GlueProjectDir = Join-Path $root "apps\glue-python"
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
$glue = if ($env:AIWF_GLUE_URL) { $env:AIWF_GLUE_URL } else { "http://127.0.0.1:18081" }
$tempAccelUrl = "http://127.0.0.1:$TempAccelPort"
$tempGlueUrl = "http://127.0.0.1:$TempGluePort"
$tempAccelProc = $null
$tempGlueProc = $null

Info "base=$base"
Info "glue=$glue"

$null = Invoke-RestMethod "$base/actuator/health" -Method Get
Ok "health checks passed"

try {
  if ($StartTempAccel) {
    if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
      Fail "cargo not found in PATH"
    }
    if (-not (Test-Path $AccelProjectDir)) {
      Fail "accel-rust project not found: $AccelProjectDir"
    }

    Info "starting temporary accel-rust on $tempAccelUrl"
    $env:AIWF_ACCEL_RUST_HOST = "127.0.0.1"
    $env:AIWF_ACCEL_RUST_PORT = "$TempAccelPort"
    $tempTargetDir = Join-Path $env:TEMP "aiwf-accel-temp-target"
    Info "building accel-rust"
    & cargo build --quiet --target-dir $tempTargetDir --manifest-path (Join-Path $AccelProjectDir "Cargo.toml")
    if ($LASTEXITCODE -ne 0) {
      Fail "cargo build failed for accel-rust"
    }

    $accelExe = Join-Path $tempTargetDir "debug\accel-rust.exe"
    if (-not (Test-Path $accelExe)) {
      Fail "accel executable not found: $accelExe"
    }
    $tempAccelProc = Start-Process -FilePath $accelExe -WorkingDirectory $AccelProjectDir -PassThru

    $ready = $false
    for ($i = 0; $i -lt 120; $i++) {
      Start-Sleep -Seconds 1
      try {
        $h = Invoke-RestMethod "$tempAccelUrl/health" -Method Get -TimeoutSec 2
        if ($h.ok) {
          $ready = $true
          break
        }
      } catch {
      }
      if ($tempAccelProc.HasExited) {
        break
      }
    }
    if (-not $ready) {
      Fail "temporary accel-rust failed to become healthy at $tempAccelUrl"
    }
    Ok "temporary accel-rust is healthy"
  }

  $runGlueBase = $glue
  if ($StartTempGlue) {
    if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
      Fail "python not found in PATH"
    }
    if (-not (Test-Path $GlueProjectDir)) {
      Fail "glue-python project not found: $GlueProjectDir"
    }

    Info "starting temporary glue-python on $tempGlueUrl"
    $env:AIWF_BASE_URL = $base
    $env:AIWF_GLUE_PY_PORT = "$TempGluePort"
    $tempGlueProc = Start-Process -FilePath "python" -ArgumentList "-m uvicorn app:app --host 127.0.0.1 --port $TempGluePort" -WorkingDirectory $GlueProjectDir -PassThru

    $glueReady = $false
    for ($i = 0; $i -lt 60; $i++) {
      Start-Sleep -Milliseconds 500
      try {
        $gh = Invoke-RestMethod "$tempGlueUrl/health" -Method Get -TimeoutSec 2
        if ($gh.ok) {
          $glueReady = $true
          break
        }
      } catch {
      }
    }
    if (-not $glueReady) {
      Fail "temporary glue-python failed to become healthy at $tempGlueUrl"
    }
    Ok "temporary glue-python is healthy"
    $runGlueBase = $tempGlueUrl
  }

  $job = Invoke-RestMethod "$base/api/v1/tools/create_job?owner=$Owner" -Method Post -ContentType "application/json" -Body "{}"
  $jobId = $job.job_id
  Info "created job_id=$jobId"

  $params = @{
    accel_force_bad_parquet = $true
  }
  if ($StartTempAccel) {
    $params["accel_url"] = $tempAccelUrl
  }

  $runBody = @{
    actor = "local"
    ruleset_version = "v1"
    params = $params
  } | ConvertTo-Json -Depth 10

  $run = Invoke-RestMethod "$runGlueBase/jobs/$jobId/run/cleaning" -Method Post -ContentType "application/json" -Body $runBody

if (-not $run.ok) { Fail "run.ok is false" }
if (-not $run.accel.attempted) { Fail "run.accel.attempted is false" }
if (-not $run.accel.used_fallback) { Fail "expected run.accel.used_fallback=true" }
if (-not $run.accel.validation_error) { Fail "expected run.accel.validation_error to be set" }

  $parquetArtifact = $run.artifacts | Where-Object { $_.kind -eq "parquet" } | Select-Object -First 1
  if (-not $parquetArtifact) { Fail "parquet artifact not found" }

  $accelParquetPath = $null
  if ($run.accel.detail -and $run.accel.detail.response -and $run.accel.detail.response.outputs -and $run.accel.detail.response.outputs.cleaned_parquet) {
    $accelParquetPath = $run.accel.detail.response.outputs.cleaned_parquet.path
  }

  Write-Host ""
  Write-Host "=== Invalid Parquet Fallback Result ==="
  Write-Host "job_id              : $jobId"
  Write-Host "run_ok              : $($run.ok)"
  Write-Host "accel_attempted     : $($run.accel.attempted)"
  Write-Host "accel_used_fallback : $($run.accel.used_fallback)"
  Write-Host "validation_error    : $($run.accel.validation_error)"
  Write-Host "result_parquet      : $($parquetArtifact.path)"
  Write-Host "accel_parquet       : $accelParquetPath"
  Write-Host "fallback_assertion  : run.accel.used_fallback == True"

  Ok "invalid parquet fallback test passed"
}
finally {
  if ($tempGlueProc -and -not $tempGlueProc.HasExited) {
    Info "stopping temporary glue-python pid=$($tempGlueProc.Id)"
    Stop-Process -Id $tempGlueProc.Id -Force -ErrorAction SilentlyContinue
  }
  if ($tempAccelProc -and -not $tempAccelProc.HasExited) {
    Info "stopping temporary accel-rust pid=$($tempAccelProc.Id)"
    Stop-Process -Id $tempAccelProc.Id -Force -ErrorAction SilentlyContinue
  }
}
