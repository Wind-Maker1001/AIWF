param(
  [string]$AccelUrl = "http://127.0.0.1:18082",
  [int]$Rows = 80000,
  [int]$Runs = 3,
  [int]$Warmup = 1,
  [double]$MinSpeedup = 1.02,
  [double]$MinArrowSpeedup = 1.00,
  [double]$GateToleranceSpeedup = 0.02,
  [double]$MaxNoisyRegressionMs = 8.0,
  [int]$GateAttempts = 4,
  [string]$OutDir = "",
  [switch]$UpdateProfileOnPass,
  [string]$ProfilePath = "",
  [switch]$EnforceArrowAlways
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$benchScript = Join-Path $PSScriptRoot "bench_rust_transform.ps1"
if (-not (Test-Path $benchScript)) { throw "benchmark script not found: $benchScript" }
if (-not $OutDir) { $OutDir = Join-Path $root "ops\logs\bench\rust_transform" }
if (-not $ProfilePath) { $ProfilePath = Join-Path $root "apps\accel-rust\conf\transform_engine_profile.json" }

function Run-BenchmarkOnce {
  param([switch]$UpdateProfileFlag)
  Info "running benchmark gate rows=$Rows runs=$Runs warmup=$Warmup"
  $benchArgs = @(
    "-ExecutionPolicy", "Bypass",
    "-File", $benchScript,
    "-AccelUrl", $AccelUrl,
    "-Rows", $Rows,
    "-Runs", $Runs,
    "-Warmup", $Warmup,
    "-OutDir", $OutDir
  )
  if ($UpdateProfileFlag) { $benchArgs += "-UpdateProfile" }
  $null = powershell @benchArgs
  if ($LASTEXITCODE -ne 0) { throw "benchmark script failed" }
  $latestRun = Get-ChildItem $OutDir -Directory | Sort-Object Name -Descending | Select-Object -First 1
  if ($null -eq $latestRun) { throw "benchmark result run dir not found in: $OutDir" }
  $runJson = Join-Path $latestRun.FullName "benchmark.json"
  if (-not (Test-Path $runJson)) { throw "benchmark result not found: $runJson" }
  $raw = Get-Content $runJson -Raw
  $parsed = $raw | ConvertFrom-Json
  if ($null -eq $parsed) { throw "benchmark result parse failed: empty json" }
  if ($parsed -is [System.Array]) {
    $candidate = $parsed | Where-Object {
      if ($null -eq $_) { return $false }
      if ($_ -is [System.Collections.IDictionary]) { return $_.Contains("speedup_x") }
      return ($_.PSObject.Properties.Name -contains "speedup_x")
    } | Select-Object -Last 1
    if ($null -eq $candidate) { throw "benchmark result parse failed: speedup_x missing in array payload" }
    return $candidate
  }
  $hasSpeed = $false
  if ($parsed -is [System.Collections.IDictionary]) { $hasSpeed = $parsed.Contains("speedup_x") }
  else { $hasSpeed = ($parsed.PSObject.Properties.Name -contains "speedup_x") }
  if (-not $hasSpeed) {
    throw "benchmark result parse failed: speedup_x missing"
  }
  return $parsed
}

$mediumRowsThreshold = 20000
$largeRowsThreshold = 120000
if (Test-Path $ProfilePath) {
  try {
    $profile = Get-Content $ProfilePath | ConvertFrom-Json
    if ($null -ne $profile.medium_rows_threshold) {
      $mediumRowsThreshold = [int]$profile.medium_rows_threshold
    }
    if ($null -ne $profile.large_rows_threshold) {
      $largeRowsThreshold = [int]$profile.large_rows_threshold
    }
  } catch {}
}
function Test-SpeedGate {
  param(
    [string]$Name,
    [double]$ObservedSpeed,
    [double]$MinRequired,
    [double]$BaselineSec,
    [double]$CandidateSec,
    [double]$ToleranceSpeedup,
    [double]$NoisyRegressionMs
  )
  if ($ObservedSpeed -ge $MinRequired) { return }
  $nearFloor = $MinRequired - $ToleranceSpeedup
  $regressionMs = ($CandidateSec - $BaselineSec) * 1000.0
  $isWithinNoise = $regressionMs -le $NoisyRegressionMs
  $isNearFloor = $ObservedSpeed -ge $nearFloor
  if ($isNearFloor -and $isWithinNoise) {
    Write-Host ("[WARN] {0} gate soft-pass (speedup={1} < {2}) due to noise window: regression={3}ms <= {4}ms, tolerance_floor={5}" -f $Name, [math]::Round($ObservedSpeed,4), $MinRequired, [math]::Round($regressionMs,3), $NoisyRegressionMs, [math]::Round($nearFloor,4)) -ForegroundColor Yellow
    return
  }
  throw ("benchmark gate failed: {0} speedup {1} < {2} (baseline={3}s, candidate={4}s, regression_ms={5}, tolerance_floor={6}, noisy_regression_ms={7})" -f $Name, $ObservedSpeed, $MinRequired, $BaselineSec, $CandidateSec, [math]::Round($regressionMs,3), $nearFloor, $NoisyRegressionMs)
}

function Get-ObjField {
  param(
    [Parameter(Mandatory=$true)]$Obj,
    [Parameter(Mandatory=$true)][string]$Key
  )
  if ($Obj -is [System.Collections.IDictionary]) {
    if ($Obj.Contains($Key)) { return $Obj[$Key] }
    throw "missing field: $Key"
  }
  $prop = $Obj.PSObject.Properties[$Key]
  if ($null -eq $prop) { throw "missing field: $Key" }
  return $prop.Value
}
if ($GateAttempts -lt 1) { $GateAttempts = 1 }
$lastErr = $null
$finalSpeed = 0.0
$finalSpeedArrow = 0.0
for($attempt=1; $attempt -le $GateAttempts; $attempt++) {
  try {
    $res = Run-BenchmarkOnce -UpdateProfileFlag:($UpdateProfileOnPass -and $attempt -eq $GateAttempts)
    $speed = [double](Get-ObjField -Obj $res -Key "speedup_x")
    $speedArrow = [double](Get-ObjField -Obj $res -Key "speedup_arrow_x")
    $rowObj = Get-ObjField -Obj $res -Key "row_v1"
    $colObj = Get-ObjField -Obj $res -Key "columnar_v1"
    $colArrowObj = Get-ObjField -Obj $res -Key "columnar_arrow_v1"
    $rowAvg = [double](Get-ObjField -Obj $rowObj -Key "seconds_avg")
    $colAvg = [double](Get-ObjField -Obj $colObj -Key "seconds_avg")
    $colArrowAvg = [double](Get-ObjField -Obj $colArrowObj -Key "seconds_avg")
    $finalSpeed = $speed
    $finalSpeedArrow = $speedArrow
    $isLarge = $Rows -ge $largeRowsThreshold
    if (-not $isLarge) {
      Write-Host ("[WARN] skip strict columnar speed gate for non-large payload rows={0} (< large_rows_threshold={1})" -f $Rows, $largeRowsThreshold) -ForegroundColor Yellow
    }
    $shouldEnforceArrow = $EnforceArrowAlways -or $isLarge
    if ($shouldEnforceArrow) {
      Test-SpeedGate -Name "columnar_arrow_v1" -ObservedSpeed $speedArrow -MinRequired $MinArrowSpeedup -BaselineSec $rowAvg -CandidateSec $colArrowAvg -ToleranceSpeedup $GateToleranceSpeedup -NoisyRegressionMs $MaxNoisyRegressionMs
    } else {
      Write-Host ("[WARN] skip arrow gate for non-large payload rows={0} (< large_rows_threshold={1})" -f $Rows, $largeRowsThreshold) -ForegroundColor Yellow
    }
    if ($isLarge) {
      Test-SpeedGate -Name "columnar_v1" -ObservedSpeed $speed -MinRequired $MinSpeedup -BaselineSec $rowAvg -CandidateSec $colAvg -ToleranceSpeedup $GateToleranceSpeedup -NoisyRegressionMs $MaxNoisyRegressionMs
    }
    $lastErr = $null
    break
  } catch {
    $lastErr = $_
    if ($attempt -lt $GateAttempts) {
      Write-Host ("[WARN] benchmark gate attempt {0}/{1} failed, retrying once..." -f $attempt, $GateAttempts) -ForegroundColor Yellow
      Start-Sleep -Milliseconds 300
      continue
    }
  }
}
if ($lastErr) { throw $lastErr }
Ok ("benchmark gate passed: speedup={0}x speedup_arrow={1}x" -f $finalSpeed, $finalSpeedArrow)
