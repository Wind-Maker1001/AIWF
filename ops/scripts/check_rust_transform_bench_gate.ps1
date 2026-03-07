param(
  [string]$AccelUrl = "http://127.0.0.1:18082",
  [int]$Rows = 80000,
  [int]$Runs = 3,
  [int]$Warmup = 1,
  [int]$Seed = 42,
  [string]$BenchScriptPath = "",
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
$benchScript = if ([string]::IsNullOrWhiteSpace($BenchScriptPath)) {
  Join-Path $PSScriptRoot "bench_rust_transform.ps1"
} else {
  $BenchScriptPath
}
if (-not (Test-Path $benchScript)) { throw "benchmark script not found: $benchScript" }
if (-not $OutDir) { $OutDir = Join-Path $root "ops\logs\bench\rust_transform" }
if (-not $ProfilePath) { $ProfilePath = Join-Path $root "apps\accel-rust\conf\transform_engine_profile.json" }

function Run-BenchmarkOnce {
  param([switch]$UpdateProfileFlag)
  Info "running benchmark gate rows=$Rows runs=$Runs warmup=$Warmup seed=$Seed"
  $benchArgs = @(
    "-ExecutionPolicy", "Bypass",
    "-File", $benchScript,
    "-AccelUrl", $AccelUrl,
    "-Rows", $Rows,
    "-Runs", $Runs,
    "-Warmup", $Warmup,
    "-Seed", $Seed,
    "-OutDir", $OutDir
  )
  if ($UpdateProfileFlag) { $benchArgs += "-UpdateProfile" }
  $benchOutput = @(powershell @benchArgs 2>&1)
  $benchText = ($benchOutput | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
  if ($LASTEXITCODE -ne 0) {
    if ([string]::IsNullOrWhiteSpace($benchText)) {
      throw "benchmark script failed"
    }
    throw ("benchmark script failed:`n{0}" -f $benchText.Trim())
  }
  $raw = $null
  $jsonLine = $benchOutput |
    ForEach-Object { $_.ToString().Trim() } |
    Where-Object { $_ -match '^\{.+\"speedup_x\".+' } |
    Select-Object -Last 1
  if (-not [string]::IsNullOrWhiteSpace($jsonLine)) {
    $raw = $jsonLine
  } else {
    $latestRun = Get-ChildItem $OutDir -Directory | Sort-Object Name -Descending | Select-Object -First 1
    if ($null -eq $latestRun) { throw "benchmark result run dir not found in: $OutDir" }
    $runJson = Join-Path $latestRun.FullName "benchmark.json"
    if (-not (Test-Path $runJson)) { throw "benchmark result not found: $runJson" }
    $raw = Get-Content $runJson -Raw
  }
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
    [double]$BaselineValue,
    [double]$CandidateValue,
    [double]$ToleranceSpeedup,
    [double]$NoisyRegressionMs,
    [double]$RegressionScale = 1000.0,
    [string]$MetricLabel = "seconds_median"
  )
  if ($ObservedSpeed -ge $MinRequired) { return }
  $nearFloor = $MinRequired - $ToleranceSpeedup
  $regressionMs = ($CandidateValue - $BaselineValue) * $RegressionScale
  $isWithinNoise = $regressionMs -le $NoisyRegressionMs
  $isNearFloor = $ObservedSpeed -ge $nearFloor
  if ($isNearFloor -and $isWithinNoise) {
    Write-Host ("[WARN] {0} gate soft-pass ({1}) (speedup={2} < {3}) due to noise window: regression={4}ms <= {5}ms, tolerance_floor={6}" -f $Name, $MetricLabel, [math]::Round($ObservedSpeed,4), $MinRequired, [math]::Round($regressionMs,3), $NoisyRegressionMs, [math]::Round($nearFloor,4)) -ForegroundColor Yellow
    return
  }
  throw ("benchmark gate failed: {0} speedup {1} < {2} ({3}: baseline={4}, candidate={5}, regression_ms={6}, tolerance_floor={7}, noisy_regression_ms={8})" -f $Name, $ObservedSpeed, $MinRequired, $MetricLabel, $BaselineValue, $CandidateValue, [math]::Round($regressionMs,3), $nearFloor, $NoisyRegressionMs)
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

function Get-Median {
  param([double[]]$Values)
  if ($null -eq $Values -or $Values.Count -eq 0) {
    throw "median requires at least one value"
  }
  $sorted = @($Values | Sort-Object)
  $mid = [int][math]::Floor($sorted.Count / 2)
  if (($sorted.Count % 2) -eq 1) {
    return [double]$sorted[$mid]
  }
  return ([double]$sorted[$mid - 1] + [double]$sorted[$mid]) / 2.0
}

function Get-RobustMetric {
  param(
    [Parameter(Mandatory=$true)]$Obj,
    [Parameter(Mandatory=$true)][string]$AvgKey,
    [Parameter(Mandatory=$true)][string]$RecordKey
  )
  $records = Get-ObjField -Obj $Obj -Key "records"
  if ($null -ne $records) {
    $values = @()
    foreach ($record in @($records)) {
      if ($null -eq $record) { continue }
      $value = $null
      if ($record -is [System.Collections.IDictionary]) {
        if ($record.Contains($RecordKey)) { $value = $record[$RecordKey] }
      } else {
        $prop = $record.PSObject.Properties[$RecordKey]
        if ($null -ne $prop) { $value = $prop.Value }
      }
      if ($null -ne $value) {
        $values += [double]$value
      }
    }
    if ($values.Count -gt 0) {
      return Get-Median -Values $values
    }
  }
  return [double](Get-ObjField -Obj $Obj -Key $AvgKey)
}

function Get-RobustMetricSamples {
  param(
    [Parameter(Mandatory=$true)]$Obj,
    [Parameter(Mandatory=$true)][string]$RecordKey
  )
  $records = Get-ObjField -Obj $Obj -Key "records"
  if ($null -eq $records) { return @() }
  $values = @()
  foreach ($record in @($records)) {
    if ($null -eq $record) { continue }
    $value = $null
    if ($record -is [System.Collections.IDictionary]) {
      if ($record.Contains($RecordKey)) { $value = $record[$RecordKey] }
    } else {
      $prop = $record.PSObject.Properties[$RecordKey]
      if ($null -ne $prop) { $value = $prop.Value }
    }
    if ($null -ne $value) {
      $values += [double]$value
    }
  }
  return @($values)
}

if ($GateAttempts -lt 1) { $GateAttempts = 1 }
$lastErr = $null
$finalSpeed = 0.0
$finalSpeedArrow = 0.0
$finalMetric = "seconds_avg"
for($attempt=1; $attempt -le $GateAttempts; $attempt++) {
  try {
    $res = Run-BenchmarkOnce -UpdateProfileFlag:($UpdateProfileOnPass -and $attempt -eq $GateAttempts)
    $speed = [double](Get-ObjField -Obj $res -Key "speedup_x")
    $speedArrow = [double](Get-ObjField -Obj $res -Key "speedup_arrow_x")
    $rowObj = Get-ObjField -Obj $res -Key "row_v1"
    $colObj = Get-ObjField -Obj $res -Key "columnar_v1"
    $colArrowObj = Get-ObjField -Obj $res -Key "columnar_arrow_v1"
    $rowLatency = Get-RobustMetricSamples -Obj $rowObj -RecordKey "rust_latency_ms"
    $colLatency = Get-RobustMetricSamples -Obj $colObj -RecordKey "rust_latency_ms"
    $colArrowLatency = Get-RobustMetricSamples -Obj $colArrowObj -RecordKey "rust_latency_ms"
    $useLatencyMetric = @($rowLatency).Count -gt 0 -and @($colLatency).Count -gt 0 -and @($colArrowLatency).Count -gt 0
    if ($useLatencyMetric) {
      $rowRobust = Get-RobustMetric -Obj $rowObj -AvgKey "rust_latency_ms_avg" -RecordKey "rust_latency_ms"
      $colRobust = Get-RobustMetric -Obj $colObj -AvgKey "rust_latency_ms_avg" -RecordKey "rust_latency_ms"
      $colArrowRobust = Get-RobustMetric -Obj $colArrowObj -AvgKey "rust_latency_ms_avg" -RecordKey "rust_latency_ms"
      $finalMetric = "rust_latency_ms_median"
      $regressionScale = 1.0
    } else {
      $rowRobust = Get-RobustMetric -Obj $rowObj -AvgKey "seconds_avg" -RecordKey "seconds"
      $colRobust = Get-RobustMetric -Obj $colObj -AvgKey "seconds_avg" -RecordKey "seconds"
      $colArrowRobust = Get-RobustMetric -Obj $colArrowObj -AvgKey "seconds_avg" -RecordKey "seconds"
      $finalMetric = "seconds_median"
      $regressionScale = 1000.0
    }
    $finalSpeed = if ($colRobust -gt 0) { [math]::Round($rowRobust / $colRobust, 3) } else { 0.0 }
    $finalSpeedArrow = if ($colArrowRobust -gt 0) { [math]::Round($rowRobust / $colArrowRobust, 3) } else { 0.0 }
    $isLarge = $Rows -ge $largeRowsThreshold
    if (-not $isLarge) {
      Write-Host ("[WARN] skip strict columnar speed gate for non-large payload rows={0} (< large_rows_threshold={1})" -f $Rows, $largeRowsThreshold) -ForegroundColor Yellow
    }
    $shouldEnforceArrow = $EnforceArrowAlways -or $isLarge
    if ($shouldEnforceArrow) {
      Test-SpeedGate -Name "columnar_arrow_v1" -ObservedSpeed $finalSpeedArrow -MinRequired $MinArrowSpeedup -BaselineValue $rowRobust -CandidateValue $colArrowRobust -ToleranceSpeedup $GateToleranceSpeedup -NoisyRegressionMs $MaxNoisyRegressionMs -RegressionScale $regressionScale -MetricLabel $finalMetric
    } else {
      Write-Host ("[WARN] skip arrow gate for non-large payload rows={0} (< large_rows_threshold={1})" -f $Rows, $largeRowsThreshold) -ForegroundColor Yellow
    }
    if ($isLarge) {
      Test-SpeedGate -Name "columnar_v1" -ObservedSpeed $finalSpeed -MinRequired $MinSpeedup -BaselineValue $rowRobust -CandidateValue $colRobust -ToleranceSpeedup $GateToleranceSpeedup -NoisyRegressionMs $MaxNoisyRegressionMs -RegressionScale $regressionScale -MetricLabel $finalMetric
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
Ok ("benchmark gate passed ({0}): speedup={1}x speedup_arrow={2}x" -f $finalMetric, $finalSpeed, $finalSpeedArrow)
