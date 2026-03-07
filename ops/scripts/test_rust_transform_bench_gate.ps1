Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$gateScript = Join-Path $PSScriptRoot "check_rust_transform_bench_gate.ps1"
if (-not (Test-Path $gateScript)) {
  throw "gate script not found: $gateScript"
}

$scratch = Join-Path $env:TEMP ("aiwf_rust_transform_gate_test_{0}" -f ([Guid]::NewGuid().ToString("N")))
New-Item -ItemType Directory -Force -Path $scratch | Out-Null

function New-BenchStub {
  param(
    [Parameter(Mandatory=$true)][string]$Path,
    [Parameter(Mandatory=$true)][hashtable]$Payload
  )

  $json = $Payload | ConvertTo-Json -Depth 12 -Compress
  $escapedJson = $json.Replace("'", "''")
  $content = @'
param(
  [string]$AccelUrl = "",
  [int]$Rows = 0,
  [int]$Runs = 0,
  [int]$Warmup = 0,
  [int]$Seed = 0,
  [string]$OutDir = "",
  [switch]$UpdateProfile
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($OutDir)) {
  throw "OutDir is required"
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$runDir = Join-Path $OutDir "20260101_000000"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$json = '__JSON_PAYLOAD__'
$jsonPath = Join-Path $runDir "benchmark.json"
Set-Content -Path $jsonPath -Value $json -Encoding UTF8
Write-Output $json
'@
  $content = $content.Replace("__JSON_PAYLOAD__", $escapedJson)
  Set-Content -Path $Path -Value $content -Encoding UTF8
}

function Invoke-Gate {
  param(
    [Parameter(Mandatory=$true)][string]$BenchScript,
    [Parameter(Mandatory=$true)][string]$OutDir
  )

  $stdoutPath = Join-Path $OutDir "gate.stdout.log"
  $stderrPath = Join-Path $OutDir "gate.stderr.log"
  New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
  $process = Start-Process `
    -FilePath "powershell" `
    -ArgumentList @(
      "-ExecutionPolicy", "Bypass",
      "-File", $gateScript,
      "-BenchScriptPath", $BenchScript,
      "-OutDir", $OutDir,
      "-Rows", "120000",
      "-Runs", "4",
      "-Warmup", "1",
      "-MinSpeedup", "1.01",
      "-MinArrowSpeedup", "0.90",
      "-GateToleranceSpeedup", "0.03",
      "-MaxNoisyRegressionMs", "15",
      "-EnforceArrowAlways"
    ) `
    -NoNewWindow `
    -Wait `
    -PassThru `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath
  $output = @()
  if (Test-Path $stdoutPath) {
    $output += Get-Content -Path $stdoutPath
  }
  if (Test-Path $stderrPath) {
    $output += Get-Content -Path $stderrPath
  }
  return [pscustomobject]@{
    ExitCode = [int]$process.ExitCode
    Output = @($output)
  }
}

function Write-GateOutput {
  param([object[]]$Lines)
  foreach ($line in @($Lines)) {
    Write-Host $line
  }
}

function Get-NormalizedGateFailure {
  param([object[]]$Lines)
  $text = (@($Lines) | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
  $match = [regex]::Match($text, 'benchmark gate failed:.*?(?=\s+At\s+[A-Z]:\\|\s+\+\s|$)', [System.Text.RegularExpressions.RegexOptions]::Singleline)
  if (-not $match.Success) {
    return ""
  }
  return ($match.Value -replace '\s+', ' ').Trim()
}

function New-SecondsRecord {
  param([double]$Seconds, [Nullable[double]]$RustLatencyMs = $null)
  $record = @{
    seconds = $Seconds
    rows_out = 57687
    trace_id = [Guid]::NewGuid().ToString("N")
  }
  if ($null -ne $RustLatencyMs) {
    $record["rust_latency_ms"] = [double]$RustLatencyMs
  }
  return $record
}

function New-EnginePayload {
  param(
    [double[]]$Seconds,
    [Nullable[double]]$LatencyMs = $null
  )

  $records = @()
  foreach ($value in $Seconds) {
    $records += New-SecondsRecord -Seconds $value -RustLatencyMs $LatencyMs
  }

  $payload = @{
    engine = "stub"
    runs = $records.Count
    rows_out = 57687
    seconds_avg = [math]::Round(($Seconds | Measure-Object -Average).Average, 4)
    seconds_median = [math]::Round((($Seconds | Sort-Object)[[int][math]::Floor($Seconds.Count / 2)] + ($Seconds | Sort-Object)[[int][math]::Floor(($Seconds.Count - 1) / 2)]) / 2.0, 4)
    seconds_p95 = [math]::Round(($Seconds | Sort-Object | Select-Object -Last 1), 4)
    records = $records
  }

  if ($null -ne $LatencyMs) {
    $payload["rust_latency_ms_avg"] = [double]$LatencyMs
    $payload["rust_latency_ms_median"] = [double]$LatencyMs
  }

  return $payload
}

try {
  $scenario1 = Join-Path $scratch "latency_metric_prefers_engine_latency.ps1"
  $scenario2 = Join-Path $scratch "seconds_fallback_soft_pass.ps1"
  $scenario3 = Join-Path $scratch "real_regression_fails.ps1"

  New-BenchStub -Path $scenario1 -Payload @{
    ok = $true
    seed = 42
    rows_in = 120000
    warmup = 1
    runs = 4
    row_v1 = (New-EnginePayload -Seconds @(0.5757, 0.5664, 0.5416, 0.5394) -LatencyMs 5708)
    columnar_v1 = (New-EnginePayload -Seconds @(0.5925, 0.5586, 0.5752, 0.5345) -LatencyMs 3940)
    columnar_arrow_v1 = (New-EnginePayload -Seconds @(0.5609, 0.5415, 0.5677, 0.5551) -LatencyMs 5561)
    speedup_x = 0.983
    speedup_arrow_x = 0.999
  }

  New-BenchStub -Path $scenario2 -Payload @{
    ok = $true
    seed = 42
    rows_in = 120000
    warmup = 1
    runs = 4
    row_v1 = (New-EnginePayload -Seconds @(0.5590, 0.5390, 0.5588, 0.5458))
    columnar_v1 = (New-EnginePayload -Seconds @(0.5725, 0.5367, 0.5625, 0.5366))
    columnar_arrow_v1 = (New-EnginePayload -Seconds @(0.5708, 0.5581, 0.5856, 0.6076))
    speedup_x = 0.997
    speedup_arrow_x = 0.948
  }

  New-BenchStub -Path $scenario3 -Payload @{
    ok = $true
    seed = 42
    rows_in = 120000
    warmup = 1
    runs = 4
    row_v1 = (New-EnginePayload -Seconds @(0.5500, 0.5520, 0.5490, 0.5510) -LatencyMs 5600)
    columnar_v1 = (New-EnginePayload -Seconds @(0.6050, 0.6100, 0.6080, 0.6070) -LatencyMs 6200)
    columnar_arrow_v1 = (New-EnginePayload -Seconds @(0.5300, 0.5320, 0.5310, 0.5290) -LatencyMs 5400)
    speedup_x = 0.907
    speedup_arrow_x = 1.037
  }

  Info "scenario 1: latency metric should override noisy wall-clock regression"
  $result = Invoke-Gate -BenchScript $scenario1 -OutDir (Join-Path $scratch "out1")
  Write-GateOutput -Lines $result.Output
  if ($result.ExitCode -ne 0) {
    throw "scenario 1 failed: expected success, exit code=$($result.ExitCode)"
  }
  Ok "scenario 1 passed"

  Info "scenario 2: wall-clock fallback should soft-pass within noise window"
  $result = Invoke-Gate -BenchScript $scenario2 -OutDir (Join-Path $scratch "out2")
  Write-GateOutput -Lines $result.Output
  if ($result.ExitCode -ne 0) {
    throw "scenario 2 failed: expected success, exit code=$($result.ExitCode)"
  }
  Ok "scenario 2 passed"

  Info "scenario 3: large regression should still fail"
  $result = Invoke-Gate -BenchScript $scenario3 -OutDir (Join-Path $scratch "out3")
  if ($result.ExitCode -eq 0) {
    throw "scenario 3 failed: expected non-zero exit code"
  }
  $expectedFailure = Get-NormalizedGateFailure -Lines $result.Output
  if (-not [string]::IsNullOrWhiteSpace($expectedFailure)) {
    Write-Host $expectedFailure
  }
  Ok "scenario 3 passed"

  Ok "rust transform benchmark gate self-test passed"
}
finally {
  Remove-Item -Recurse -Force $scratch -ErrorAction SilentlyContinue
}
