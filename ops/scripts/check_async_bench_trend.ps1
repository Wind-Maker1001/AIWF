param(
  [string]$AccelUrl = "http://127.0.0.1:18082",
  [int]$Tasks = 12,
  [int]$RowsPerTask = 600,
  [int]$PollIntervalMs = 150,
  [int]$TimeoutSeconds = 120
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function EnvOr([string]$name, [string]$defaultVal) {
  $v = [System.Environment]::GetEnvironmentVariable($name, "Process")
  if ([string]::IsNullOrWhiteSpace($v)) { return $defaultVal }
  return $v
}

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$benchScript = Join-Path $PSScriptRoot "bench_rust_async_tasks.ps1"
if (-not (Test-Path $benchScript)) {
  throw "missing benchmark script: $benchScript"
}

Info "running async benchmark for trend gate"
powershell -ExecutionPolicy Bypass -File $benchScript -AccelUrl $AccelUrl -Tasks $Tasks -RowsPerTask $RowsPerTask -PollIntervalMs $PollIntervalMs -TimeoutSeconds $TimeoutSeconds
if ($LASTEXITCODE -ne 0) { throw "async benchmark failed" }

$perfDir = Join-Path $root "ops\logs\perf"
$latest = Join-Path $perfDir "async_tasks_baseline_latest.json"
if (-not (Test-Path $latest)) {
  throw "missing benchmark output: $latest"
}
$obj = Get-Content $latest -Raw | ConvertFrom-Json

$historyPath = Join-Path $perfDir "async_tasks_bench_history.jsonl"
$sample = [ordered]@{
  ts = (Get-Date).ToString("s")
  submit_p50 = [double]$obj.submit_ms.p50
  submit_p90 = [double]$obj.submit_ms.p90
  e2e_p50 = [double]$obj.end_to_end_ms.p50
  e2e_p90 = [double]$obj.end_to_end_ms.p90
  timeout = [int]$obj.result.timeout
  done = [int]$obj.result.done
}
($sample | ConvertTo-Json -Compress) | Add-Content -Path $historyPath -Encoding UTF8

$window = [Math]::Max(1, [int](EnvOr "AIWF_ASYNC_BENCH_TREND_WINDOW" "7"))
$minSamples = [Math]::Max(1, [int](EnvOr "AIWF_ASYNC_BENCH_TREND_MIN_SAMPLES" "5"))
$maxE2eP50 = [double](EnvOr "AIWF_ASYNC_BENCH_E2E_P50_MAX" "1200")
$maxSubmitP50 = [double](EnvOr "AIWF_ASYNC_BENCH_SUBMIT_P50_MAX" "300")
$maxTimeout = [int](EnvOr "AIWF_ASYNC_BENCH_TIMEOUT_MAX" "0")

$lines = @(Get-Content $historyPath | Where-Object { $_ -match "\S" })
$records = $lines | ForEach-Object { $_ | ConvertFrom-Json }
$recent = @($records | Select-Object -Last $window)

if ($recent.Count -lt $minSamples) {
  Warn "async trend warm-up: samples=$($recent.Count), required=$minSamples"
  exit 0
}

$median = {
  param($arr)
  $s = @($arr | Sort-Object)
  $n = $s.Count
  if ($n -eq 0) { return [double]0 }
  if ($n % 2 -eq 1) { return [double]$s[[int]($n/2)] }
  return [double](($s[($n/2)-1] + $s[$n/2]) / 2.0)
}

$medE2eP50 = & $median ($recent | ForEach-Object { [double]$_.e2e_p50 })
$medSubmitP50 = & $median ($recent | ForEach-Object { [double]$_.submit_p50 })
$maxTimeoutSeen = ($recent | Measure-Object -Property timeout -Maximum).Maximum

if ($medE2eP50 -gt $maxE2eP50) {
  throw "async trend gate failed: median e2e_p50=$medE2eP50 > $maxE2eP50"
}
if ($medSubmitP50 -gt $maxSubmitP50) {
  throw "async trend gate failed: median submit_p50=$medSubmitP50 > $maxSubmitP50"
}
if ($maxTimeoutSeen -gt $maxTimeout) {
  throw "async trend gate failed: timeout max=$maxTimeoutSeen > $maxTimeout"
}

Ok "async trend gate passed (median e2e_p50=$medE2eP50, median submit_p50=$medSubmitP50, timeout_max=$maxTimeoutSeen)"
