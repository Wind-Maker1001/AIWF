param(
  [string]$RustDir = "",
  [int]$MaxColumnarMs = 1200,
  [int]$MaxStreamWindowMs = 1200,
  [int]$MaxSketchMs = 1200
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $RustDir) {
  $RustDir = Join-Path $root "apps\accel-rust"
}
if (-not (Test-Path $RustDir)) {
  throw "rust dir not found: $RustDir"
}

$env:AIWF_BENCH_MAX_COLUMNAR_MS = "$MaxColumnarMs"
$env:AIWF_BENCH_MAX_STREAM_WINDOW_MS = "$MaxStreamWindowMs"
$env:AIWF_BENCH_MAX_SKETCH_MS = "$MaxSketchMs"

Info "running rust new-ops benchmark gate"
Push-Location $RustDir
try {
  cargo test -q benchmark_new_ops_gate -- --ignored
}
finally {
  Pop-Location
}
if ($LASTEXITCODE -ne 0) {
  throw "rust new-ops benchmark gate failed"
}
Ok "rust new-ops benchmark gate passed"
