param(
  [string]$AccelUrl = "http://127.0.0.1:18082"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

Info "contract test: health"
$h = Invoke-RestMethod -Uri "$AccelUrl/health" -TimeoutSec 10
if (-not $h.ok) { throw "health contract failed: ok missing/false" }

Info "contract test: transform_rows_v2"
$body = @{
  run_id = "contract-1"
  rows = @(
    @{ id = "1"; amount = "10" },
    @{ id = "2"; amount = "20" }
  )
  rules = @{ casts = @{ id = "int"; amount = "float" } }
  quality_gates = @{ min_output_rows = 1 }
} | ConvertTo-Json -Depth 8
$t = Invoke-RestMethod -Method Post -Uri "$AccelUrl/operators/transform_rows_v2" -ContentType "application/json" -Body $body -TimeoutSec 20
if (-not $t.ok) { throw "transform_rows_v2 contract failed: ok false" }
foreach($k in @("operator","status","trace_id","rows","stats","quality","gate_result")) {
  if (-not $t.PSObject.Properties.Name.Contains($k)) { throw "transform_rows_v2 missing field: $k" }
}

Info "contract test: async submit/poll/cancel shape"
$sub = Invoke-RestMethod -Method Post -Uri "$AccelUrl/operators/transform_rows_v2/submit" -ContentType "application/json" -Body $body -TimeoutSec 20
if (-not $sub.task_id) { throw "submit missing task_id" }
$task = Invoke-RestMethod -Uri ("$AccelUrl/tasks/{0}" -f $sub.task_id) -TimeoutSec 20
foreach($k in @("task_id","operator","status","created_at","updated_at")) {
  if (-not $task.PSObject.Properties.Name.Contains($k)) { throw "task missing field: $k" }
}
$cancel = Invoke-RestMethod -Method Post -Uri ("$AccelUrl/tasks/{0}/cancel" -f $sub.task_id) -ContentType "application/json" -Body "{}" -TimeoutSec 20
foreach($k in @("ok","task_id","status")) {
  if (-not $cancel.PSObject.Properties.Name.Contains($k)) { throw "cancel missing field: $k" }
}

Ok "rust api contract tests passed"
