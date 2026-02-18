param(
  [string]$AccelUrl = "http://127.0.0.1:18082"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

Info "chaos check: submit task and verify service remains available"
$payload = @{
  run_id = "chaos-check"
  rows = @(
    @{ id = "1"; amount = "1" },
    @{ id = "2"; amount = "2" }
  )
  rules = @{ casts = @{ id = "int"; amount = "float" } }
} | ConvertTo-Json -Depth 8

$sub = Invoke-RestMethod -Method Post -Uri "$AccelUrl/operators/transform_rows_v2/submit" -ContentType "application/json" -Body $payload -TimeoutSec 15
if (-not $sub.task_id) { throw "submit missing task_id" }
Start-Sleep -Milliseconds 500
$task = Invoke-RestMethod -Method Get -Uri ("$AccelUrl/tasks/{0}" -f $sub.task_id) -TimeoutSec 15
$health = Invoke-RestMethod -Method Get -Uri "$AccelUrl/health" -TimeoutSec 10
if (-not $health.ok) { throw "health not ok after chaos submit" }
Ok ("chaos pass: task={0}, status={1}, health_ok={2}" -f $sub.task_id, $task.status, $health.ok)
