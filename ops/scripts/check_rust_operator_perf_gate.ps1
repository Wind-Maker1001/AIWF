param(
  [string]$Base = "http://127.0.0.1:18082",
  [int]$MaxP95TransformMs = 1500,
  [int]$MaxP95JoinMs = 1800
)

$ErrorActionPreference = "Stop"
function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Fail($m){ Write-Host "[FAIL] $m" -ForegroundColor Red; throw $m }

Info "fetching runtime stats"
$resp = Invoke-RestMethod -Uri "$Base/operators/runtime_stats_v1" -Method Post -ContentType "application/json" -Body (@{
  op = "summary"
} | ConvertTo-Json -Depth 5)
if (-not $resp.ok) { Fail "runtime_stats_v1 returned not ok" }

$items = @($resp.items)
function FindP95([string]$opName) {
  $hit = $items | Where-Object { $_.operator -eq $opName } | Select-Object -First 1
  if ($null -eq $hit) { return 0 }
  return [int]($hit.p95_ms)
}

$p95Transform = FindP95 "transform_rows_v3"
$p95Join = FindP95 "join_rows_v4"

Info "p95 transform_rows_v3 = $p95Transform ms"
Info "p95 join_rows_v4 = $p95Join ms"

if ($p95Transform -gt $MaxP95TransformMs) {
  Fail "perf gate failed: transform_rows_v3 p95=$p95Transform > $MaxP95TransformMs"
}
if ($p95Join -gt $MaxP95JoinMs) {
  Fail "perf gate failed: join_rows_v4 p95=$p95Join > $MaxP95JoinMs"
}

Ok "rust operator perf gate passed"
