param(
  [string]$AccelUrl = "http://127.0.0.1:18082",
  [int]$Rows = 50000,
  [int]$Runs = 3,
  [int]$Warmup = 1,
  [int]$Seed = 42,
  [string]$OutDir = "",
  [switch]$UpdateProfile,
  [string]$ProfilePath = "",
  [int]$MinMediumRowsFloor = 20000,
  [int]$MinLargeRowsFloor = 120000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $OutDir) { $OutDir = Join-Path $root "ops\logs\bench\rust_transform" }
if (-not $ProfilePath) { $ProfilePath = Join-Path $root "apps\accel-rust\conf\transform_engine_profile.json" }
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$runDir = Join-Path $OutDir $stamp
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

$tmp = Join-Path $env:TEMP "aiwf_bench_rust_transform_compare.py"
$py = @'
import json
import sys
import time
import statistics
import tempfile
import os
import random
import requests

base = sys.argv[1].rstrip("/")
rows_n = int(sys.argv[2])
runs = int(sys.argv[3])
warmup = int(sys.argv[4])
seed = int(sys.argv[5])
out_json = sys.argv[6]

random.seed(seed)

url = base + "/operators/transform_rows_v2"
rows = []
for i in range(rows_n):
    rid = i % (rows_n // 2 + 1)
    amount = random.randint(1, 1000)
    qty = random.randint(1, 20)
    score = random.randint(1, 10000) / 100.0
    active = "true" if (i % 3 != 0) else "false"
    rows.append({
        "id": str(rid),
        "amount": str(amount),
        "qty": str(qty),
        "score": f"{score:.2f}",
        "active": active,
        "group": "g" + str(i % 20),
    })
tmp_input = tempfile.NamedTemporaryFile(prefix="aiwf_bench_rows_", suffix=".csv", delete=False, mode="w", encoding="utf-8")
try:
    tmp_input.write("id,amount,qty,score,active,group\n")
    for row in rows:
        tmp_input.write(
            f"{row['id']},{row['amount']},{row['qty']},{row['score']},{row['active']},{row['group']}\n"
        )
    tmp_input.close()
    input_uri = tmp_input.name
except Exception:
    try:
        tmp_input.close()
    except Exception:
        pass
    raise

def one(engine):
    payload = {
      "run_id": f"bench_rust_v2_{engine}_{time.time_ns()}",
      "input_uri": input_uri,
      "rules": {
        "execution_engine": engine,
        "casts": {"id": "int", "amount": "float", "qty": "int", "score": "float", "active": "bool"},
        "filters": [
          {"field": "amount", "op": "gte", "value": 10},
          {"field": "qty", "op": "gte", "value": 2},
          {"field": "active", "op": "eq", "value": "true"}
        ],
        "deduplicate_by": ["id"],
        "deduplicate_keep": "last",
        "sort_by": [{"field": "id", "order": "asc"}, {"field": "score", "order": "desc"}],
        "aggregate": {
          "group_by": ["group"],
          "metrics": [
            {"field": "amount", "op": "sum", "as": "sum_amount"},
            {"field": "qty", "op": "sum", "as": "sum_qty"},
            {"field": "score", "op": "avg", "as": "avg_score"}
          ]
        }
      },
      "quality_gates": {"max_invalid_rows": 0}
    }
    t0 = time.perf_counter()
    r = requests.post(url, json=payload, timeout=180)
    t1 = time.perf_counter()
    if r.status_code >= 400:
      raise RuntimeError(f"{engine}: {r.status_code} {r.text[:400]}")
    j = r.json()
    if not j.get("ok", False):
      raise RuntimeError(f"{engine}: non-ok response: {json.dumps(j, ensure_ascii=False)[:400]}")
    if str(j.get("status", "")).lower() not in ("success", "ok", "done"):
      raise RuntimeError(f"{engine}: bad status: {json.dumps(j, ensure_ascii=False)[:400]}")
    stats = j.get("stats") or {}
    input_rows = int(stats.get("input_rows") or 0)
    if input_rows <= 0:
      raise RuntimeError(f"{engine}: empty input detected: {json.dumps(j, ensure_ascii=False)[:400]}")
    return {
      "seconds": t1 - t0,
      "rust_latency_ms": stats.get("latency_ms"),
      "rows_out": len(j.get("rows") or []),
      "trace_id": j.get("trace_id"),
    }

def bench(engine):
    for _ in range(max(0, warmup)):
      one(engine)
    records = [one(engine) for _ in range(max(1, runs))]
    secs = [x["seconds"] for x in records]
    lat = [x["rust_latency_ms"] for x in records if x.get("rust_latency_ms") is not None]
    return {
      "engine": engine,
      "runs": len(records),
      "rows_out": records[-1]["rows_out"],
      "seconds_avg": round(statistics.mean(secs), 4),
      "seconds_median": round(statistics.median(secs), 4),
      "seconds_p95": round(sorted(secs)[max(0, int(len(secs)*0.95)-1)], 4),
      "rust_latency_ms_avg": round(statistics.mean(lat), 2) if lat else None,
      "rust_latency_ms_median": round(statistics.median(lat), 2) if lat else None,
      "records": records,
    }

try:
    row = bench("row_v1")
    col = bench("columnar_v1")
    col_arrow = bench("columnar_arrow_v1")
    speedup = (row["seconds_avg"] / col["seconds_avg"]) if col["seconds_avg"] > 0 else None
    speedup_arrow = (row["seconds_avg"] / col_arrow["seconds_avg"]) if col_arrow["seconds_avg"] > 0 else None
    out = {
      "ok": True,
      "seed": seed,
      "rows_in": rows_n,
      "warmup": warmup,
      "runs": runs,
      "row_v1": row,
      "columnar_v1": col,
      "columnar_arrow_v1": col_arrow,
      "speedup_x": round(speedup, 3) if speedup else None,
      "speedup_arrow_x": round(speedup_arrow, 3) if speedup_arrow else None,
    }
    with open(out_json, "w", encoding="utf-8") as f:
      json.dump(out, f, ensure_ascii=False, indent=2)
    print(json.dumps(out, ensure_ascii=False))
finally:
    try:
        os.remove(input_uri)
    except Exception:
        pass
'@

Set-Content -Path $tmp -Encoding UTF8 -Value $py
try {
  $jsonPath = Join-Path $runDir "benchmark.json"
  Info "running rust transform benchmark compare rows=$Rows runs=$Runs warmup=$Warmup seed=$Seed"
  $raw = & python $tmp $AccelUrl $Rows $Runs $Warmup $Seed $jsonPath
  if ($LASTEXITCODE -ne 0) { throw "benchmark failed" }
  $res = $raw | ConvertFrom-Json
  if (-not $res.ok) { throw "rust transform benchmark returned not ok" }

  $profileForEval = $null
  if (Test-Path $ProfilePath) {
    try { $profileForEval = Get-Content $ProfilePath | ConvertFrom-Json } catch {}
  }
  $mediumForEval = if ($profileForEval -and $null -ne $profileForEval.medium_rows_threshold) { [int]$profileForEval.medium_rows_threshold } else { $MinMediumRowsFloor }
  $largeForEval = if ($profileForEval -and $null -ne $profileForEval.large_rows_threshold) { [int]$profileForEval.large_rows_threshold } else { $MinLargeRowsFloor }
  if ($largeForEval -le $mediumForEval) { $largeForEval = $mediumForEval + 1000 }
  $predictedEngine = if ($Rows -ge $largeForEval) { "columnar_arrow_v1" } elseif ($Rows -ge $mediumForEval) { "columnar_v1" } else { "row_v1" }
  $scoresEval = @(
    [pscustomobject]@{ engine = "row_v1"; seconds = [double]$res.row_v1.seconds_avg },
    [pscustomobject]@{ engine = "columnar_v1"; seconds = [double]$res.columnar_v1.seconds_avg },
    [pscustomobject]@{ engine = "columnar_arrow_v1"; seconds = [double]$res.columnar_arrow_v1.seconds_avg }
  ) | Sort-Object -Property seconds
  $bestEngine = [string]($scoresEval | Select-Object -First 1 -ExpandProperty engine)
  $res | Add-Member -NotePropertyName auto_decision_profile -NotePropertyValue ("medium={0},large={1}" -f $mediumForEval, $largeForEval) -Force
  $res | Add-Member -NotePropertyName auto_decision_predicted_engine -NotePropertyValue $predictedEngine -Force
  $res | Add-Member -NotePropertyName auto_decision_best_engine -NotePropertyValue $bestEngine -Force
  $res | Add-Member -NotePropertyName auto_decision_hit_rate -NotePropertyValue $(if ($predictedEngine -eq $bestEngine) { 1.0 } else { 0.0 }) -Force
  ($res | ConvertTo-Json -Depth 12) | Set-Content -Path $jsonPath -Encoding UTF8

  $md = Join-Path $runDir "benchmark_report.md"
  $lines = @()
  $lines += "# Rust Transform Engine Benchmark"
  $lines += ""
  $lines += "- Time: $(Get-Date -Format o)"
  $lines += "- AccelUrl: $AccelUrl"
  $lines += "- Rows: $Rows"
  $lines += "- Runs: $Runs"
  $lines += "- Warmup: $Warmup"
  $lines += "- Seed: $Seed"
  $lines += ""
  $lines += "## Summary"
  $lines += "- row_v1 seconds_avg: $($res.row_v1.seconds_avg)"
  $lines += "- row_v1 seconds_median: $($res.row_v1.seconds_median)"
  $lines += "- columnar_v1 seconds_avg: $($res.columnar_v1.seconds_avg)"
  $lines += "- columnar_v1 seconds_median: $($res.columnar_v1.seconds_median)"
  $lines += "- columnar_arrow_v1 seconds_avg: $($res.columnar_arrow_v1.seconds_avg)"
  $lines += "- columnar_arrow_v1 seconds_median: $($res.columnar_arrow_v1.seconds_median)"
  $lines += "- speedup_x (row/columnar_v1): $($res.speedup_x)"
  $lines += "- speedup_arrow_x (row/columnar_arrow_v1): $($res.speedup_arrow_x)"
  if ($null -ne $res.auto_decision_hit_rate) {
    $lines += "- auto_decision_hit_rate: $([math]::Round([double]$res.auto_decision_hit_rate, 4))"
  }
  if ($null -ne $res.auto_decision_profile) {
    $lines += "- auto_decision_profile: $($res.auto_decision_profile)"
  }
  $lines += ""
  $lines += "## Rust Latency"
  $lines += "- row_v1 rust_latency_ms_avg: $($res.row_v1.rust_latency_ms_avg)"
  $lines += "- row_v1 rust_latency_ms_median: $($res.row_v1.rust_latency_ms_median)"
  $lines += "- columnar_v1 rust_latency_ms_avg: $($res.columnar_v1.rust_latency_ms_avg)"
  $lines += "- columnar_v1 rust_latency_ms_median: $($res.columnar_v1.rust_latency_ms_median)"
  $lines += "- columnar_arrow_v1 rust_latency_ms_avg: $($res.columnar_arrow_v1.rust_latency_ms_avg)"
  $lines += "- columnar_arrow_v1 rust_latency_ms_median: $($res.columnar_arrow_v1.rust_latency_ms_median)"
  Set-Content -Path $md -Value ($lines -join [Environment]::NewLine) -Encoding UTF8

  Copy-Item $jsonPath (Join-Path $OutDir "latest.json") -Force
  Copy-Item $md (Join-Path $OutDir "latest.md") -Force
  $historyPath = Join-Path $OutDir "history.jsonl"
  Add-Content -Path $historyPath -Encoding UTF8 -Value ($res | ConvertTo-Json -Depth 12 -Compress)

  if ($UpdateProfile) {
    $history = @()
    if (Test-Path $historyPath) {
      Get-Content $historyPath | ForEach-Object {
        $line = $_.Trim()
        if (-not $line) { return }
        try { $history += ($line | ConvertFrom-Json) } catch {}
      }
    }
    $mediumCandidates = @($history | Where-Object { [double]($_.speedup_x) -ge 1.0 } | Sort-Object rows_in)
    $largeCandidates = @($history | Where-Object { [double]($_.speedup_arrow_x) -ge 1.0 } | Sort-Object rows_in)
    $mediumRows = if ($mediumCandidates.Count -gt 0) { [int]$mediumCandidates[0].rows_in } else { $MinMediumRowsFloor }
    $mediumRows = [Math]::Max($MinMediumRowsFloor, $mediumRows)
    $largeRows = if ($largeCandidates.Count -gt 0) { [int]$largeCandidates[0].rows_in } else { [Math]::Max($MinLargeRowsFloor, $mediumRows * 2) }
    $largeRows = [Math]::Max($MinLargeRowsFloor, $largeRows)
    if ($largeRows -le $mediumRows) { $largeRows = $mediumRows + 1000 }

    $decisionRecords = @($history | Where-Object {
      $null -ne $_.row_v1 -and $null -ne $_.columnar_v1 -and $null -ne $_.columnar_arrow_v1
    })
    $decisionHitCount = 0
    foreach ($item in $decisionRecords) {
      $rowsIn = [int]$item.rows_in
      $predicted = if ($rowsIn -ge $largeRows) { "columnar_arrow_v1" } elseif ($rowsIn -ge $mediumRows) { "columnar_v1" } else { "row_v1" }
      $scores = @(
        [pscustomobject]@{ engine = "row_v1"; seconds = [double]$item.row_v1.seconds_avg },
        [pscustomobject]@{ engine = "columnar_v1"; seconds = [double]$item.columnar_v1.seconds_avg },
        [pscustomobject]@{ engine = "columnar_arrow_v1"; seconds = [double]$item.columnar_arrow_v1.seconds_avg }
      ) | Sort-Object -Property seconds
      $best = [string]($scores | Select-Object -First 1 -ExpandProperty engine)
      if ($predicted -eq $best) { $decisionHitCount += 1 }
    }
    $decisionHitRate = if ($decisionRecords.Count -gt 0) { [double]$decisionHitCount / [double]$decisionRecords.Count } else { $null }

    $profile = @{
      medium_rows_threshold = $mediumRows
      large_rows_threshold = $largeRows
      medium_complexity_threshold = 8
      medium_bytes_threshold = 12582912
      large_bytes_threshold = 50331648
      auto_decision_hit_rate = $decisionHitRate
      auto_decision_hit_samples = $decisionRecords.Count
      updated_at = (Get-Date -Format o)
      source = "bench_learning_v2"
      samples = $history.Count
    }
    New-Item -ItemType Directory -Path (Split-Path -Parent $ProfilePath) -Force | Out-Null
    ($profile | ConvertTo-Json -Depth 6) | Set-Content -Path $ProfilePath -Encoding UTF8
    if ($null -ne $decisionHitRate) {
      Ok ("engine profile updated: medium_rows={0}, large_rows={1}, samples={2}, hit_rate={3}" -f $mediumRows, $largeRows, $history.Count, ([Math]::Round($decisionHitRate, 4)))
    } else {
      Ok ("engine profile updated: medium_rows={0}, large_rows={1}, samples={2}" -f $mediumRows, $largeRows, $history.Count)
    }
  }

  Ok ("benchmark passed: row_v1={0}s columnar_v1={1}s columnar_arrow_v1={2}s speedup={3}x speedup_arrow={4}x" -f $res.row_v1.seconds_avg, $res.columnar_v1.seconds_avg, $res.columnar_arrow_v1.seconds_avg, $res.speedup_x, $res.speedup_arrow_x)
  Write-Host "report: $md"
}
finally {
  Remove-Item -Path $tmp -ErrorAction SilentlyContinue
}
