param(
  [string]$AccelUrl = "http://127.0.0.1:18082",
  [int]$Tasks = 30,
  [int]$RowsPerTask = 2000,
  [int]$PollIntervalMs = 200,
  [int]$TimeoutSeconds = 180
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$logDir = Join-Path $root "ops\logs\perf"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$jsonOut = Join-Path $logDir ("async_tasks_baseline_{0}.json" -f $stamp)
$mdOut = Join-Path $logDir ("async_tasks_baseline_{0}.md" -f $stamp)
$latestJson = Join-Path $logDir "async_tasks_baseline_latest.json"
$latestMd = Join-Path $logDir "async_tasks_baseline_latest.md"
$tmp = Join-Path $env:TEMP ("aiwf_bench_async_{0}.py" -f $stamp)

$py = @'
import json
import sys
import time
import statistics
from random import randint
import requests

base = sys.argv[1].rstrip("/")
tasks = int(sys.argv[2])
rows_per_task = int(sys.argv[3])
poll_ms = int(sys.argv[4])
timeout_s = int(sys.argv[5])

submit_url = base + "/operators/transform_rows_v2/submit"
task_url = base + "/tasks/{}"

def pct(arr, p):
    if not arr:
        return 0.0
    arr = sorted(arr)
    k = (len(arr)-1) * p
    f = int(k)
    c = min(f+1, len(arr)-1)
    if f == c:
        return float(arr[f])
    return float(arr[f] + (arr[c]-arr[f]) * (k-f))

submit_lat_ms = []
task_records = []
for i in range(tasks):
    rows = [{"id": str(j), "amount": str(randint(1, 9999)), "g": f"g{j%17}"} for j in range(rows_per_task)]
    payload = {
      "run_id": f"bench_async_{i}",
      "rows": rows,
      "rules": {
        "casts": {"id":"int","amount":"float"},
        "filters":[{"field":"amount","op":"gte","value":1}],
        "aggregate":{"group_by":["g"],"metrics":[{"field":"amount","op":"sum","as":"sum_amount"}]}
      },
      "quality_gates": {"min_output_rows": 1}
    }
    t0 = time.perf_counter()
    r = requests.post(submit_url, json=payload, timeout=60)
    t1 = time.perf_counter()
    if r.status_code >= 400:
        raise RuntimeError(f"submit failed {r.status_code}: {r.text[:300]}")
    j = r.json()
    task_id = str(j.get("task_id") or "")
    if not task_id:
        raise RuntimeError("submit missing task_id")
    submit_lat_ms.append((t1-t0)*1000.0)
    task_records.append({"task_id": task_id, "submit_t": time.time(), "status": "queued", "done_t": 0.0})

deadline = time.time() + timeout_s
pending = {x["task_id"]: x for x in task_records}
while pending and time.time() < deadline:
    done_ids = []
    for tid, rec in list(pending.items()):
        rr = requests.get(task_url.format(tid), timeout=20)
        if rr.status_code == 404:
            continue
        if rr.status_code >= 400:
            continue
        jj = rr.json()
        st = str(jj.get("status") or "")
        rec["status"] = st
        if st in ("done", "failed", "cancelled"):
            rec["done_t"] = time.time()
            done_ids.append(tid)
    for tid in done_ids:
        pending.pop(tid, None)
    if pending:
        time.sleep(max(0.05, poll_ms/1000.0))

dur_ms = []
success = 0
failed = 0
cancelled = 0
timeouts = 0
for r in task_records:
    if r["done_t"] <= 0:
        timeouts += 1
        continue
    d = (r["done_t"] - r["submit_t"]) * 1000.0
    dur_ms.append(d)
    if r["status"] == "done":
        success += 1
    elif r["status"] == "cancelled":
        cancelled += 1
    else:
        failed += 1

summary = {
  "ok": True,
  "accel_url": base,
  "tasks": tasks,
  "rows_per_task": rows_per_task,
  "timeout_seconds": timeout_s,
  "submit_ms": {
    "p50": round(pct(submit_lat_ms, 0.50), 3),
    "p90": round(pct(submit_lat_ms, 0.90), 3),
    "p99": round(pct(submit_lat_ms, 0.99), 3),
    "avg": round(statistics.fmean(submit_lat_ms) if submit_lat_ms else 0.0, 3),
  },
  "end_to_end_ms": {
    "p50": round(pct(dur_ms, 0.50), 3),
    "p90": round(pct(dur_ms, 0.90), 3),
    "p99": round(pct(dur_ms, 0.99), 3),
    "avg": round(statistics.fmean(dur_ms) if dur_ms else 0.0, 3),
  },
  "result": {
    "done": success,
    "failed": failed,
    "cancelled": cancelled,
    "timeout": timeouts
  }
}
print(json.dumps(summary, ensure_ascii=False))
'@

Set-Content -Path $tmp -Encoding UTF8 -Value $py
try {
  Info "running async benchmark: tasks=$Tasks rows_per_task=$RowsPerTask"
  $raw = & python $tmp $AccelUrl $Tasks $RowsPerTask $PollIntervalMs $TimeoutSeconds
  if ($LASTEXITCODE -ne 0) { throw "async benchmark failed" }
  $res = $raw | ConvertFrom-Json
  $res | ConvertTo-Json -Depth 8 | Set-Content -Path $jsonOut -Encoding UTF8
  Copy-Item -Path $jsonOut -Destination $latestJson -Force

  $md = @(
    "# Rust Async Task Baseline",
    "",
    ("- time: {0}" -f (Get-Date).ToString("s")),
    ("- accel_url: {0}" -f $res.accel_url),
    ("- tasks: {0}" -f $res.tasks),
    ("- rows_per_task: {0}" -f $res.rows_per_task),
    "",
    "## Submit Latency (ms)",
    ("- p50: {0}" -f $res.submit_ms.p50),
    ("- p90: {0}" -f $res.submit_ms.p90),
    ("- p99: {0}" -f $res.submit_ms.p99),
    ("- avg: {0}" -f $res.submit_ms.avg),
    "",
    "## End-to-End Latency (ms)",
    ("- p50: {0}" -f $res.end_to_end_ms.p50),
    ("- p90: {0}" -f $res.end_to_end_ms.p90),
    ("- p99: {0}" -f $res.end_to_end_ms.p99),
    ("- avg: {0}" -f $res.end_to_end_ms.avg),
    "",
    "## Result Count",
    ("- done: {0}" -f $res.result.done),
    ("- failed: {0}" -f $res.result.failed),
    ("- cancelled: {0}" -f $res.result.cancelled),
    ("- timeout: {0}" -f $res.result.timeout)
  ) -join "`r`n"
  Set-Content -Path $mdOut -Encoding UTF8 -Value $md
  Copy-Item -Path $mdOut -Destination $latestMd -Force

  Ok ("async benchmark done: p50_submit={0}ms p50_e2e={1}ms done={2} timeout={3}" -f $res.submit_ms.p50, $res.end_to_end_ms.p50, $res.result.done, $res.result.timeout)
  Ok ("json: {0}" -f $jsonOut)
  Ok ("md: {0}" -f $mdOut)
}
finally {
  Remove-Item -Path $tmp -ErrorAction SilentlyContinue
}
