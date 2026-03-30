param(
  [string]$AccelUrl = "http://127.0.0.1:18082",
  [int]$Tasks = 30,
  [int]$RowsPerTask = 2000,
  [int]$PollIntervalMs = 200,
  [int]$TimeoutSeconds = 180,
  [string]$TenantId = "bench_async",
  [int]$MaxInFlight = 4
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function EnvOr([string]$name, [string]$defaultVal) {
  $v = [System.Environment]::GetEnvironmentVariable($name, "Process")
  if ([string]::IsNullOrWhiteSpace($v)) { return $defaultVal }
  return $v
}

if (-not $PSBoundParameters.ContainsKey("TenantId")) {
  $TenantId = EnvOr "AIWF_ASYNC_BENCH_TENANT_ID" $TenantId
}
if (-not $PSBoundParameters.ContainsKey("MaxInFlight")) {
  $MaxInFlight = [int](EnvOr "AIWF_ASYNC_BENCH_MAX_IN_FLIGHT" ([string]$MaxInFlight))
}
$MaxInFlight = [Math]::Max(1, $MaxInFlight)

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
import uuid
from random import randint
import requests

base = sys.argv[1].rstrip("/")
tasks = int(sys.argv[2])
rows_per_task = int(sys.argv[3])
poll_ms = int(sys.argv[4])
timeout_s = int(sys.argv[5])
tenant_id = str(sys.argv[6] or "").strip() or "bench_async"
max_in_flight = max(1, int(sys.argv[7]))

submit_url = base + "/operators/transform_rows_v2/submit"
task_url = base + "/tasks/{}"
session = requests.Session()

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

deadline = time.time() + timeout_s
submit_lat_ms = []
task_records = []
pending = {}
submitted = 0
rejected_submissions = 0

def submit_one(index):
    global rejected_submissions
    rows = [{"id": str(j), "amount": str(randint(1, 9999)), "g": f"g{j%17}"} for j in range(rows_per_task)]
    run_id = f"bench_async_{index}_{uuid.uuid4().hex[:12]}"
    payload = {
      "run_id": run_id,
      "tenant_id": tenant_id,
      "idempotency_key": f"{run_id}_submit",
      "rows": rows,
      "rules": {
        "casts": {"id":"int","amount":"float"},
        "filters":[{"field":"amount","op":"gte","value":1}],
        "aggregate":{"group_by":["g"],"metrics":[{"field":"amount","op":"sum","as":"sum_amount"}]}
      },
      "quality_gates": {"min_output_rows": 1}
    }
    t0 = time.perf_counter()
    r = session.post(submit_url, json=payload, timeout=60)
    t1 = time.perf_counter()
    if r.status_code >= 400:
        body = (r.text or "")[:300]
        if r.status_code == 429 and "tenant concurrency exceeded" in body.lower():
            rejected_submissions += 1
            raise RuntimeError(
                f"submit failed 429: benchmark max_in_flight={max_in_flight} exceeds active tenant capacity "
                f"for tenant_id={tenant_id}; response={body}"
            )
        raise RuntimeError(f"submit failed {r.status_code}: {body}")
    j = r.json()
    task_id = str(j.get("task_id") or "")
    if not task_id:
        raise RuntimeError("submit missing task_id")
    rec = {"task_id": task_id, "submit_t": time.time(), "status": "queued", "done_t": 0.0}
    submit_lat_ms.append((t1-t0)*1000.0)
    task_records.append(rec)
    pending[task_id] = rec

while (submitted < tasks or pending) and time.time() < deadline:
    while submitted < tasks and len(pending) < max_in_flight:
        submit_one(submitted)
        submitted += 1

    done_ids = []
    poll_t = time.time()
    for tid, rec in list(pending.items()):
        rr = session.get(task_url.format(tid), timeout=20)
        if rr.status_code == 404:
            continue
        if rr.status_code >= 400:
            continue
        jj = rr.json()
        st = str(jj.get("status") or "")
        rec["status"] = st
        if st in ("done", "failed", "cancelled"):
            rec["done_t"] = poll_t
            done_ids.append(tid)
    for tid in done_ids:
        pending.pop(tid, None)
    if pending or submitted < tasks:
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
  "tenant_id": tenant_id,
  "max_in_flight": max_in_flight,
  "submission_mode": "quota_respecting",
  "rejected_submissions": rejected_submissions,
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
  Info "running async benchmark: tasks=$Tasks rows_per_task=$RowsPerTask tenant_id=$TenantId max_in_flight=$MaxInFlight"
  $raw = & python $tmp $AccelUrl $Tasks $RowsPerTask $PollIntervalMs $TimeoutSeconds $TenantId $MaxInFlight
  if ($LASTEXITCODE -ne 0) { throw "async benchmark failed" }
  $res = $raw | ConvertFrom-Json
  $res | ConvertTo-Json -Depth 8 | Set-Content -Path $jsonOut -Encoding UTF8
  Copy-Item -Path $jsonOut -Destination $latestJson -Force

  $md = @(
    "# Rust Async Task Baseline",
    "",
    ("- time: {0}" -f (Get-Date).ToString("s")),
    ("- accel_url: {0}" -f $res.accel_url),
    ("- tenant_id: {0}" -f $res.tenant_id),
    ("- max_in_flight: {0}" -f $res.max_in_flight),
    ("- submission_mode: {0}" -f $res.submission_mode),
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
    ("- timeout: {0}" -f $res.result.timeout),
    ("- rejected_submissions: {0}" -f $res.rejected_submissions)
  ) -join "`r`n"
  Set-Content -Path $mdOut -Encoding UTF8 -Value $md
  Copy-Item -Path $mdOut -Destination $latestMd -Force

  Ok ("async benchmark done: p50_submit={0}ms p50_e2e={1}ms done={2} timeout={3} rejected={4}" -f $res.submit_ms.p50, $res.end_to_end_ms.p50, $res.result.done, $res.result.timeout, $res.rejected_submissions)
  Ok ("json: {0}" -f $jsonOut)
  Ok ("md: {0}" -f $mdOut)
}
finally {
  Remove-Item -Path $tmp -ErrorAction SilentlyContinue
}
