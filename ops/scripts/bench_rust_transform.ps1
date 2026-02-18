param(
  [string]$AccelUrl = "http://127.0.0.1:18082",
  [int]$Rows = 50000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$tmp = Join-Path $env:TEMP "aiwf_bench_rust_transform.py"

$py = @'
import json
import sys
import time
from random import randint
import requests

url = sys.argv[1].rstrip("/") + "/operators/transform_rows_v2"
n = int(sys.argv[2])
rows = [{"id": i, "amount": randint(1, 1000), "group": "g" + str(i % 20)} for i in range(n)]
payload = {
  "run_id": "bench_rust_v2",
  "rows": rows,
  "rules": {
    "casts": {"id": "int", "amount": "float"},
    "filters": [{"field": "amount", "op": "gte", "value": 10}],
    "deduplicate_by": ["id"],
    "sort_by": [{"field": "id", "order": "asc"}],
    "aggregate": {
      "group_by": ["group"],
      "metrics": [{"field": "amount", "op": "sum", "as": "sum_amount"}]
    }
  },
  "quality_gates": {"max_invalid_rows": 0}
}
t0 = time.perf_counter()
r = requests.post(url, json=payload, timeout=120)
t1 = time.perf_counter()
if r.status_code >= 400:
  raise RuntimeError(f"{r.status_code} {r.text[:400]}")
j = r.json()
print(json.dumps({
  "ok": j.get("ok", False),
  "rows_in": n,
  "rows_out": len(j.get("rows") or []),
  "seconds": round(t1 - t0, 3),
  "rust_latency_ms": (j.get("stats") or {}).get("latency_ms"),
  "trace_id": j.get("trace_id")
}, ensure_ascii=False))
'@

Set-Content -Path $tmp -Encoding UTF8 -Value $py
try {
  Info "running rust transform benchmark rows=$Rows"
  $raw = & python $tmp $AccelUrl $Rows
  if ($LASTEXITCODE -ne 0) { throw "benchmark failed" }
  $res = $raw | ConvertFrom-Json
  if (-not $res.ok) { throw "rust transform benchmark returned not ok" }
  Ok ("benchmark passed: rows_in={0}, rows_out={1}, seconds={2}, rust_latency_ms={3}" -f $res.rows_in, $res.rows_out, $res.seconds, $res.rust_latency_ms)
}
finally {
  Remove-Item -Path $tmp -ErrorAction SilentlyContinue
}
