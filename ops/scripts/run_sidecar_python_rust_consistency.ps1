param(
  [string]$DatasetDir = "",
  [string]$OutDir = "",
  [string]$AccelUrl = "",
  [switch]$RequireAccel
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $DatasetDir) {
  $DatasetDir = Join-Path $root "lake\datasets\regression_v1_2_sidecar_gold"
}
if (-not $OutDir) {
  $OutDir = Join-Path $root "ops\logs\regression"
}
if (-not $AccelUrl) {
  $AccelUrl = if ($env:AIWF_ACCEL_URL) { $env:AIWF_ACCEL_URL } else { "http://127.0.0.1:18082" }
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$tmp = Join-Path $env:TEMP "aiwf_sidecar_consistency.py"
$py = @'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

repo = Path(sys.argv[1])
dataset = Path(sys.argv[2])
out_dir = Path(sys.argv[3])
accel_url = str(sys.argv[4])
require_accel = str(sys.argv[5]).strip().lower() in {"1", "true", "yes", "on"}
out_dir.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(repo / "apps" / "glue-python"))

from aiwf.sidecar_regression import (
    evaluate_consistency_report,
    load_sidecar_dataset,
    run_python_rust_consistency,
)

items = []
for entry in load_sidecar_dataset(dataset):
    scenario = entry["scenario"]
    if not bool(scenario.get("consistency_compare", False)):
        continue
    items.append({
        "id": entry["id"],
        **run_python_rust_consistency(entry["dir"], scenario, accel_url),
    })

summary = evaluate_consistency_report(items, require_accel=require_accel)
report = {
    "ok": bool(summary["ok"]),
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "accel_url": accel_url,
    "require_accel": require_accel,
    "items": items,
    "failed": summary["failed"],
    "skipped": summary["skipped"],
}
json_path = out_dir / "sidecar_python_rust_consistency_report.json"
json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
md = [
    "# Sidecar Python Rust Consistency Report",
    "",
    f"- Time: {report['generated_at']}",
    f"- AccelUrl: {accel_url}",
    f"- Overall: {'PASS' if report['ok'] else 'FAIL'}",
    f"- RequireAccel: {require_accel}",
    "",
    "| Scenario | Status |",
    "|---|---|",
]
for item in items:
    md.append(f"| {item['id']} | {item['status']} |")
    if item.get("mismatches"):
        md.append(f"| {item['id']} mismatches | {'; '.join(item['mismatches'])} |")
md_path = out_dir / "sidecar_python_rust_consistency_report.md"
md_path.write_text("\n".join(md) + "\n", encoding="utf-8")
print(json.dumps({"ok": report["ok"], "json": str(json_path), "md": str(md_path)}, ensure_ascii=False))
'@

Set-Content -Path $tmp -Value $py -Encoding UTF8
try {
  Info "running sidecar python/rust consistency checks"
  $raw = & python $tmp $root $DatasetDir $OutDir $AccelUrl $RequireAccel
  if ($LASTEXITCODE -ne 0) { throw "python sidecar consistency script failed" }
  $res = $raw | ConvertFrom-Json
  if (-not $res.ok) { throw "sidecar python/rust consistency checks failed. report: $($res.json)" }
  Ok "sidecar python/rust consistency checks passed"
  Ok "json: $($res.json)"
  Ok "md: $($res.md)"
}
finally {
  Remove-Item -Path $tmp -ErrorAction SilentlyContinue
}
