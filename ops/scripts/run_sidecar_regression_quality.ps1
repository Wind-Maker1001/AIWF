param(
  [string]$DatasetDir = "",
  [string]$OutDir = "",
  [switch]$Quick
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
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$tmp = Join-Path $env:TEMP "aiwf_sidecar_regression_quality.py"
$py = @'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

repo = Path(sys.argv[1])
dataset = Path(sys.argv[2])
out_dir = Path(sys.argv[3])
quick_mode = str(sys.argv[4]).strip().lower() in {"1", "true", "yes", "on"}
out_dir.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(repo / "apps" / "glue-python"))

from aiwf.sidecar_regression import (
    compare_expected_quality,
    compare_expected_rows,
    load_sidecar_dataset,
    run_sidecar_extract_for_scenario,
    validate_ingest_extract_contract,
)

scenarios = load_sidecar_dataset(dataset)
if quick_mode:
    scenarios = [
        entry
        for entry in scenarios
        if bool((entry.get("scenario") or {}).get("quick_gate", False))
    ]
items = []
failed = []

for entry in scenarios:
    scenario = entry["scenario"]
    result = run_sidecar_extract_for_scenario(entry["dir"], scenario)
    payload = result["payload"] if isinstance(result.get("payload"), dict) else {}
    errors = []
    if int(result["status_code"]) != 200:
        errors.append(f"http {result['status_code']}")
    else:
        errors.extend(validate_ingest_extract_contract(payload))
        errors.extend(compare_expected_quality(payload, entry["expected_quality"], scenario))
        errors.extend(compare_expected_rows(payload.get("rows") or [], entry["expected_rows"], scenario))
    item = {
        "id": entry["id"],
        "status_code": result["status_code"],
        "ok": len(errors) == 0,
        "errors": errors,
        "quality_blocked": bool(payload.get("quality_blocked")),
        "row_count": len(payload.get("rows") or []),
        "table_cells": len(payload.get("table_cells") or []),
        "sheet_frames": len(payload.get("sheet_frames") or []),
        "engine_trace": list(payload.get("engine_trace") or []),
    }
    items.append(item)
    if errors:
        failed.append(entry["id"])

report = {
    "ok": len(failed) == 0,
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "dataset_dir": str(dataset),
    "quick_mode": quick_mode,
    "total": len(items),
    "failed": failed,
    "items": items,
}

json_path = out_dir / "sidecar_regression_quality_report.json"
json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

md = [
    "# Sidecar Regression Quality Report",
    "",
    f"- Time: {report['generated_at']}",
    f"- Overall: {'PASS' if report['ok'] else 'FAIL'}",
    f"- Dataset: {report['dataset_dir']}",
    f"- QuickMode: {quick_mode}",
    "",
    "| Scenario | Status | Rows | Table Cells | Sheet Frames |",
    "|---|---|---:|---:|---:|",
]
for item in items:
    md.append(
        f"| {item['id']} | {'PASS' if item['ok'] else 'FAIL'} | {item['row_count']} | {item['table_cells']} | {item['sheet_frames']} |"
    )
    if item["errors"]:
        md.append(f"| {item['id']} errors | {'; '.join(item['errors'])} |  |  |  |")
md_path = out_dir / "sidecar_regression_quality_report.md"
md_path.write_text("\n".join(md) + "\n", encoding="utf-8")

print(json.dumps({"ok": report["ok"], "json": str(json_path), "md": str(md_path)}, ensure_ascii=False))
'@

Set-Content -Path $tmp -Value $py -Encoding UTF8
try {
  Info "running sidecar regression quality"
  $raw = & python $tmp $root $DatasetDir $OutDir $Quick
  if ($LASTEXITCODE -ne 0) { throw "python sidecar regression script failed" }
  $res = $raw | ConvertFrom-Json
  if (-not $res.ok) { throw "sidecar regression quality checks failed. report: $($res.json)" }
  Ok "sidecar regression quality passed"
  Ok "json: $($res.json)"
  Ok "md: $($res.md)"
}
finally {
  Remove-Item -Path $tmp -ErrorAction SilentlyContinue
}
