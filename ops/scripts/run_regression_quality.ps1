param(
  [string]$DatasetDir = "",
  [string]$OutDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $DatasetDir) {
  $DatasetDir = Join-Path $root "lake\datasets\regression_v1_1"
}
if (-not $OutDir) {
  $OutDir = Join-Path $root "ops\logs\regression"
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$tmp = Join-Path $env:TEMP "aiwf_regression_quality.py"
$py = @'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

repo = Path(sys.argv[1])
dataset = Path(sys.argv[2])
out_dir = Path(sys.argv[3])
out_dir.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(repo / "apps" / "glue-python"))

from aiwf import preprocess
from aiwf.flows import cleaning

cleaning._base_step_start = lambda **kwargs: None
cleaning._base_step_done = lambda **kwargs: None
cleaning._base_step_fail = lambda **kwargs: None
cleaning._base_artifact_upsert = lambda **kwargs: None

exp = json.loads((dataset / "expectations.json").read_text(encoding="utf-8-sig"))
acc = exp.get("acceptance", {})

finance_raw = dataset / "raw_finance.csv"
debate_raw = dataset / "raw_debate.jsonl"

finance_pre = out_dir / "finance_preprocessed.csv"
finance_pre_res = preprocess.preprocess_file(
    str(finance_raw),
    str(finance_pre),
    {
        "input_format": "csv",
        "output_format": "csv",
        "header_map": {"id": "id", "amount": "amount", "biz_date": "biz_date", "category": "category"},
        "amount_fields": ["amount"],
        "date_fields": ["biz_date"],
        "date_input_formats": ["%Y/%m/%d", "%Y-%m-%d"],
        "deduplicate_by": ["id", "amount", "biz_date"],
        "deduplicate_keep": "first",
    },
)

clean_job_root = out_dir / "cleaning_job"
clean_res = cleaning.run_cleaning(
    job_id=f"reg_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
    actor="regression",
    params={
        "job_root": str(clean_job_root),
        "input_csv_path": str(finance_pre),
        "local_parquet_strict": False,
        "drop_negative_amount": True,
        "max_amount": 100000,
    },
)

pipeline_res = preprocess.run_preprocess_pipeline(
    pipeline={
        "stages": [
            {"name": "extract", "config": {"input_format": "jsonl", "output_format": "jsonl"}},
            {"name": "clean", "config": {"header_map": {"text": "claim_text"}, "trim_strings": True}},
            {"name": "structure", "config": {"standardize_evidence": True, "output_format": "jsonl"}},
            {
                "name": "audit",
                "config": {
                    "generate_quality_report": True,
                    "quality_required_fields": ["claim_text", "source_url"],
                    "detect_conflicts": True,
                    "conflict_text_field": "claim_text",
                    "conflict_positive_words": ["support", "good"],
                    "conflict_negative_words": ["oppose", "bad"],
                },
            },
        ]
    },
    job_root=str(out_dir),
    stage_dir=str(out_dir / "debate_pipeline"),
    input_path=str(debate_raw),
    final_output_path=str(out_dir / "debate_final.csv"),
)

debate_rows, _ = preprocess._read_csv(str(out_dir / "debate_final.csv"))
required_fields = list(acc.get("preprocess_required_fields", []))
missing_required = 0
for r in debate_rows:
    for f in required_fields:
        v = r.get(f)
        if v is None or str(v).strip() == "":
            missing_required += 1

checks = {
    "cleaning_min_output_rows": int(clean_res["profile"]["quality"]["output_rows"]) >= int(acc.get("cleaning_min_output_rows", 0)),
    "cleaning_max_invalid_rows": int(clean_res["profile"]["quality"]["invalid_rows"]) <= int(acc.get("cleaning_max_invalid_rows", 10**9)),
    "preprocess_min_output_rows": len(debate_rows) >= int(acc.get("preprocess_min_output_rows", 0)),
}

required_cells = max(1, len(debate_rows) * max(1, len(required_fields)))
missing_ratio = float(missing_required) / float(required_cells)
checks["preprocess_required_missing_ratio"] = missing_ratio <= float(acc.get("preprocess_max_required_missing_ratio", 1.0))

overall_ok = all(checks.values())
report = {
    "ok": overall_ok,
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "dataset_dir": str(dataset),
    "checks": checks,
    "acceptance": acc,
    "cleaning": {
        "job_id": clean_res.get("job_id"),
        "quality": clean_res.get("profile", {}).get("quality", {}),
        "artifacts": clean_res.get("artifacts", []),
    },
    "preprocess": {
        "finance": finance_pre_res,
        "debate_pipeline": pipeline_res,
        "debate_rows": len(debate_rows),
        "required_missing_ratio": missing_ratio,
    },
}

json_path = out_dir / "regression_quality_report.json"
json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

md = [
    "# Regression Quality Report",
    "",
    f"- Time: {report['generated_at']}",
    f"- Overall: {'PASS' if report['ok'] else 'FAIL'}",
    "",
    "## Checks",
]
for k, v in checks.items():
    md.append(f"- {k}: {'PASS' if v else 'FAIL'}")
md.extend(
    [
        "",
        "## Cleaning",
        f"- output_rows: {report['cleaning']['quality'].get('output_rows', 0)}",
        f"- invalid_rows: {report['cleaning']['quality'].get('invalid_rows', 0)}",
        "",
        "## Preprocess",
        f"- debate_rows: {len(debate_rows)}",
        f"- required_missing_ratio: {missing_ratio:.6f}",
    ]
)
(out_dir / "regression_quality_report.md").write_text("\n".join(md) + "\n", encoding="utf-8")

print(json.dumps({"ok": overall_ok, "json": str(json_path), "md": str(out_dir / "regression_quality_report.md")}, ensure_ascii=False))
'@

Set-Content -Path $tmp -Value $py -Encoding UTF8
try {
  Info "running regression quality pipeline"
  $raw = & python $tmp $root $DatasetDir $OutDir
  if ($LASTEXITCODE -ne 0) { throw "python regression script failed" }
  $res = $raw | ConvertFrom-Json
  if (-not $res.ok) { throw "regression quality checks failed. report: $($res.json)" }
  Ok "regression quality passed"
  Ok "json: $($res.json)"
  Ok "md: $($res.md)"
}
finally {
  Remove-Item -Path $tmp -ErrorAction SilentlyContinue
}
