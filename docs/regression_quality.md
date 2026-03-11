# Regression Quality

This document tracks the regression dataset and commands used by the repository quality gate.

## Dataset

- path: `lake/datasets/regression_v1_1`
- inputs:
  - `raw_finance.csv`
  - `raw_debate.jsonl`
- expectations:
  - `expectations.json`

## Commands

Primary regression run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_regression_quality.ps1
```

Full CI entrypoint:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
```

Release packaging entrypoint:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_productize.ps1 -Version <x.y.z>
```

## Outputs

- `ops/logs/regression/regression_quality_report.json`
- `ops/logs/regression/regression_quality_report.md`
- related evidence may also appear in:
  - `ops/logs/perf/`
  - `ops/logs/route_bench/`

## Notes

- treat the generated reports as the source of truth for current pass/fail state
- do not rely on static “current status” text in this document
- see [verification.md](verification.md) for the wider CI matrix
