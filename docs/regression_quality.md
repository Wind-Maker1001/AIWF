# Regression Quality (Aligned: 2026-02-18)

## Scope
- This doc tracks the regression quality gate used by CI and release packaging.
- It is aligned with:
  - `docs/codex_handoff.md`
  - `docs/project_audit_20260218.md`

## Dataset
- Path: `lake/datasets/regression_v1_1`
- Inputs:
  - `raw_finance.csv`
  - `raw_debate.jsonl`
- Expectations:
  - `expectations.json`

## Commands
```powershell
powershell -ExecutionPolicy Bypass -File ops/scripts/run_regression_quality.ps1
```

Full CI (recommended):
```powershell
powershell -ExecutionPolicy Bypass -File ops/scripts/ci_check.ps1
```

Release gate:
```powershell
powershell -ExecutionPolicy Bypass -File ops/scripts/release_productize.ps1 -Version <x.y.z>
```

## Outputs
- `ops/logs/regression/regression_quality_report.json`
- `ops/logs/regression/regression_quality_report.md`
- Additional CI evidence:
  - `ops/logs/perf/async_tasks_baseline_latest.json`
  - `ops/logs/route_bench/routing_bench_latest.json`

## Current Gate Status
- Regression quality gate: pass
- Full `ci_check` pipeline: pass
- Risk level: low

## Notes
- ODBC task-store SQL is parameterized; backend fallback is enabled (`odbc -> sqlcmd -> base_api -> disable`).
- OpenAPI contract is schema-complete for current Rust endpoints.
- Runtime dependency checks support bundled desktop tools/fonts in addition to system installs.
