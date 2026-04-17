# Regression Quality

This document tracks the regression datasets and commands used by the repository quality gates.

## Datasets

- smoke: `lake/datasets/regression_v1_1`
  - purpose: lightweight finance/debate regression
  - inputs:
    - `raw_finance.csv`
    - `raw_debate.jsonl`
  - expectations:
    - `expectations.json`
- gold: `lake/datasets/regression_v1_2_sidecar_gold`
  - purpose: image/xlsx sidecar regression
  - scenarios:
    - `xlsx_customer_multi_sheet`
    - `xlsx_finance_units`
    - `image_blank_blocked`
    - `xlsx_dirty_header_repeat`
    - `xlsx_dirty_subtotal_note_rows`
    - `xlsx_bank_signed_amount_conflict`
    - `xlsx_customer_ocr_phone_account_fix`
    - `xlsx_hidden_sheet_formula_gap`
    - `xlsx_utf8_gbk_mixed_text`
  - per-scenario assets:
    - `scenario.json`
    - `expected_rows.jsonl`
    - `expected_quality.json`

## Commands

Primary smoke regression run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_regression_quality.ps1
```

Sidecar gold regression run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_sidecar_regression_quality.ps1
```

Optional Python/Rust consistency run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_sidecar_python_rust_consistency.ps1
```

Release/CI consistency run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_sidecar_python_rust_consistency.ps1 -RequireAccel
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
- `ops/logs/regression/sidecar_regression_quality_report.json`
- `ops/logs/regression/sidecar_regression_quality_report.md`
- `ops/logs/regression/sidecar_python_rust_consistency_report.json`
- `ops/logs/regression/sidecar_python_rust_consistency_report.md`
- related evidence may also appear in:
  - `ops/logs/perf/`
  - `ops/logs/route_bench/`

## Notes

- treat the generated reports as the source of truth for current pass/fail state
- smoke and gold serve different purposes; do not replace one with the other
- dirty-table coverage should exercise both Python and Rust row transforms before new rules move from shadow to default
- local desktop fixture tests may skip real XLSX reads when `apps/dify-desktop/node_modules/exceljs/package.json` is absent; use `ops/scripts/check_desktop_fixture_deps.ps1` before treating that as a problem
- do not rely on static "current status" text in this document
- see [verification.md](verification.md) for the wider CI matrix
