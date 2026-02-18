# AIWF Quickstart

## 1. Run Canonical DB Migration

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\db_migrate.ps1 -SqlPassword "<YOUR_SA_PASSWORD>"
```

Optional app user creation:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\db_migrate.ps1 -SqlPassword "<YOUR_SA_PASSWORD>" -AppUser "aiwf_app" -AppPassword "<APP_PASSWORD>"
```

Notes:
- Canonical migration chain is managed by `db_migrate.ps1`.
- Legacy compatibility SQL is archived under `infra/sqlserver/legacy/` and is not part of the canonical chain.

## 2. Start `glue-python`

First-time setup with venv:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_glue_python.ps1 -CreateVenv
```

Normal startup:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_glue_python.ps1
```

## 3. Start `base-java`

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_base_java.ps1
```

## 4. Start `accel-rust` (Recommended for full flow)

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_accel_rust.ps1
```

## 5. Run Smoke Test

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1

# Desktop-style end-to-end flow (through dify-console API)
powershell -ExecutionPolicy Bypass -File .\ops\scripts\e2e_desktop_flow.ps1
```

`smoke_test.ps1` now includes SQL persistence verification by default (`jobs/steps/artifacts` are verified in SQL Server).
It also includes Office output quality gate by default (`xlsx/docx/pptx` structure, image bounds, mojibake checks).
If you only want API-level smoke, use:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1 -SkipSqlVerify -SkipOfficeQualityGate
```

Run smoke + invalid parquet fallback integration test:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1 -WithInvalidParquetFallbackTest
```

## 6. Cleaning Request JSON Examples

Basic request body:

```json
{
  "actor": "local",
  "ruleset_version": "v1",
  "params": {
    "rows": [
      { "id": 1, "amount": 100.125 },
      { "id": 1, "amount": 120.225 },
      { "id": 2, "amount": -5.0 },
      { "id": 3, "amount": 999.0 }
    ]
  }
}
```

Strict cleaning rules example:

```json
{
  "actor": "local",
  "ruleset_version": "v1",
  "params": {
    "rows": [
      { "id": "1", "amount": "100.126" },
      { "id": "2", "amount": "-5" },
      { "id": "1", "amount": "$120.225" },
      { "id": "3", "amount": "999" }
    ],
    "rules": {
      "drop_negative_amount": true,
      "min_amount": 0,
      "max_amount": 500,
      "deduplicate_by_id": true,
      "deduplicate_keep": "last",
      "sort_by_id": true,
      "amount_round_digits": 2,
      "id_field": "id",
      "amount_field": "amount"
    },
    "allow_empty_output": false,
    "office_theme": "assignment",
    "office_lang": "zh",
    "office_quality_mode": "high",
    "report_title": "Assignment Data Report"
  }
}
```

Call flow API directly from glue:

```powershell
$jobId = "job-local-001"
$body = @'
{
  "actor": "local",
  "ruleset_version": "v1",
  "params": {
    "input_csv_path": "D:\\AIWF\\examples\\input.csv",
    "drop_negative_amount": true,
    "max_amount": 100000,
    "deduplicate_by_id": true,
    "deduplicate_keep": "last",
    "amount_round_digits": 2
  }
}
'@

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:18081/jobs/$jobId/run/cleaning" `
  -ContentType "application/json" `
  -Body $body
```

Preprocess raw CSV into cooked CSV (decoupled utility):

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\preprocess_csv.ps1 `
  -InputCsv "D:\AIWF\data\raw.csv" `
  -OutputCsv "D:\AIWF\data\cooked.csv" `
  -ConfigJson ".\rules\templates\preprocess_finance_basic.json"
```

Validate preprocess spec before run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\validate_preprocess_spec.ps1 -SpecFile .\rules\templates\preprocess_finance_basic.json
```

Ingest a mixed evidence pack (`pdf/docx/txt/image/xlsx`) into structured JSONL:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ingest_evidence_pack.ps1 `
  -InputDir "D:\AIWF\evidence_raw" `
  -OutputJsonl "D:\AIWF\evidence_cooked\evidence.jsonl" `
  -ConfigJson ".\rules\templates\preprocess_debate_evidence.json" `
  -OcrEnabled $true `
  -XlsxAllSheets $true `
  -MaxRetries 2 `
  -OnFileError "skip"
```

When `generate_quality_report=true` in preprocess config, the run also writes:
- `D:\AIWF\evidence_cooked\evidence.jsonl.quality.json`

Validate cleaning rules before run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\validate_cleaning_rules.ps1 -ListTemplates
powershell -ExecutionPolicy Bypass -File .\ops\scripts\validate_cleaning_rules.ps1 -RuleFile .\rules\templates\generic_finance_strict.json
```

## 7. One-Command Local CI Check

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
```

Only skip docs checks when needed:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -SkipDocsChecks
```

## 8. Common Issues

- `sqlcmd not found`: install SQL Server command-line tools and add to PATH.
- `mvn not found`: install Maven and add to PATH.
- `python not found`: install Python and add to PATH.
- `cargo not found`: install Rust toolchain and add to PATH.
- `pdf support requires pypdf`: run `pip install -r apps/glue-python/requirements.txt`.
- `image OCR support requires pytesseract`: install Python deps from `requirements.txt` and install system Tesseract OCR binary (Windows users can install Tesseract and ensure `tesseract.exe` is in PATH).

Runtime dependency precheck:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_runtime_deps.ps1
```

Developer tool precheck (`git/sqlcmd/tesseract`):

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_dev_tools.ps1
```

Auto-fix missing `git/tesseract`:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_dev_tools.ps1 -AutoFix
```

One-command robust service restart + health checks:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\restart_services.ps1
```

Dify local integration guide:
- `docs/dify_local_integration.md`
- smoke script: `ops/scripts/dify_bridge_smoke.ps1`

Legacy standalone web frontend for AIWF (compatibility path only):
- `docs/archive/dify_standalone_frontend_legacy_20260216.md`
- start script: `ops/scripts/run_dify_console.ps1`

Desktop GUI app (Electron):
- `docs/dify_desktop_app.md`
- start/build script: `ops/scripts/run_dify_desktop.ps1`

Large dataset office-output guardrail/perf test:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\perf_office_large_dataset.ps1 -Rows 20000 -OfficeMaxRows 5000
```

Standalone Office quality check:

```powershell
$busRoot = $env:AIWF_BUS
if (-not $busRoot) { $busRoot = "D:\AIWF\bus" }

powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_office_artifacts_quality.ps1 `
  -XlsxPath "$busRoot\jobs\<job_id>\artifacts\fin.xlsx" `
  -DocxPath "$busRoot\jobs\<job_id>\artifacts\audit.docx" `
  -PptxPath "$busRoot\jobs\<job_id>\artifacts\deck.pptx" `
  -MinScore 80
```

Bootstrap local git repo and create first commit:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\git_bootstrap.ps1
```

Clean bulky local build artifacts (desktop dist/logs):

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\clean_workspace_artifacts.ps1 -RemoveLogs
```

Check docs local links only:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_docs_links.ps1 -IncludeReadme
```
