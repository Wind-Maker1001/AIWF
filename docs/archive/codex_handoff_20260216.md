# Codex Handoff (Updated)

## 1. Scope And Goal
This repo is now a working local data platform for:
- mixed raw evidence ingestion (`pdf/docx/txt/image/xlsx`),
- generic preprocessing and cleaning,
- SQL Server-tracked execution,
- polished office artifacts (`xlsx/docx/pptx`) for assignment/debate outputs.

Primary goal of this handoff: provide a truthful current-state snapshot and a practical runbook for next operator.

## 2. Current State (2026-02-16)
### What works now
- End-to-end chain is operational: `base-java -> glue-python -> accel-rust`.
- SQL Server control-plane integration is working (jobs/steps/artifacts persisted and queryable).
- Preprocess supports generic rule-driven transformations and multi-format ingestion.
- Cleaning supports declarative rules, quality gates, accel path + fallback path.
- Office artifact generation is substantially upgraded:
  - table + visual mix,
  - theme support (`professional|academic|debate`),
  - language support (`office_lang=zh|en`, default `zh`),
  - dynamic PPT image fitting (no shape overflow),
  - unified CJK font strategy for better Chinese output readability.

### Important behavior change
- `base-java` `run/cleaning` request handling now correctly passes `ruleset_version` + nested `params` to glue.
  - This is required for `office_theme`, `office_lang`, and custom titles to take effect through base API.

## 3. Verification Results (fresh recheck)
Commands run and results:
- `python -m unittest discover -s .\tests -v` in `apps/glue-python` -> PASS (`38` tests).
- `cargo test -q` in `apps/accel-rust` -> PASS (`3` tests).
- `mvn -q -DskipTests compile` in `apps/base-java` -> PASS.
- `powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1 -EnvFile .\ops\config\dev.env -Owner handoff_recheck` -> PASS.
  - Example: `job_id=6633cc121b4148c39fbe2eb4a6710379`, `run_ok=True`, `artifacts=6`.

## 4. Key Files Touched In Current Cycle
- `apps/glue-python/aiwf/flows/cleaning.py`
  - office theming/lang pipeline,
  - rich xlsx/docx/pptx rendering,
  - PPT image fit logic,
  - Chinese font stabilization,
  - accel success path now still regenerates rich office artifacts locally.
- `apps/glue-python/tests/test_cleaning_flow.py`
  - rich output checks,
  - PPT bounds assertions,
  - English mode checks.
- `apps/glue-python/aiwf/preprocess.py`
  - decoupled preprocess engine,
  - chunking + conflict detection + quality report,
  - BOM-tolerant config loading (`utf-8-sig`).
- `apps/glue-python/aiwf/ingest.py`
  - multi-format raw ingestion,
  - `xlsx_all_sheets`, source metadata enrichment.
- `apps/base-java/src/main/java/com/aiwf/base/web/JobController.java`
  - fixed run payload parsing/forwarding semantics.
- `apps/accel-rust/src/main.rs`
  - improved built-in office generation script template.
- `docs/quickstart.md`
- `docs/cleaning_rules.md`

## 5. Runbook (Operator)
### 5.1 Start services
Open three terminals:
```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_accel_rust.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_glue_python.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_base_java.ps1
```

### 5.2 Health check
```powershell
Invoke-RestMethod http://127.0.0.1:18080/actuator/health
Invoke-RestMethod http://127.0.0.1:18081/health
Invoke-RestMethod http://127.0.0.1:18082/health
```

### 5.3 Smoke
```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1 -EnvFile .\ops\config\dev.env
```

### 5.4 Mixed raw evidence ingest
```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ingest_evidence_pack.ps1 `
  -InputDir "D:\AIWF\examples\evidence_raw_demo" `
  -OutputJsonl "D:\AIWF\examples\evidence_cooked_demo\evidence.jsonl" `
  -ConfigJson ".\rules\templates\preprocess_debate_evidence.json" `
  -OcrEnabled $false `
  -XlsxAllSheets $true `
  -MaxRetries 1 `
  -OnFileError "skip"
```

### 5.5 Office style/language options in cleaning params
```json
{
  "office_theme": "debate",
  "office_lang": "zh",
  "report_title": "作业与辩论数据报告",
  "cover_title": "辩论证据展示"
}
```

## 6. Known Caveats
- Console mojibake on Windows PowerShell can still occur when printing Chinese text in terminal; this does **not** necessarily mean Office file content is corrupted.
- OCR pipeline requires system/runtime deps:
  - Python: `Pillow`, `pytesseract`
  - System: `tesseract.exe` in PATH
- PDF extraction requires `pypdf`.
- If accel returns invalid parquet, glue fallback is expected behavior (guardrail is active).

## 7. Suggested Next Work
- Add template-level typography controls (font families/sizes) per theme in config.
- Add chart rendering options (bar/line/pie) for office visual panels.
- Add explicit `/api` endpoint contract tests from base -> glue for `office_*` params.
- Add one-click packaging command to export final assignment/debate bundle to desktop.

## 8. Handoff Bottom Line
The project is in a usable state for assignment/debate production workflows.
Core chain, tests, and artifact quality upgrades are in place and recently revalidated.