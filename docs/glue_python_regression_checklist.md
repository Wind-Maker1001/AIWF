# Glue-Python Regression Checklist

Use this checklist when changing any of the high-churn `glue-python` runtime paths, especially:

- `apps/glue-python/aiwf/preprocess.py`
- `apps/glue-python/aiwf/ingest.py`
- `apps/glue-python/aiwf/flows/cleaning.py`
- `apps/glue-python/app.py`

This checklist is derived from the current mainline tests and from archaeology of older branches where these behaviors were easy to regress.

## Core Commands

Fast local regression:

```powershell
cd .\apps\glue-python
python -m unittest discover -s tests -v
```

If you touched import/loading behavior, also run:

```powershell
python -m compileall -q .
```

If you changed cross-service behavior, also run:

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1
```

## Route and Runtime Contract

Primary tests:

- `apps/glue-python/tests/test_app.py`
- `apps/glue-python/tests/test_paths.py`

Must-pass behaviors:

- `/health` returns `{"ok": true}`
- `/capabilities` reports registered flows, input formats, preprocess operators, artifact selection tokens, and registry metadata
- unknown flow returns `404`
- internal errors hide traceback by default
- debug mode exposes traceback when `AIWF_DEBUG_ERRORS=true`
- custom flow registration works without editing `app.py`
- extension autoload works through configured extension modules
- extension force-reload re-registers module content
- conflict policies behave correctly:
  - `keep`
  - `error`
  - `warn`
- alias conflict handling does not hide canonical flow names

## Job Context and Path Safety

Primary tests:

- `apps/glue-python/tests/test_app.py`
- `apps/glue-python/tests/test_paths.py`

Must-pass behaviors:

- default job layout uses resolved jobs root
- explicit `job_context` is accepted and preferred when present
- legacy path params are rejected:
  - `params.job_root`
  - `params.stage_dir`
  - `params.artifacts_dir`
  - `params.evidence_dir`
- invalid `job_context` path escape returns `400`
- traversal `job_id` is rejected
- external absolute job root override is rejected by default
- external absolute job root override only works with explicit opt-in

## Cleaning Flow

Primary tests:

- `apps/glue-python/tests/test_cleaning_flow.py`

Must-pass behaviors:

- office theme and language defaults remain stable
- profile generation keeps:
  - dynamic columns
  - numeric stats
  - missing amount handling
- Office writers still generate:
  - `xlsx`
  - `docx`
  - `pptx`
- Office English output still works
- custom office artifacts can register and emit correctly
- custom core artifacts can register and emit correctly
- office outputs can be disabled
- optional core artifacts can be disabled
- required parquet artifact cannot be disabled
- nested artifact selection object remains supported
- cleaning rules validation still distinguishes:
  - valid config
  - invalid config
  - unknown keys
  - artifact selection object errors
- quality gates still enforce:
  - max invalid rows
  - max invalid ratio
  - pass path
- generic rules pipeline still works
- declarative `rules` block still works
- filtering, deduplication, and rounding stay stable
- CSV path loading still works
- large-input quality counts remain stable
- parquet magic-byte validation still works
- accel invalid parquet still falls back correctly
- local invalid parquet still fails in strict mode
- generic mode still skips accel when intended
- preprocess-enabled cleaning path still works
- preprocess pipeline inside cleaning still works
- Rust v2 path still works when enabled
- Rust v2 fallback still works when unavailable
- cleaned CSV still quotes delimited fields correctly

## Preprocess Pipeline

Primary tests:

- `apps/glue-python/tests/test_preprocess.py`

Must-pass behaviors:

- preprocess spec validation still rejects bad values for:
  - round digits
  - wrong types
  - bad `on_file_error`
  - bad `ocr_*`
  - bad `chunk_mode`
  - empty pipeline
  - invalid custom transform/filter names
- OCR options still pass through to ingest
- CSV preprocessing still normalizes amount/date fields
- JSONL transforms and row filters still apply in order
- registered custom field transforms still work
- registered custom row filters still work
- TXT + DOCX mixed input still works
- image input is skipped when OCR is disabled
- deduplication still works
- XLSX all-sheets ingestion still works
- standardized evidence output and quality report still work
- canonical bundle export still works
- canonical bundle path escape is rejected
- chunking and conflict detection still work
- pipeline stages `extract -> clean -> structure -> audit` still work
- unknown pipeline stage is rejected
- bare filename outputs still work
- registered custom pipeline stage still works
- final output format is respected
- final output path escape is rejected

## Ingest Layer

Primary tests:

- `apps/glue-python/tests/test_ingest.py`

Must-pass behaviors:

- OCR try-mode parsing remains stable
- OCR text extraction still prefers the highest-signal result
- `TESSERACT_CMD` env override still wins
- known Tesseract candidate paths still resolve
- TXT reader still splits and loads correctly
- XLSX reader still handles:
  - default first-sheet path
  - `xlsx_all_sheets=true`
- DOCX reader still works
- image files are skipped correctly when OCR is disabled
- custom input reader registration still works
- conflicting extension ownership still errors when requested
- extension owner replacement still works when allowed

## HTTP Clients and Transport

Primary tests:

- `apps/glue-python/tests/test_http_clients.py`

Must-pass behaviors:

- accel cleaning request serialization stays stable
- transform v2 request serialization stays stable
- accel cleaning response parsing stays stable
- invalid transform response shape is rejected
- `BaseClient` returns `{ok: true}` for empty success bodies
- `BaseClient` raises clear errors for invalid JSON
- Rust client returns `{ok: true}` for empty success bodies
- Rust client raises clear errors for invalid JSON
- accel client returns structured success for cleaning operator
- accel client tolerates invalid JSON body from cleaning operator response
- accel transform operator still rejects invalid response shape

## Manual Review Triggers

Do a focused manual review when changes touch any of these areas:

- extension loading order
- flow registration conflict policy
- `job_context` normalization
- artifact selection schema
- preprocess stage registration
- OCR dispatch and fallback
- accel/local parquet handoff
- Office artifact enabling/disabling

## Use This as a Guardrail

- Do not reintroduce legacy flow path params.
- Do not assume structural refactors are behavior-preserving without running the full checklist.
- Prefer adding or updating tests before changing `preprocess.py`, `ingest.py`, or `flows/cleaning.py`.
