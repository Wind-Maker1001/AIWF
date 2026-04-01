# AIWF Backend Quickstart

This guide covers the canonical local backend chain:

- SQL Server
- `apps/base-java`
- `apps/glue-python`
- `apps/accel-rust`

## Prerequisites

- Windows + PowerShell
- JDK 21
- Maven 3.9+
- Python 3.11+
- Rust toolchain
- SQL Server
- `sqlcmd` in PATH

Use `ops/config/dev.env` as the local environment file. The tracked shape is in `ops/config/dev.env.example`.

`run_base_java.ps1` requires either:

- a real `AIWF_SQL_PASSWORD`, or
- `AIWF_SQL_TRUSTED=true` for integrated authentication

## 1. Migrate the Database

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\db_migrate.ps1 -SqlPassword "<YOUR_SA_PASSWORD>"
```

Optional app-user setup:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\db_migrate.ps1 -SqlPassword "<YOUR_SA_PASSWORD>" -AppUser "aiwf_app" -AppPassword "<APP_PASSWORD>"
```

## 2. Start the Services

First-time `glue-python` setup can create a local venv:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_glue_python.ps1 -CreateVenv
```

`run_glue_python.ps1` now prints optional extraction dependency status for `pandera` / `python-calamine` / `paddleocr` / `docling`. Image and XLSX enhanced ingest depends on the local `glue-python` sidecar being available.

If you want startup to fail fast when enhanced ingest dependencies are missing, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_glue_python.ps1 -RequireEnhancedIngest
```

Normal startup:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_accel_rust.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_glue_python.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_base_java.ps1
```

Or use the restart helper that stops listeners and waits for health:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\restart_services.ps1
```

## 3. Verify Health

- `base-java`: `GET http://127.0.0.1:18080/actuator/health`
- `base-java`: `GET http://127.0.0.1:18080/api/v1/backend/capabilities`
- `glue-python`: `GET http://127.0.0.1:18081/health`
- `glue-python`: `GET http://127.0.0.1:18081/capabilities`
- `accel-rust`: `GET http://127.0.0.1:18082/health`
- `accel-rust`: `GET http://127.0.0.1:18082/capabilities`

## 4. Run the Canonical Backend Flow

The canonical control-plane path is:

1. create a job in `base-java`
2. run a flow for that job
3. query steps and artifacts

```powershell
$base = "http://127.0.0.1:18080"

$job = Invoke-RestMethod `
  -Method Post `
  -Uri "$base/api/v1/tools/create_job?owner=local" `
  -ContentType "application/json" `
  -Body "{}"

$jobId = $job.job_id

$body = @{
  actor = "local"
  ruleset_version = "v1"
  params = @{
    input_csv_path = "D:\AIWF\examples\finance_raw_demo\finance_sheet.csv"
  }
} | ConvertTo-Json -Depth 10

Invoke-RestMethod `
  -Method Post `
  -Uri "$base/api/v1/jobs/$jobId/run/cleaning" `
  -ContentType "application/json" `
  -Body $body

Invoke-RestMethod -Method Get -Uri "$base/api/v1/jobs/$jobId/steps"
Invoke-RestMethod -Method Get -Uri "$base/api/v1/jobs/$jobId/artifacts"
```

If you want a Dify-friendly single-call endpoint, use `POST /api/v1/integrations/dify/run_cleaning` instead.

Contract notes:

- `job_context` is the canonical transport contract between `base-java` and `glue-python`
- legacy path fields under `params` are not supported
- preferred cleaning config contract is `contracts/glue/cleaning_spec.v2.schema.json`
- legacy `params.rules` / `params.preprocess` are still accepted, but they are compiled into `cleaning_spec.v2` before execution
- local cleaning default switch controls:
  - `AIWF_CLEANING_RUST_V2_MODE=off|shadow|default`
  - `AIWF_CLEANING_RUST_V2_VERIFY_ON_DEFAULT=true|false`

## 5. Smoke and Validation

Smoke:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1
```

Smoke plus invalid parquet fallback validation:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1 -WithInvalidParquetFallbackTest
```

Rules validation:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\validate_cleaning_rules.ps1 -ListTemplates
powershell -ExecutionPolicy Bypass -File .\ops\scripts\validate_cleaning_rules.ps1 -RuleFile .\rules\templates\generic_finance_strict.json
powershell -ExecutionPolicy Bypass -File .\ops\scripts\validate_preprocess_spec.ps1 -SpecFile .\rules\templates\preprocess_debate_evidence.json
```

Enhanced sidecar ingest contract:

- `contracts/glue/ingest_extract.schema.json`

Unified cleaning contract:

- `contracts/glue/cleaning_spec.v2.schema.json`

Sidecar regression:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_sidecar_regression_quality.ps1
```

Sidecar Python/Rust consistency:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_sidecar_python_rust_consistency.ps1 -RequireAccel
```

Cleaning rollout governance gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_cleaning_rust_v2_rollout.ps1
```

This gate consumes the latest cleaning rollout `run_mode_audit.jsonl`, validates `execution.shadow_compare`, and checks that the sidecar Python/Rust consistency report stays free of `reason_counts` mismatches before package or release paths proceed.

## Related Docs

- [verification.md](verification.md)
- [cleaning_rules.md](cleaning_rules.md)
- [dify_local_integration.md](dify_local_integration.md)
- [backend_chiplet_decoupling.md](backend_chiplet_decoupling.md)
