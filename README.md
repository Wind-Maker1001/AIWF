# AIWF (AI Workflow Framework)

AIWF 是一个本地优先（local-first）的工作流平台，面向“生肉数据 -> 清洗预处理 -> 结构化输出 -> Office 成品”的全流程。
AIWF is a local-first workflow platform for the full pipeline from raw inputs to cleaned data, structured outputs, and Office deliverables.

核心服务 / Core services:
- `base-java`: control plane APIs and orchestration entrypoints
- `glue-python`: flow runner (`cleaning` flow implemented)
- `accel-rust`: operator service used by `cleaning` flow

## Repository Layout

- `apps/base-java`: Spring Boot service
- `apps/glue-python`: FastAPI service
- `apps/accel-rust`: Rust operator service
- `infra/sqlserver/init`: SQL initialization/migration scripts
- `ops/config/dev.env`: local environment configuration
- `ops/scripts`: run, migration, smoke, and verification scripts
- `docs`: quickstart and verification docs

## Prerequisites

- Windows + PowerShell
- JDK 21
- Maven 3.9+
- Python 3.11+
- Rust toolchain (`cargo`)
- SQL Server reachable from local environment
- `sqlcmd` in PATH

## Recommended Startup Flow

1. Run database migration (canonical entrypoint):

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\db_migrate.ps1 -SqlPassword "<YOUR_SA_PASSWORD>"
```

2. Optional: create/update app DB user:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\db_migrate.ps1 -SqlPassword "<YOUR_SA_PASSWORD>" -AppUser "aiwf_app" -AppPassword "<APP_PASSWORD>"
```

3. Start services in separate terminals:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_accel_rust.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_glue_python.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_base_java.ps1
```

4. Run smoke test:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1
```

5. Optional: include invalid-parquet fallback integration check:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1 -WithInvalidParquetFallbackTest
```

## One-Command Local Verification

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
```

This runs:
- `accel-rust` tests
- `glue-python` unit tests
- smoke + invalid parquet fallback integration check

## Key Endpoints

- `base-java`: `GET /actuator/health`
- `glue-python`: `GET /health`
- `accel-rust`: `GET /health`
- run flow: `POST /api/v1/jobs/{jobId}/run/cleaning`

## Notes

- Canonical DB migration entrypoint: `ops/scripts/db_migrate.ps1`.
- `ops/scripts/aiwf_one_shot_setup.ps1` still exists, but DB setup is delegated to `db_migrate.ps1`.
- See `docs/quickstart.md`, `docs/verification.md`, and `docs/cleaning_rules.md` for operations and data-rule details.
