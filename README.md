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

Notes:
- `run_glue_python.ps1` now defaults `AIWF_STRICT_JOB_CONTEXT=1` when the variable is not already set.
- Prefer top-level `job_context` over legacy `params.job_root` / `params.stage_dir` / `params.artifacts_dir` / `params.evidence_dir`.
- `ops\config\dev.env.example` documents the strict-mode default.

4. Run smoke test:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1
```

5. Optional: include invalid-parquet fallback integration check:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1 -WithInvalidParquetFallbackTest
```

## Local Verification

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Quick
```

Quick profile keeps the fast gates:
- docs, secret, encoding, and sync checks
- Rust / Java / Python / desktop unit tests
- desktop packaged startup checks

Run the full profile when you need acceptance, smoke, contract, chaos, and benchmark gates:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
```

This runs:
- `accel-rust` tests
- `glue-python` unit tests
- smoke + invalid parquet fallback integration check
- strict `job_context` transport validation through the normal startup path

## GitHub Actions

- `Quick CI` runs on push / pull request for fast feedback.
- `Full Integration (Self-Hosted)` is intended for the Windows self-hosted runner and also runs nightly at `18:00 UTC`.
- Manual full runs accept `ci_profile=Full` and `run_full_integration=true`.
- Self-hosted full runs now write the local transcript path into the job summary instead of uploading an artifact. The transcript itself stays on the runner workspace under `ops/logs/ci`.
- GitHub scheduled workflows run from the default branch (`master`). Before this branch is merged, use the manual dispatch helper to validate current-branch full CI.

Manual dispatch from local PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\dispatch_full_integration_self_hosted.ps1 -Wait
```

Query the latest branch and nightly CI status:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\get_ci_status.ps1
```

Verify the current branch end-to-end (wait for `Quick CI`, then ensure `Full Integration (Self-Hosted)` is green for the same head):

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\verify_branch_ci.ps1
```

## Key Endpoints

- `base-java`: `GET /actuator/health`
- `glue-python`: `GET /health`
- `accel-rust`: `GET /health`
- run flow: `POST /api/v1/jobs/{jobId}/run/cleaning`

## Notes

- Canonical DB migration entrypoint: `ops/scripts/db_migrate.ps1`.
- `ops/scripts/aiwf_one_shot_setup.ps1` still exists, but DB setup is delegated to `db_migrate.ps1`.
- See `docs/quickstart.md`, `docs/verification.md`, and `docs/cleaning_rules.md` for operations and data-rule details.
