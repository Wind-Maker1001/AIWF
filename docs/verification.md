# AIWF Verification Guide

This document lists the current verification commands for local development.

## 1. Unit/Module Checks

```powershell
# accel-rust
cd D:\AIWF\apps\accel-rust
cargo check
cargo test -q

# glue-python
cd D:\AIWF\apps\glue-python
python -m unittest discover -s tests -v
```

## 2. Standard End-to-End Smoke

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1
```

Expected high-level signals:
- `run_ok : True`
- `artifacts : 6`
- `[ OK ] smoke test finished`

## 3. Invalid Parquet Fallback Integration Test

Purpose:
- Force accel-rust to return an invalid parquet payload.
- Verify glue-python rejects that parquet and falls back safely.

Direct run:

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\test_invalid_parquet_fallback.ps1
```

Or from smoke script:

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1 -WithInvalidParquetFallbackTest
```

Expected high-level signals:
- `accel_attempted : True`
- `accel_used_fallback : True`
- `[ OK ] invalid parquet fallback test passed`

## 4. Notes

- The fallback integration script starts temporary accel/glue processes and stops them automatically.
- It uses a dedicated temporary cargo `--target-dir` to avoid binary lock conflicts with long-running local accel processes.

## 5. One-Command Local CI Check

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
```

Optional skips:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -SkipRustTests
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -SkipPythonTests
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -SkipSmoke
```

## 6. GitHub Actions Workflows

- `.github/workflows/ci.yml`
  - Trigger: push / pull_request
  - Runner: `windows-latest` (GitHub-hosted)
  - Runs: `ci_check.ps1 -SkipSmoke` (unit/module checks only)

- `.github/workflows/full-integration-self-hosted.yml`
  - Trigger: manual (`workflow_dispatch`)
  - Runner: `self-hosted` + `windows`
  - Default: runs `ci_check.ps1 -SkipSmoke` (does not require SQL Server)
  - Optional: set input `run_full_integration=true` to run full `ci_check.ps1` including smoke + invalid parquet fallback integration test
  - Full integration mode should be used only on an environment where SQL Server and required local runtime prerequisites are available.
