# AIWF Verification Guide

This document maps local verification entrypoints to the actual CI scripts in the repository.

## Local CI Entry Points

Fast local profile:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Quick
```

The quick profile still runs:

- docs link checks
- release evidence checks
- OpenAPI / SDK sync checks
- secret scan
- encoding checks
- Rust / Java / Python tests
- rust transform benchmark self-test
- desktop unit, UI, and packaged-startup checks unless you explicitly skip them

Full local profile:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
```

The full profile adds:

- smoke
- invalid parquet fallback integration
- contract tests
- chaos checks
- routing and async benchmark gates
- rust transform and new-ops benchmark gates
- native WinUI smoke outside CI

If your local machine does not have SQL ready yet, add `-SkipSqlConnectivityGate`.

## Component-Level Checks

Rust:

```powershell
cd .\apps\accel-rust
cargo test -q
```

Python:

```powershell
cd .\apps\glue-python
python -m unittest discover -s tests -v
```

See also:

- [glue_python_regression_checklist.md](glue_python_regression_checklist.md)

Java:

```powershell
cd .\apps\base-java
mvn -q test
```

Desktop:

```powershell
cd .\apps\dify-desktop
npm run smoke
npm run test:unit
npm run test:workflow-ui
```

Packaged desktop startup checks:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_desktop_packaged_startup.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_desktop_lite_packaged_startup.ps1
```

## Backend Smoke and Fallback

Restart the full local backend:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\restart_services.ps1
```

Smoke:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1
```

Fallback validation:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\test_invalid_parquet_fallback.ps1
```

## Dependency and Security Checks

Runtime dependency precheck:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_runtime_deps.ps1
```

Developer tool precheck:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_dev_tools.ps1
```

RustSec audit:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_cargo_audit.ps1
```

## Performance Gates

Rust transform gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_rust_transform_bench_gate.ps1
```

Rust new-ops gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_rust_new_ops_bench_gate.ps1
```

Routing benchmark gate:

```powershell
cd .\apps\dify-desktop
npm run bench:routing
```

## GitHub Workflows

GitHub-hosted quick workflow:

- file: `.github/workflows/ci.yml`
- actual command:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Quick -SkipToolChecks -SkipSqlConnectivityGate -SkipDesktopUiTests -SkipDesktopPackageTests
```

Self-hosted full workflow:

- file: `.github/workflows/full-integration-self-hosted.yml`
- default behavior: runs `ci_check.ps1` on a Windows self-hosted runner
- `run_full_integration=false` skips smoke in that workflow
- scheduled runs execute from the default branch only

## CI Helper Scripts

- `ops/scripts/dispatch_full_integration_self_hosted.ps1`
- `ops/scripts/get_ci_status.ps1`
- `ops/scripts/verify_branch_ci.ps1`

Use those scripts when you want to validate branch CI from a local PowerShell session.
