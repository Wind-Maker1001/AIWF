# AIWF

AIWF is a local-first workflow platform for turning raw files and tabular inputs into cleaned datasets, structured outputs, and Office deliverables.

## Repository Layout

- `apps/base-java`: control plane and orchestration APIs
- `apps/glue-python`: flow runtime and preprocessing service
- `apps/accel-rust`: operator runtime and HTTP operator endpoints
- `apps/dify-native-winui`: primary native WinUI desktop frontend
- `apps/dify-desktop`: secondary Electron compatibility shell with offline engine and Workflow Studio
- `ops/scripts`: startup, packaging, CI, smoke, and release scripts
- `docs`: active guides, reference docs, and historical snapshots

## Start Here

- Documentation hub: [docs/README.md](docs/README.md)
- Quickstart: [docs/quickstart.md](docs/quickstart.md)
- Native WinUI quickstart: [docs/quickstart_native_winui.md](docs/quickstart_native_winui.md)
- Backend quickstart: [docs/quickstart_backend.md](docs/quickstart_backend.md)
- Verification guide: [docs/verification.md](docs/verification.md)
- Architecture authority charter: [docs/architecture_authority_charter_20260425.md](docs/architecture_authority_charter_20260425.md)
- Current authority convergence note: [docs/authority_execution_convergence_20260406.md](docs/authority_execution_convergence_20260406.md)

## Common Commands

Backend bootstrap:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\db_migrate.ps1 -SqlPassword "<YOUR_SA_PASSWORD>"
powershell -ExecutionPolicy Bypass -File .\ops\scripts\restart_services.ps1
```

Frontend run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_native_winui.ps1 -Configuration Debug
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -BuildWin -Configuration Release -Version "<version>"
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -BuildInstaller -Configuration Release -Version "<version>" -CreateZip
```

Frontend release:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_frontend_productize.ps1 -Version "<version>" -Frontend WinUI -Configuration Release -CreateZip
```

Electron compatibility run or package:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -Frontend Electron
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_electron_compatibility.ps1 -Version "<version>"
```

Local verification:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Quick
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
```

If your local machine does not have SQL connectivity ready yet, add `-SkipSqlConnectivityGate` for a reduced local check.

## Compatibility Paths

- Desktop offline quickstart: [docs/quickstart_desktop_offline.md](docs/quickstart_desktop_offline.md)
- Electron compatibility guide: [docs/dify_desktop_app.md](docs/dify_desktop_app.md)
- Electron offline bundle delivery: [docs/offline_delivery_minimal.md](docs/offline_delivery_minimal.md)

## Key HTTP Endpoints

- `base-java`: `GET /actuator/health`
- `base-java`: `GET /api/v1/backend/capabilities`
- `base-java`: `POST /api/v1/tools/create_job`
- `base-java`: `POST /api/v1/jobs/{jobId}/run/{flow}`
- `base-java`: `POST /api/v1/integrations/dify/run_cleaning`
- `glue-python`: `GET /health`
- `glue-python`: `GET /capabilities`
- `accel-rust`: `GET /health`
- `accel-rust`: `GET /capabilities`

## Documentation Scope

The canonical documentation entrypoint is [docs/README.md](docs/README.md).

Recommended current reading order:

- [docs/quickstart.md](docs/quickstart.md)
- [docs/verification.md](docs/verification.md)
- [docs/architecture_authority_charter_20260425.md](docs/architecture_authority_charter_20260425.md)
- [docs/authority_execution_convergence_20260406.md](docs/authority_execution_convergence_20260406.md)

Compatibility-only docs remain in the repo, but are not the primary onboarding path:

- [docs/quickstart_desktop_offline.md](docs/quickstart_desktop_offline.md)
- [docs/dify_desktop_app.md](docs/dify_desktop_app.md)
- [docs/offline_delivery_minimal.md](docs/offline_delivery_minimal.md)
- [docs/electron_compatibility_retirement_plan_20260321.md](docs/electron_compatibility_retirement_plan_20260321.md)

Historical handoff and snapshot docs remain under [docs/archive/README.md](docs/archive/README.md).

## Notes

- The canonical backend flow is `create_job -> run flow -> query steps/artifacts`.
- `job_context` is the canonical flow path contract transported from `base-java` to `glue-python`.
- `workflow_definition` is the canonical workflow field across workflow save/publish/governance paths.
- `accel-rust` is the sole executable authority for workflow validation and workflow draft/reference execution.
- Legacy path fields under `params` are not supported.
- GitHub-hosted `Quick CI` and self-hosted `Full Integration (Self-Hosted)` are separate workflows; see [docs/verification.md](docs/verification.md) for the exact commands they run.
